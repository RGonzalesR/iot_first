from datetime import datetime
from pathlib import Path

import pytest
import yaml
from pyspark.sql import Row, types as T

import kafka_consumer_spark as kc


# =======================
# Paths e helpers
# =======================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
METADATA_DIR = PROJECT_ROOT / "db" / "metadata"


def load_table_metadata(table_name: str):
    """
    Carrega o metadata YAML (db/metadata/*.yml) e devolve o dict da tabela.
    """
    path = METADATA_DIR / f"{table_name}.yml"
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    for t in data.get("table", []):
        if t["name"] == table_name:
            return t

    raise ValueError(f"Tabela {table_name} não encontrada em {path}")


def map_pg_to_spark_type(pg_type: str):
    """
    Mapeia tipos Postgres do YAML para tipos Spark.
    Cobre apenas os tipos usados nos metadatas do projeto.
    """
    dt = pg_type.lower()

    if dt.startswith("varchar") or dt == "text":
        return T.StringType()
    if dt.startswith("double"):
        return T.DoubleType()
    if dt in ("int", "integer"):
        return T.IntegerType()
    if dt == "bigint":
        return T.LongType()
    if dt == "boolean":
        return T.BooleanType()
    if dt == "timestamp":
        return T.TimestampType()
    if dt == "serial":
        return T.IntegerType()

    raise ValueError(f"Tipo Postgres não mapeado: {pg_type}")


def expected_schema_from_metadata(table_name: str):
    """
    A partir do YAML, devolve:
      - lista de nomes de colunas esperadas no DF vindo do Spark
      - dict {nome_coluna: tipo_spark_esperado}

    Ignora colunas gerenciadas apenas pelo banco:
      - id
      - created_at
    """
    table_meta = load_table_metadata(table_name)
    cols_meta = table_meta["columns"]

    cols_meta = [
        c for c in cols_meta
        if c["name"] not in ("id", "created_at")
    ]

    expected_names = [c["name"] for c in cols_meta]
    expected_types = {
        c["name"]: map_pg_to_spark_type(c["data_type"])
        for c in cols_meta
    }
    return expected_names, expected_types


def _wrap_as_kafka_source_df(spark, messages):
    """
    Constrói um DataFrame que imita a saída de readStream("kafka"):
      - colunas: key, value, topic, partition, offset, timestamp
      - value é STRING (JSON em string; pode vir inválido)
    """
    schema = T.StructType([
        T.StructField("key", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("topic", T.StringType(), False),
        T.StructField("partition", T.IntegerType(), False),
        T.StructField("offset", T.LongType(), False),
        T.StructField("timestamp", T.TimestampType(), False),
    ])

    rows = []
    for i, msg in enumerate(messages):
        import json as _json
        value = _json.dumps(msg) if isinstance(msg, dict) else msg
        rows.append(Row(
            key=None,
            value=value,
            topic="iot_sensor_data",
            partition=0,
            offset=i,
            timestamp=datetime(2024, 1, 1, 12, 0, i),
        ))

    return spark.createDataFrame(rows, schema=schema)


# =====================================================
#  sensor_readings
# =====================================================

def test_sensor_readings_schema(spark):
    """
    Verifica se o DF gravado em sensor_readings bate com db/metadata/sensor_readings.yml
    (ignorando id e created_at).
    """
    base_msg = {
        "sensor_id": "s-1",
        "sensor_type": "temperature",
        "value": 50.0,
        "status": "active",
        "location": "room-1",
        "manufacturer": "acme",
        "model": "x",
        "unit": "C",
        "min_value": 0.0,
        "max_value": 100.0,
        "timestamp": "2024-01-01T12:00:00Z",
        "maintenance_date": "2024-01-01",
        "installation_date": "2023-01-01",
    }

    messages = [
        dict(base_msg, sensor_id="v-1", value=10.0),
        dict(base_msg, sensor_id="v-2", value=90.0),
    ]

    kafka_df = _wrap_as_kafka_source_df(spark, messages)
    valid_df, invalid_df = kc.split_valid_invalid(kafka_df)
    assert invalid_df.count() == 0

    processed_df = kc.process_sensor_data(valid_df)

    readings_df = processed_df.select(
        "timestamp",
        "sensor_id",
        "sensor_type",
        "value",
        "normalized_value",
        "status",
        "location",
        "manufacturer",
        "model",
        "unit",
        "high_alert",
        "low_alert",
        "processing_time",
    )

    expected_cols, expected_types = expected_schema_from_metadata("sensor_readings")

    assert set(readings_df.columns) == set(expected_cols)

    for field in readings_df.schema.fields:
        exp_type = expected_types[field.name]
        assert isinstance(field.dataType, type(exp_type)), \
            f"Coluna {field.name}: {field.dataType} != {exp_type}"


# =====================================================
#  sensor_aggregations
# =====================================================

def test_sensor_aggregations_schema(spark):
    """
    Verifica se o DF de agregações bate com db/metadata/sensor_aggregations.yml
    (ignorando id e created_at).
    """
    base_msg = {
        "sensor_id": "A",
        "sensor_type": "temperature",
        "value": 10.0,
        "status": "active",
        "location": "SP",
        "manufacturer": "ACME",
        "model": "X",
        "unit": "C",
        "min_value": 0.0,
        "max_value": 100.0,
        "timestamp": "2025-01-01T12:00:10Z",
        "maintenance_date": "2024-01-01",
        "installation_date": "2023-01-01",
    }

    messages = [
        dict(base_msg, value=10.0),
        dict(base_msg, value=20.0, timestamp="2025-01-01T12:00:40Z"),
        dict(base_msg, value=30.0, timestamp="2025-01-01T12:01:05Z"),
    ]

    kafka_df = _wrap_as_kafka_source_df(spark, messages)
    valid_df, invalid_df = kc.split_valid_invalid(kafka_df)
    assert invalid_df.count() == 0

    processed_df = kc.process_sensor_data(valid_df)
    agg_df = kc.create_aggregations(processed_df)

    expected_cols, expected_types = expected_schema_from_metadata("sensor_aggregations")

    assert set(agg_df.columns) == set(expected_cols), \
        f"Esperado {expected_cols}, mas o DF tem {agg_df.columns}"

    for field in agg_df.schema.fields:
        exp_type = expected_types[field.name]
        assert isinstance(field.dataType, type(exp_type)), \
            f"Coluna {field.name}: {field.dataType} != {exp_type}"


# =====================================================
#  sensor_errors (DLQ)
# =====================================================

def test_sensor_errors_schema(spark):
    """
    Verifica se o DF de DLQ (sensor_errors) bate com db/metadata/sensor_errors.yml
    (ignorando id e created_at).
    """
    base_msg = {
        "sensor_id": "i-1",
        "sensor_type": "temperature",
        "value": -1.0,  # abaixo do min -> erro
        "status": "active",
        "location": "room-1",
        "manufacturer": "acme",
        "model": "x",
        "unit": "C",
        "min_value": 0.0,
        "max_value": 100.0,
        "timestamp": "2024-01-01T12:00:00Z",
        "maintenance_date": "2024-01-01",
        "installation_date": "2023-01-01",
    }

    messages = [
        base_msg,
        "THIS IS NOT JSON",   # erro de parse
    ]

    kafka_df = _wrap_as_kafka_source_df(spark, messages)
    _, invalid_df = kc.split_valid_invalid(kafka_df)

    dlq_df = kc.create_dlq_select(invalid_df)

    expected_cols, expected_types = expected_schema_from_metadata("sensor_errors")

    assert set(dlq_df.columns) == set(expected_cols)

    for field in dlq_df.schema.fields:
        exp_type = expected_types[field.name]
        assert isinstance(field.dataType, type(exp_type)), \
            f"Coluna {field.name}: {field.dataType} != {exp_type}"
