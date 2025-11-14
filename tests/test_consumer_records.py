import json
from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

import kafka_consumer_spark as kc


def _wrap_as_kafka_source_df(spark, messages):
    """
    Constrói um DataFrame que imita a saída de readStream("kafka"):
      - colunas: key, value, topic, partition, offset, timestamp
      - value é STRING (JSON em string; pode vir inválido)
    """

    schema = T.StructType([
        T.StructField("key", T.StringType(), True),         # em Kafka real é binary; aqui string/None
        T.StructField("value", T.StringType(), True),       # string com JSON (ou lixo)
        T.StructField("topic", T.StringType(), False),
        T.StructField("partition", T.IntegerType(), False),
        T.StructField("offset", T.LongType(), False),
        T.StructField("timestamp", T.TimestampType(), False),
    ])

    rows = []
    for i, msg in enumerate(messages):
        value = json.dumps(msg) if isinstance(msg, dict) else msg
        rows.append(Row(
            key=None,
            value=value,
            topic="iot_sensor_data",
            partition=0,
            offset=i,
            timestamp=datetime(2024, 1, 1, 12, 0, i),
        ))

    return spark.createDataFrame(rows, schema=schema)


def test_split_valid_invalid_separa_registros_corretamente(spark):
    """
    Garante que split_valid_invalid devolve:
      - df_valid apenas com leituras ativas e dentro do range
      - df_invalid com as demais, incluindo um JSON inválido
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
    }

    messages = [
        # Válido
        dict(base_msg, sensor_id="v-1", value=10.0),
        # Válido
        dict(base_msg, sensor_id="v-2", value=90.0),
        # Inativo
        dict(base_msg, sensor_id="i-1", status="inactive"),
        # Abaixo do mínimo
        dict(base_msg, sensor_id="i-2", value=-5.0),
        # Acima do máximo
        dict(base_msg, sensor_id="i-3", value=150.0),
        # JSON inválido
        "THIS IS NOT JSON",
    ]

    kafka_df = _wrap_as_kafka_source_df(spark, messages)

    valid_df, invalid_df = kc.split_valid_invalid(kafka_df)

    # Dois válidos: v-1 e v-2
    valid_ids = {r["sensor_id"] for r in valid_df.collect()}
    assert valid_ids == {"v-1", "v-2"}

    # Quatro inválidos: i-1, i-2, i-3 e JSON inválido
    invalid = invalid_df.collect()
    assert len(invalid) == 4

    by_id = {r["sensor_id"]: r for r in invalid if r["sensor_id"] is not None}

    assert by_id["i-1"]["error_type"] == "INACTIVE_SENSOR"
    assert by_id["i-2"]["error_type"] == "BELOW_MIN"
    assert by_id["i-3"]["error_type"] == "ABOVE_MAX"

    # JSON inválido: data == null, mas temos raw_value e error_type adequado
    json_error_rows = [r for r in invalid if r["sensor_id"] is None]
    assert len(json_error_rows) == 1
    je = json_error_rows[0]
    assert je["error_type"] == "JSON_PARSE_ERROR"
    assert "THIS IS NOT JSON" in je["raw_value"]


def test_process_sensor_data_apenas_com_validos(spark):
    """
    Encadeia split_valid_invalid -> process_sensor_data e verifica:
      - só entram IDs válidos
      - normalização em [0, 1]
      - flags de low_alert/high_alert coerentes
    """
    base_msg = {
        "sensor_id": "base",
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
    }

    messages = [
        # valor bem próximo do máximo -> high_alert True
        dict(base_msg, sensor_id="t-1", value=95.0),
        # valor bem próximo do mínimo -> low_alert True
        dict(base_msg, sensor_id="t-2", value=5.0),
        # fora do range -> deve cair no DLQ
        dict(base_msg, sensor_id="bad-1", value=150.0),
    ]

    kafka_df = _wrap_as_kafka_source_df(spark, messages)
    valid_df, invalid_df = kc.split_valid_invalid(kafka_df)

    # Apenas t-1 e t-2 seguem para o processamento
    processed_df = kc.process_sensor_data(valid_df)
    out = processed_df.collect()

    ids = {r["sensor_id"] for r in out}
    assert ids == {"t-1", "t-2"}

    by_id = {r["sensor_id"]: r.asDict() for r in out}

    # t-1: valor perto do max => high_alert True
    assert by_id["t-1"]["high_alert"] is True
    assert by_id["t-1"]["low_alert"] is False

    # t-2: valor perto do min => low_alert True
    assert by_id["t-2"]["low_alert"] is True
    assert by_id["t-2"]["high_alert"] is False

    # Normalização 0..1 e coerente com a fórmula
    for r in out:
        nv = r["normalized_value"]
        assert 0.0 <= nv <= 1.0

        expected = (r["value"] - r["min_value"]) / (r["max_value"] - r["min_value"])
        assert pytest.approx(expected, rel=1e-9) == nv


def test_split_valid_invalid_preserva_metadados_kafka(spark):
    """
    split_valid_invalid deve preservar metadados do Kafka
    (topic, partition, offset, kafka_timestamp) no DF de DLQ.
    """
    msg = {
        "sensor_id": "i-1",
        "sensor_type": "temperature",
        "value": -1.0,  # abaixo do min
        "status": "active",
        "location": "room-1",
        "manufacturer": "acme",
        "model": "x",
        "unit": "C",
        "min_value": 0.0,
        "max_value": 100.0,
        "timestamp": "2024-01-01T12:00:00Z",
    }

    kafka_df = _wrap_as_kafka_source_df(spark, [msg])
    _, invalid_df = kc.split_valid_invalid(kafka_df)

    row = invalid_df.collect()[0]

    # Deve existir raw_value e metadados de Kafka
    assert row["raw_value"] is not None
    assert "topic" in invalid_df.columns
    assert "partition" in invalid_df.columns
    assert "offset" in invalid_df.columns
    assert "kafka_timestamp" in invalid_df.columns
