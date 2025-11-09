# tests/test_consumer_transform.py
import json
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import kafka_consumer_spark as kc

def _wrap_as_kafka_source_df(spark, rows):
    """
    Monta um DF no formato esperado por read_kafka_stream() -> process_sensor_data():
    - coluna 'value' contendo JSON em string
    - coluna 'timestamp' simulando o timestamp do Kafka
    """
    json_rows = []
    for r in rows:
        json_rows.append(
            Row(
                value=json.dumps(r),
                timestamp=datetime.fromisoformat(r["timestamp"])  # kafka_timestamp simulado
            )
        )
    return spark.createDataFrame(json_rows)

def test_process_sensor_data_filters_and_flags(spark):
    base_ts = datetime(2025, 1, 1, 12, 0, 0).isoformat()

    # Três leituras "temperature" ativas:
    # - uma perto do max para acionar high_alert
    # - uma perto do min para acionar low_alert
    # - uma no meio sem alertas
    temp_attr = kc.sensor_schema  # apenas para referência de campos
    rows = []
    def rec(sensor_type, unit, mn, mx, val, status="active", location="SP", sid="id-1", manuf="ACME", model="M1"):
        return {
            "timestamp": base_ts,
            "sensor_type": sensor_type,
            "value": val,
            "status": status,
            "maintenance_date": None,
            "unit": unit,
            "min_value": mn,
            "max_value": mx,
            "location": location,
            "sensor_id": sid,
            "manufacturer": manuf,
            "model": model,
            "installation_date": "2025-01-01"
        }

    # parâmetros do sensor temperature
    mn, mx = -10.0, 40.0
    rows.extend([
        rec("temperature", "Celsius", mn, mx, mx * 0.95, sid="t-1"),      # high_alert True
        rec("temperature", "Celsius", mn, mx, mn * 1.05, sid="t-2"),      # low_alert True (>= min e < min*1.1)
        rec("temperature", "Celsius", mn, mx, (mn + mx) / 2.0, sid="t-3") # sem alertas
    ])

    # Leituras que devem ser filtradas:
    rows.append(rec("temperature", "Celsius", mn, mx, mx + 1.0, sid="t-4"))      # fora do range -> some
    rows.append(rec("temperature", "Celsius", mn, mx, (mn + mx) / 2.0, status="maintenance", sid="t-5"))  # status != active -> some

    df_kafka_like = _wrap_as_kafka_source_df(spark, rows)
    processed = kc.process_sensor_data(df_kafka_like)

    out = processed.select(
        "sensor_id", "value", "min_value", "max_value",
        "normalized_value", "high_alert", "low_alert", "status"
    ).collect()

    # Devem sobrar apenas t-1, t-2, t-3 (ativas e dentro do range)
    ids = {r["sensor_id"] for r in out}
    assert ids == {"t-1", "t-2", "t-3"}

    by_id = {r["sensor_id"]: r.asDict() for r in out}
    # t-1: valor perto do max => high_alert True
    assert by_id["t-1"]["high_alert"] is True
    assert by_id["t-1"]["low_alert"] is False

    # t-2: valor perto do min (entre min e min*1.1) => low_alert True
    assert by_id["t-2"]["low_alert"] is True
    assert by_id["t-2"]["high_alert"] is False

    # Normalização 0..1
    for r in out:
        nv = r["normalized_value"]
        assert 0.0 <= nv <= 1.0
