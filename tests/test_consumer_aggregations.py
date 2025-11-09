# tests/test_consumer_aggregations.py
from datetime import datetime

from pyspark.sql import Row, functions as F, types as T

import kafka_consumer_spark as kc

def test_create_aggregations_minute_window(spark):
    # Cria DF "processado" mínimo com as colunas usadas na agregação
    schema = T.StructType([
        T.StructField("timestamp", T.TimestampType(), True),
        T.StructField("sensor_id", T.StringType(), True),
        T.StructField("sensor_type", T.StringType(), True),
        T.StructField("location", T.StringType(), True),
        T.StructField("manufacturer", T.StringType(), True),
        T.StructField("value", T.DoubleType(), True),
        T.StructField("normalized_value", T.DoubleType(), True),
        T.StructField("high_alert", T.BooleanType(), True),
        T.StructField("low_alert", T.BooleanType(), True),
    ])

    t0 = datetime(2025, 1, 1, 12, 0, 10)
    t1 = datetime(2025, 1, 1, 12, 0, 40)  # mesma janela de 1 min
    t2 = datetime(2025, 1, 1, 12, 1, 5)   # próxima janela

    rows = [
        Row(timestamp=t0, sensor_id="A", sensor_type="temperature", location="SP", manufacturer="ACME",
            value=10.0, normalized_value=0.5, high_alert=False, low_alert=True),
        Row(timestamp=t1, sensor_id="A", sensor_type="temperature", location="SP", manufacturer="ACME",
            value=20.0, normalized_value=0.7, high_alert=True, low_alert=False),
        Row(timestamp=t2, sensor_id="A", sensor_type="temperature", location="SP", manufacturer="ACME",
            value=30.0, normalized_value=0.9, high_alert=True, low_alert=False),
    ]

    df = spark.createDataFrame(rows, schema=schema)

    agg = kc.create_aggregations(df)

    out = agg.select(
        F.col("sensor_id"),
        F.col("sensor_type"),
        F.col("location"),
        F.col("manufacturer"),
        F.col("window_start"),
        F.col("window_end"),
        F.col("avg_value"),
        F.col("min_value_reading"),
        F.col("max_value_reading"),
        F.col("reading_count"),
        F.col("high_alert_count"),
        F.col("low_alert_count"),
    ).orderBy("window_start").collect()

    # Duas janelas: [12:00–12:01) e [12:01–12:02)
    assert len(out) == 2

    first = out[0].asDict()
    second = out[1].asDict()

    # Primeira janela tem 2 leituras (t0, t1)
    assert first["reading_count"] == 2
    assert first["min_value_reading"] == 10.0
    assert first["max_value_reading"] == 20.0
    # um high_alert (t1) e um low_alert (t0)
    assert first["high_alert_count"] == 1
    assert first["low_alert_count"] == 1

    # Segunda janela (t2) tem 1 leitura
    assert second["reading_count"] == 1
    assert second["min_value_reading"] == 30.0
    assert second["max_value_reading"] == 30.0
    assert second["high_alert_count"] == 1
    assert second["low_alert_count"] == 0
