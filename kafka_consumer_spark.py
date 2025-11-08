# kafka_consumer_spark.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Configurações
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'iot_sensor_data'
POSTGRES_URL = 'jdbc:postgresql://postgres:5432/iot_db'
POSTGRES_USER = 'iot_user'
POSTGRES_PASSWORD = 'iot_password'
CHECKPOINT_DIR = '/tmp/spark-checkpoint'

# Schema dos dados do sensor
sensor_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("maintenance_date", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("min_value", DoubleType(), True),
    StructField("max_value", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("model", StringType(), True),
    StructField("installation_date", StringType(), True)
])

def create_spark_session():
    """Cria sessão Spark com configurações otimizadas para versão 3.4.0."""
    return SparkSession.builder \
        .appName("IoT-Sensor-Consumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "org.postgresql:postgresql:42.7.1") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

def read_kafka_stream(spark):
    """Lê stream do Kafka."""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.session.timeout.ms", "30000") \
        .option("kafka.request.timeout.ms", "40000") \
        .load()

def process_sensor_data(df):
    """Processa e transforma dados dos sensores."""
    
    # Parse JSON e converte timestamp
    parsed_df = df.select(
        from_json(col("value").cast("string"), sensor_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Converte timestamps para formato correto
    processed_df = parsed_df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("kafka_timestamp", to_timestamp(col("kafka_timestamp"))) \
        .withColumn("processing_time", current_timestamp())
    
    # Filtragens
    filtered_df = processed_df \
        .filter(col("status") == "active") \
        .filter(col("value").isNotNull()) \
        .filter(col("value") >= col("min_value")) \
        .filter(col("value") <= col("max_value"))
    
    # Adiciona flags de alerta
    alert_df = filtered_df \
        .withColumn("high_alert", 
                   when(col("value") > col("max_value") * 0.9, True).otherwise(False)) \
        .withColumn("low_alert", 
                   when(col("value") < col("min_value") * 1.1, True).otherwise(False))
    
    # Normaliza valores (0-1)
    normalized_df = alert_df \
        .withColumn("normalized_value", 
                   (col("value") - col("min_value")) / (col("max_value") - col("min_value")))
    
    return normalized_df

def create_aggregations(df):
    """Cria agregações por janela de tempo."""
    
    # Agregações por sensor e janela de 1 minuto
    agg_df = df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("sensor_id"),
            col("sensor_type"),
            col("location"),
            col("manufacturer")
        ) \
        .agg(
            avg("value").alias("avg_value"),
            min("value").alias("min_value_reading"),
            max("value").alias("max_value_reading"),
            stddev("value").alias("stddev_value"),
            count("*").alias("reading_count"),
            max("normalized_value").alias("max_normalized"),
            sum(when(col("high_alert"), 1).otherwise(0)).alias("high_alert_count"),
            sum(when(col("low_alert"), 1).otherwise(0)).alias("low_alert_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_id"),
            col("sensor_type"),
            col("location"),
            col("manufacturer"),
            col("avg_value"),
            col("min_value_reading"),
            col("max_value_reading"),
            col("stddev_value"),
            col("reading_count"),
            col("max_normalized"),
            col("high_alert_count"),
            col("low_alert_count"),
            current_timestamp().alias("aggregation_time")
        )
    
    return agg_df

def write_to_postgres(df, table_name, mode="append"):
    """Escreve dados no PostgreSQL com tratamento de erros."""
    
    def write_batch(batch_df, batch_id):
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", 1000) \
                .option("isolationLevel", "READ_COMMITTED") \
                .mode(mode) \
                .save()
            print(f"✓ Batch {batch_id} gravado com sucesso em {table_name}")
        except Exception as e:
            print(f"✗ Erro ao gravar batch {batch_id} em {table_name}: {str(e)}")
            raise
    
    return write_batch

def main():
    """Função principal."""
    print("=" * 60)
    print("IoT Sensor Consumer - Apache Spark 3.4.0")
    print("=" * 60)
    
    # Cria sessão Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✓ Spark Session criada - Versão: {spark.version}")
    
    # Lê stream do Kafka
    print(f"✓ Conectando ao Kafka: {KAFKA_BROKER}")
    kafka_df = read_kafka_stream(spark)
    
    # Processa dados brutos
    print(f"✓ Processando dados do tópico: {KAFKA_TOPIC}")
    processed_df = process_sensor_data(kafka_df)
    
    # Stream 1: Dados brutos processados
    print("✓ Iniciando stream de dados brutos...")
    raw_query = processed_df \
        .select(
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
            "processing_time"
        ) \
        .writeStream \
        .foreachBatch(write_to_postgres(processed_df, "sensor_readings")) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Stream 2: Agregações
    print("✓ Iniciando stream de agregações...")
    agg_df = create_aggregations(processed_df)
    
    agg_query = agg_df \
        .writeStream \
        .foreachBatch(write_to_postgres(agg_df, "sensor_aggregations")) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/agg") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("=" * 60)
    print("✓ Streams ativos e processando dados!")
    print(f"  → Dados brutos: sensor_readings (a cada 10s)")
    print(f"  → Agregações: sensor_aggregations (a cada 30s)")
    print(f"  → Checkpoint: {CHECKPOINT_DIR}")
    print("=" * 60)
    
    # Aguarda término
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
