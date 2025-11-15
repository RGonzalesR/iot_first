import logging
import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from tools.logger_utils import setup_rotating_log

logger = logging.getLogger("consumer")

# ==================================================
# Funções auxiliares, constantes e schema
# ==================================================
def _read_secret(path="/run/secrets/pg_password"):
    """Lê a senha do PostgreSQL a partir de um Docker Secret. Se estiver em ambiente de testes, retorna uma senha genérica."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        if os.getenv("IOT_FIRST_TEST_MODE") == "1":
            return "default_test_password"
        return None

# Configurações com default unicamente para situações de teste unitário
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:1111")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "teste")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "4242")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "teste_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "teste_user")
POSTGRES_PASSWORD = _read_secret()
CHECKPOINT_DIR = '/tmp/spark-checkpoint'

if not POSTGRES_PASSWORD:
    raise RuntimeError("Nenhuma senha encontrada — defina POSTGRES_PASSWORD ou o secret Docker.")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

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

# =========================
# Funções principais
# =========================
def create_spark_session():
    """Cria sessão Spark com configurações otimizadas para versão 3.4.0."""
    spark = (
        SparkSession.builder
                    .appName("IoT-Sensor-Consumer")
                    .config("spark.jars.packages", 
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                            "org.postgresql:postgresql:42.7.1")
                    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.shuffle.partitions", "10")
                    .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark
        

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

def split_valid_invalid(df):
    """
    Separa mensagens válidas e inválidas.

    Retorna:
      - df_valid: linhas filtradas (status ativo, valor dentro do range)
      - df_invalid: linhas rejeitadas, com motivo
    """
    # 1) Mantém o payload bruto + metadados do Kafka + struct `data`
    base = (
        df.select(
            f.col("value").cast("string").alias("raw_value"),
            f.col("topic"),
            f.col("partition"),
            f.col("offset"),
            f.col("timestamp").alias("kafka_timestamp"),
            f.from_json(f.col("value").cast("string"), sensor_schema).alias("data"),
        )
        .select(
            "raw_value",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "data",      # mantém o struct inteiro
            "data.*",    # explode em colunas planas
        )
    )

    # Condição de "payload completamente vazio" => JSON parse error
    json_error_condition = (
        f.col("timestamp").isNull()
        & f.col("sensor_id").isNull()
        & f.col("sensor_type").isNull()
        & f.col("value").isNull()
        & f.col("status").isNull()
    )

    # Condição de "válido" (a mesma que você já usa hoje)
    valid_condition = (
        (f.col("status") == "active")
        & (f.col("value").isNotNull())
        & (f.col("value") >= f.col("min_value"))
        & (f.col("value") <= f.col("max_value"))
    )

    # DataFrame com marcação de erro
    with_error = (
        base
        .withColumn(
            "error_type",
            f.when(json_error_condition, f.lit("JSON_PARSE_ERROR"))
             .when(f.col("status") != "active", f.lit("INACTIVE_SENSOR"))
             .when(f.col("value").isNull(), f.lit("NULL_VALUE"))
             .when(f.col("value") < f.col("min_value"), f.lit("BELOW_MIN"))
             .when(f.col("value") > f.col("max_value"), f.lit("ABOVE_MAX"))
             .otherwise(f.lit(None))
        )
        .withColumn(
            "error_message",
            f.when(json_error_condition, f.lit("Falha ao fazer parse do JSON"))
             .when(f.col("status") != "active", f.lit("Sensor inativo"))
             .when(f.col("value").isNull(), f.lit("Valor nulo"))
             .when(f.col("value") < f.col("min_value"), f.lit("Valor abaixo do mínimo"))
             .when(f.col("value") > f.col("max_value"), f.lit("Valor acima do máximo"))
             .otherwise(f.lit(None))
        )
    )

    # Válidos: condição de válido e sem erro
    df_valid = with_error.where(valid_condition & f.col("error_type").isNull())

    # Inválidos (DLQ): qualquer linha com error_type definido
    df_invalid = with_error.where(f.col("error_type").isNotNull())

    return df_valid, df_invalid

def process_sensor_data(valid_df):
    """Processa e transforma dados dos sensores (somente válidos)."""
    parsed_filtered_df = (
        valid_df
        .select(
            f.col("sensor_id"),
            f.col("sensor_type"),
            f.col("value"),
            f.col("status"),
            f.col("location"),
            f.col("manufacturer"),
            f.col("model"),
            f.col("unit"),
            f.col("min_value"),
            f.col("max_value"),
            f.col("timestamp"),
            f.col("kafka_timestamp"),
        )
    )

    processed_df = (
        parsed_filtered_df
        .select(
            f.col("sensor_id"),
            f.col("sensor_type"),
            f.col("value"),
            f.col("status"),
            f.col("location"),
            f.col("manufacturer"),
            f.col("model"),
            f.col("unit"),
            f.col("min_value"),
            f.col("max_value"),
            f.to_timestamp(f.col("timestamp")).alias("timestamp"),
            f.to_timestamp(f.col("kafka_timestamp")).alias("kafka_timestamp"),
            f.current_timestamp().alias("processing_time"),
            (
                (f.col("value") - f.col("min_value"))
                / (f.col("max_value") - f.col("min_value"))
            ).alias("normalized_value"),
        )
        .withColumn("low_alert", f.col("normalized_value") < 0.1)
        .withColumn("high_alert", f.col("normalized_value") > 0.9)
    )

    return processed_df

def create_dlq_select(df):
    """Seleciona colunas necessárias para a tabela sensor_errors."""

    dlq_df = (
        df.select(
            f.col("raw_value"),
            f.col("error_type"),
            f.col("error_message"),
            f.col("topic").alias("kafka_topic"),
            f.col("partition").alias("kafka_partition"),
            f.col("offset").alias("kafka_offset"),
            f.to_timestamp("kafka_timestamp").alias("kafka_timestamp"),
        )
    )

    return dlq_df

def create_aggregations(df):
    """Cria agregações por janela de tempo."""
    agg_df = (
        df.withWatermark("timestamp", "2 minutes") \
        .groupBy(
            f.window(f.col("timestamp"), "1 minute"),
            f.col("sensor_id"),
            f.col("sensor_type"),
            f.col("location"),
            f.col("manufacturer")
        )
        .agg(
            f.avg("value").alias("avg_value"),
            f.min("value").alias("min_value_reading"),
            f.max("value").alias("max_value_reading"),
            f.stddev("value").alias("stddev_value"),
            f.count("*").alias("reading_count"),
            f.max("normalized_value").alias("max_normalized"),
            f.sum(f.when(f.col("high_alert"), 1).otherwise(0)).alias("high_alert_count"),
            f.sum(f.when(f.col("low_alert"), 1).otherwise(0)).alias("low_alert_count")
        )
        .select(
            f.col("window.start").alias("window_start"),
            f.col("window.end").alias("window_end"),
            f.col("sensor_id"),
            f.col("sensor_type"),
            f.col("location"),
            f.col("manufacturer"),
            f.col("avg_value"),
            f.col("min_value_reading"),
            f.col("max_value_reading"),
            f.col("stddev_value"),
            f.col("reading_count"),
            f.col("max_normalized"),
            f.col("high_alert_count"),
            f.col("low_alert_count"),
            f.current_timestamp().alias("aggregation_time")
        )
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
            logger.info(f"--> Batch {batch_id} gravado com sucesso em {table_name}")
        except Exception as e:
            logger.info("=" * 60)
            logger.info("=" * 60)
            logger.error(f"Erro ao gravar batch {batch_id} em {table_name}: {str(e)}", exc_info=True)
            raise
    return write_batch

def main():
    """Função principal."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"--> Spark Session criada - Versão: {spark.version}")

    logger.info(f"--> Conectando ao Kafka: {KAFKA_BROKER}")
    kafka_df = read_kafka_stream(spark)

    # separa válidos/ inválidos
    valid_df, dlq_df = split_valid_invalid(kafka_df)

    logger.info(f"--> Processando dados do tópico: {KAFKA_TOPIC}")
    processed_df = process_sensor_data(valid_df)

    logger.info("--> Iniciando stream de dados brutos...")
    raw_query = (
        processed_df
        .select(
            f.col("timestamp"),
            f.col("sensor_id"),
            f.col("sensor_type"),
            f.col("value"),
            f.col("normalized_value"),
            f.col("status"),
            f.col("location"),
            f.col("manufacturer"),
            f.col("model"),
            f.col("unit"),
            f.col("high_alert"),
            f.col("low_alert"),
            f.col("processing_time")
        )
        .writeStream
        .foreachBatch(write_to_postgres(processed_df, "sensor_readings"))
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw")
        .trigger(processingTime='10 seconds')
        .start()
    )

    logger.info("--> Iniciando stream de agregações...")
    agg_df = create_aggregations(processed_df)

    agg_query = (
        agg_df
        .writeStream
        .foreachBatch(write_to_postgres(agg_df, "sensor_aggregations"))
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/agg")
        .trigger(processingTime='30 seconds')
        .start()
    )

    logger.info("--> Iniciando stream de DLQ...")
    final_dlq_df = create_dlq_select(dlq_df)

    dlq_query = (
        final_dlq_df
        .writeStream
        # .foreachBatch(write_dlq_to_postgres)
        .foreachBatch(write_to_postgres(final_dlq_df, "sensor_errors"))
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .trigger(processingTime='10 seconds')
        .start()
    )

    logger.info("=" * 60)
    logger.info("Streams ativos e processando dados!")
    logger.info(f"  → Dados brutos: sensor_readings (a cada 10s)")
    logger.info(f"  → Agregações: sensor_aggregations (a cada 30s)")
    logger.info(f"  → Checkpoint: {CHECKPOINT_DIR}")
    logger.info("=" * 60)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    logger = setup_rotating_log(
        component="consumer",
        default_dir="/app/volumes/spark/logs"
    )
    
    logger.info("=" * 60)
    logger.info("IoT Sensor Consumer")
    logger.info("=" * 60)

    main()
