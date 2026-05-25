import os
import mlflow
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, pandas_udf, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType

# Configuración de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- PASO CRUCIAL: Forzar la carga de paquetes de Kafka ---
# Esto le dice a Spark qué descargar ANTES de que el motor de ejecución empiece.
# Incluimos 'commons-pool2' que es una dependencia que a veces Spark olvida.
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
    'org.apache.kafka:kafka-clients:3.5.0,'
    'org.apache.commons:commons-pool2:2.11.1,'
    'org.postgresql:postgresql:42.7.3 '
    'pyspark-shell'
)


def run_consumer():
    # ======== Variables de entorno y credenciales proporcionadas ========
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")  # Conectar a Kafka y leer datos
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
    topic_name = "fraud-transaction-stream"

    # Credenciales de PostgreSQL (Docker Compose)
    db_host = "postgres"  # Nombre del servicio en la red de Docker
    db_port = "5432"
    db_name = "airflow"
    db_user = "airflow"
    db_password = "airflow"

    # Construcción de la URL JDBC estándar
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    # ======== Conectar MLflow ========
    mlflow.set_tracking_uri(mlflow_uri)

    # ======== Inicializar Spark Session ========
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")  # Para no saturar la terminal con logs de INFO
    logging.info("✅ Spark Session con sporte JDBC conectada exitosamente.")

    # ======== Conectar MLflow ========
    MODEL_NAME = "ModeloFraude_Pipeline_Primero"
    MODEL_ALIAS = "produccion"
    MODEL_URI = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
    logging.info(f"🔄 Cargando el modelo de MLflow desde: {MODEL_URI}")
    # spark_udf = permite que el modelo corra distribuido en todo el clúster de Spark
    predict_udf = mlflow.pyfunc.spark_udf(spark=spark,
                                          model_uri=MODEL_URI,
                                          result_type=DoubleType())

    try:
        logging.info(f"🎧 Escuchando mensajes del tópico: {topic_name}")
        # ======== Leemos el stream de kafka ========
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", broker) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()

        # ======== Parsear JSON de Kafka ========
        # Definir esquema (debe coincidir con el Productor y el Modelo (BankTransaction))
        schema = StructType([
            StructField("step", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("oldbalanceOrg", DoubleType(), True),
            StructField("newbalanceOrig", DoubleType(), True),
            StructField("oldbalanceDest", DoubleType(), True),
            StructField("newbalanceDest", DoubleType(), True)
        ])

        # Kafka entrega los datos en una columna que nombramos "value" en formato binario ========
        # Transformación: De Bytes a Columnas Estructuradas
        df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # ======== INFERENCIA EN TIEMPO REAL ========
        # Empaquetamos todas las columnas en un 'struct' para pasárselo al modelo
        feature_cols = [c.name for c in schema]
        df_predictions = df_parsed.withColumn(
            "prediction_proba",
            predict_udf(
                struct(*[col(c) for c in feature_cols])
            )
        )

        # Creamos la bandera (True of False) de fraude
        df_final = df_predictions.withColumn(
            "is_fraud_alert",
            col("prediction_proba") > 0.5
        )

        # ======== Filtro temprano para FRAUDES ========
        # logging.info("🛡️ Aplicando filtro de seguridad en tiempo real...")
        # df_alerts_only = df_final.filter(col("is_fraud_alert") == True)

        # ======== Configurar el SINK directo a postgreSQL vía JDBC ========
        logging.info("🚀 Iniciando Motor de Inferencia Continua con soporte a PostgreSQL...")

        def write_to_jdbc(df, epoch_id):
            # 1. DEBUD VISUAL: Filtramos y mostramos los registros normales en consola
            df_normal = df.filter(col("if_fraud_alert") == False)
            logging.info(f"------ 🛡️ Legitimate Transactions (Lote {epoch_id}) ------")
            df_normal.show(10, truncate=False)

            df_fraud = df.filter(col("is_fraud_alert") == True)
            df_fraud.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "fraud_alerts") \
                .option("user", db_user) \
                .option("password", db_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

        query = df_final.writeStream \
            .foreachBatch(write_to_jdbc) \
            .option("checkpointLocation", "/app/data/checkpoints/fraud_postgres_sink") \
            .outputMode("append") \
            .start()

        # query = df_final.writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .trigger(processingTime="2 seconds") \
        #     .option("checkpointLocation", "/app/data/checkpoints/fraud_streaming") \
        #     .start()

        query.awaitTermination()
    except Exception as e:
        print(f"❌❌❌ ERROR: {e}")


if __name__ == "__main__":
    run_consumer()
