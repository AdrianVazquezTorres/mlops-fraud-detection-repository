import os
import mlflow
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, pandas_udf, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType

# --- PASO CRUCIAL: Forzar la carga de paquetes de Kafka ---
# Esto le dice a Spark qué descargar ANTES de que el motor de ejecución empiece.
# Incluimos 'commons-pool2' que es una dependencia que a veces Spark olvida.
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
    'org.apache.kafka:kafka-clients:3.5.0,'
    'org.apache.commons:commons-pool2:2.11.1 '
    'pyspark-shell'
)


def run_consumer():
    # 1. Configuración Inicial
    # Spark Session
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")  # Para no saturar la terminal con logs de INFO
    logging.info("✅ Spark Session conectada exitosamente.")

    # TODO: Obtenemos la URI de MLflow desde el Docker Compose
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"))
    MODEL_NAME = "ModeloFraude_Pipeline_Primero"
    MODEL_ALIAS = "produccion"
    MODEL_URI = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
    topic_name = "fraud-transaction-stream"

    # 2. Definir esquema (debe coincidir con el Productor y el Modelo (BankTransaction))
    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True)
    ])
    # NOTE: preguntar por la eliminación de @pandas_udf, el topic_name, qué hace y devuelve predict_udf

    # 3. Cargamos el modelo puro de scikit-learn (Igual que en FastAPI)
    logging.info(f"🔄 Cargando el modelo de MLflow desde: {MODEL_URI}")
    # spark_udf = permite que el modelo corra distribuido en todo el clúster de Spark
    predict_udf = mlflow.pyfunc.spark_udf(spark=spark,
                                          model_uri=MODEL_URI,
                                          result_type=DoubleType())

    # 4. Conectar al stream de Kafka y leer datos
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")

    try:
        # Usamos el broker
        logging.info(f"🎧 Escuchando mensajes del tópico: {topic_name}")
        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", broker) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()

        # 5. Trasnformación: Kafka entrega los datos en una columna que nombramos "value" en formato binario
        # Transformación: De Bytes a Columnas Estructuradas
        df_transactions = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # 6. INFERENCIA EN TIEMPO REAL 🧠
        # Empaquetamos todas las columnas en un 'struct' para pasárselo al modelo
        feature_cols = [c.name for c in schema]
        df_predictions = df_transactions.withColumn(
            "prediction_proba",
            predict_udf(
                struct(*[col(c) for c in feature_cols])
            )
        )  # NOTE: Preguntar por el estruck y la nomenclatura de * y **

        # Creamos la bandera final de fraude
        df_final = df_predictions.withColumn(
            "is_fraud_alert_consumer",
            col("prediction_proba") > 0.5
        )

        # 7. Escribir resultados (Por ahora a consola para ver la magia)
        logging.info("🚀 Iniciando Motor de Inferencia Continua...")
        query = df_final.writeStream \
            .outputMode("append") \
            .format("console") \
            .trigger(processingTime="2 seconds") \
            .option("checkpointLocation", "/app/data/checkpoints/fraud_streaming") \
            .start()
        # NOTE: Preguntar por el trigger y option = checkpointLocation

        query.awaitTermination()
    except Exception as e:
        print(f"❌❌❌ ERROR: {e}")


if __name__ == "__main__":
    run_consumer()
