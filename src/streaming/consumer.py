import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Definir el esquema (debe coincidir exactamente con BankTransaction)
schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True)
])


def run_consumer():
    # Usamos la variable de entorno o 'kafka:9092' por defecto
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")

    # 2. Inicializar Spark Session con el paquete de Kafka
    spark = SparkSession.builder \
        .appName("FraudDetectionConsumer") \
        .config("spark.jar.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 3. Leer el flujo de Kafka
    # Usamos el broker
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", "transactions") \
        .load()  # .option("startingOffsets", "earliest") \

    # 4. Trasnformación: Kafka entrega los datos en una columnas llamada "value" en formato binario
    # Debemos castear a String y luego aplicar el formato JSON
    df_json = df_raw.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 5. Sink (Destino): Por ahora, imprimimos en consola para validar
    print(f"👀 Consumidor conectado a {broker}. Esperando datos...")
    query = df_json.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    run_consumer()
