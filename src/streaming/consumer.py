import os
import mlflow
import mlflow.pyfunc
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, pandas_udf
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
    # 1. Configuración de Spark y MLflow
    # Ya no necesitamos .config("spark.jars.packages") porque usamos la variable de entorno
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Obtenemos la URI de MLflow desde el Docker Compose
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"))
    MODEL_NAME = "ModeloFraude_Pipeline_Primero"
    MODEL_ALIAS = "produccion"
    MODEL_URI = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"

    print(f"🔄 Cargando modelo desde: {MODEL_URI}...")
    # Cargamos el modelo puro de scikit-learn (Igual que en FastAPI)
    model = mlflow.sklearn.load_model(MODEL_URI)
    # Distribuimos el modeo a todos los nodos de Spark usando Broadcast
    broadcast_model = spark.sparkContext.broadcast(model)

    # 2. CREAMOS NUESTRO PROPIO UDF 🧠 (Bypass a la caja negra de MLflow)
    @pandas_udf(DoubleType())
    def predict_udf(step: pd.Series, type_col: pd.Series, amount: pd.Series,
                    oldbalanceOrg: pd.Series, newbalanceOrig: pd.Series,
                    oldbalanceDest: pd.Series, newbalanceDest: pd.Series) -> pd.Series:
        # Construimos el dataframe exactamente como le gusta a el Pipeline
        pandas_df = pd.DataFrame({
            "step": step,
            "type": type_col,
            "amount": amount,
            "oldbalanceOrg": oldbalanceOrg,
            "newbalanceOrig": newbalanceOrig,
            "oldbalanceDest": oldbalanceDest,
            "newbalanceDest": newbalanceDest
        })

        # Usamos el modelo transmitido para predecir
        try:
            model_instance = broadcast_model.value
            proba = model_instance.predict_proba(pandas_df)[:, 1]
            return pd.Series(proba)
        except Exception as e:
            print(f"❌❌❌ ERROR: {e}")

    # 3. Definir esquema (debe coincidir con el Productor y el Modelo (BankTransaction))
    schema = StructType([
        StructField("step", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("oldbalanceOrg", FloatType(), True),
        StructField("newbalanceOrig", FloatType(), True),
        StructField("oldbalanceDest", FloatType(), True),
        StructField("newbalanceDest", FloatType(), True)
    ])

    # 4. Leer el stream de Kafka
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")

    try:
        # Usamos el broker
        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", broker) \
            .option("subscribe", "transactions") \
            .option("startingOffsets", "latest") \
            .load()

        # 5. Trasnformación: Kafka entrega los datos en una columnas llamada "value" en formato binario
        # Debemos castear a String y luego aplicar el formato JSON
        df_transactions = df_raw.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        # 6. INFERENCIA EN TIEMPO REAL 🧠
        # Pasamos las columnas desglosadas a nuestra nueva función
        # El modelo devolverá la probabilidad o clase según cómo lo guardaste
        df_predictions = df_transactions.withColumn(
            "prediction_proba",
            predict_udf(
                col("step"), col("type"), col("amount"), col("oldbalanceOrg"),
                col("newbalanceOrig"), col("oldbalanceDest"), col("newbalanceDest")
            )
        )

        # Añadimos una columna lógica para alertar fraudes (umbral > 0.5)
        df_final = df_predictions.withColumn(
            "is_fraud_alert_consumer",
            col("prediction_proba") > 0.5
        )

        # 7. Escribir resultados (Por ahora a consola para ver la magia)
        query = df_final.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        print("🚀 Sistema de Inferencia en Streaming activo y escuchando...")
        query.awaitTermination()
    except Exception as e:
        print(f"❌❌❌ ERROR: {e}")

    # 5. Sink (Destino): Por ahora, imprimimos en consola para validar
    # print(f"👀 Consumidor conectado a {broker}. Esperando datos...")
    # query = df_json.writeStream
    #    .outputMode("append")
    #    .format("console")
    #    .start()
    # query.awaitTermination()


if __name__ == "__main__":
    run_consumer()
