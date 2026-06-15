import os
import s3fs
import pandas as pd
import mlflow
import pendulum
from datetime import datetime
from airflow.decorators import dag, task

# =============================================
# CONFIGURACIONES BASE
# =============================================
# URI de MLflow: Ajustar dependiendo si se usa SQLite o el contenedor de MLflow
# MLFLOW_TRACKING_URI = "sqlite:///mlflow.db"
MLFLOW_TRACKING_URI = "http://host.docker.internal:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Rutas dentro del contenedor (mapeadas a tu host)
# BASE_DIR = "/opt/airflow/data"
# INCOMING_DIR = f"{BASE_DIR}/daily_incoming_parquet"
# PROCESSED_DIR = f"{BASE_DIR}/processed"
# RESULTS_DIR = f"{BASE_DIR}/results"

# Bucket inyectado de forma segura a través del archivo .env
# Boto3/S3fs tomarán AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY del entorno automáticamente
BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")

# Aseguramos que las rutas de salida existan
# os.makedirs(name=PROCESSED_DIR, exist_ok=True)
# os.makedirs(name=RESULTS_DIR, exist_ok=True)

default_args = {
    'owner': 'mlops_engineer',
    'retries': 1,
    'depends_on_past': False,
}

# =============================================
# Definición del DAG (Taskflow API)
# =============================================


@dag(
    dag_id='fraude_batch_inference_v1',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2026, 4, 10, tz="UTC"),
    catchup=False,  # Evita que corra todos los días desde el start_date de golpe
    tags=['mlops', 'fraud_detection', 'batch']
)
def fraud_inference_dag():

    @task()
    def check_daily_file(**kwargs) -> str:
        """
        PASO 1: SENSOR (CAPA BRONCE)

        Busca el archivo correspondiente al día.
        Retorna la ruta absoluta del archivo para la siguiente tarea.

        'ds' es la fecha lógica de ejecución de Airflow (ej. '2026-04-15')
        """
        # ds es un string con la fecha de ejecución lógica (YYYY-MM-DD)
        # logical_date = kwargs.get("ds")

        # En producción usaríamos ds.replace('-', '_'), pero para tus pruebas:
        fecha_archivo = "2026_04_15"
        s3_path = f"s3://{BUCKET_NAME}/bronce/{fecha_archivo}.parquet"

        # En el entorno real buscaríamos: f"transacciones_{logical_date}.parquet"
        # Para este proyecto, tomaremos el primer archivos disponible para simular:
        # archivos = [f for f in os.listdir(INCOMING_DIR) if f.endswith('.parquet')]

        # if not archivos:
        #    raise FileNotFoundError("¡No se encontraron datos nuevos para procesar hoy!")

        # archivo_seleccionado = archivos[0]
        # ruta_completa = os.path.join(INCOMING_DIR, archivo_seleccionado)

        fs = s3fs.S3FileSystem()
        if not fs.exists(s3_path):
            raise FileNotFoundError(f"❌ WARNING: The file {s3_path} does not exists or cannot be found...")

        # print(f"{logical_date} Archivo detectado: {ruta_completa}")
        print(f"✅ Archivo encontrado en Capa Bronce: {s3_path}")
        return s3_path

    @task()
    def preprocess_data(s3_input_path: str) -> str:
        """
        PASO 2: LIMPIEZA DEL SISTEMA (TRANSICIÓN BRONCE -> PLATA)

        Lee el archivo crudo, lo limpia y lo guarda en staging.
        """
        print(f"📥 Leyendo desde Bronce: {s3_input_path}")
        df = pd.read_parquet(s3_input_path)

        # Limpieza a nivel Data Engineering (NO feature engineering)
        df = df.drop_duplicates()
        df = df.drop(columns=["isFlaggedFraud", "isFraud"], errors="ignore")

        # -------------------------------------------------------------
        # AQUÍ VA TU LÓGICA DE LIMPIEZA (Ej. eliminar columnas inútiles)
        # Si tu modelo de MLflow ya incluye un ColumnTransformer,
        # puedes saltarte este paso.
        # -------------------------------------------------------------
        # columnas_a_eliminar = ['nameOrig', 'nameDest', 'isFlaggedFraud']
        # columnas_presentes = [col for col in columnas_a_eliminar if col in df.columns]
        # df_clean = df.drop(columns=columnas_presentes)

        # Guardar en disco para la siguiente tarea
        # nombre_base = os.path.basename(file_path)
        # ruta_procesada = os.path.join(PROCESSED_DIR, f"prep_{nombre_base}")
        # df.to_parquet(ruta_procesada, index=False)

        # Construimos la nueva ruta para la capa Plata
        nombre_archivo = s3_input_path.split('/')[-1]
        s3_output_path = f"s3://{BUCKET_NAME}/plata/prep_{nombre_archivo}"

        # Pandas usa s3fs internamente para escribir directo a la nube
        df.to_parquet(s3_output_path)
        print(f"🚀 Datos limpios guardados en Capa Plata: {s3_output_path}")

        return s3_output_path

    @task()
    def predict_batch(s3_input_path: str):
        """
        PASO 3: INFERENCIA MASIVA Y RESULTADOS (CAPA ORO)

        Descarga el modelo de MLflow, hace inferencia y guarda los resultados.
        """
        print(f"📥 Leyendo datos Plata desde: {s3_input_path}")
        df = pd.read_parquet(s3_input_path)

        # -------------------------------------------------------------
        # INTEGRACIÓN CON MLFLOW MODEL REGISTRY
        # Asegúrate de cambiar "ModeloFraude" por el nombre exacto
        # con el que registraste tu modelo en la Fase 1.
        # -------------------------------------------------------------
        model_name = "ModeloFraude_Pipeline_Primero"
        # model_stage = None  # "Production"  # o "None" si aún no usas stages
        # model_version = "1"
        model_alias = "produccion"
        model_uri = f"models:/{model_name}@{model_alias}"

        print(f"🤖 Cargando modelo desde MLflow: {model_uri}")
        model = mlflow.pyfunc.load_model(model_uri)

        print("🤔 Ejecutando inferencia vectorizada...")
        # df = df.drop(columns="isFlaggedFraud", errors="ignore")
        predicciones = model.predict(df)

        # Consolidar resultados
        df_resultados = df.copy()
        df_resultados["prediction_isfraud"] = predicciones
        df_resultados["inference_timestamp"] = datetime.now()

        # Guardar los resultados
        # nombre_base = os.path.basename(processed_file_path)
        # ruta_resultados = os.path.join(RESULTS_DIR, f"results_{nombre_base}")
        # df_resultados.to_parquet(ruta_resultados, index=False)

        # Guardar resultados finales en capa ORO
        nombre_archivo = s3_input_path.split("/")[-1].replace("prep_", "results_")
        s3_output_path = f"s3://{BUCKET_NAME}/oro/{nombre_archivo}"

        df.to_parquet(s3_output_path)
        print(f"¡Éxito! {len(predicciones)} predicciones guardadas en Capa ORO: {s3_output_path}")

    # ==========================================
    # DEFINICIÓN DE DEPENDENCIAS (El Grafo)
    # ==========================================
    ruta_bronce = check_daily_file()
    ruta_plata = preprocess_data(ruta_bronce)
    predict_batch(ruta_plata)


# Instanciar el DAG
dag_instance = fraud_inference_dag()
