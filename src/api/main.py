import joblib  # Ya no es requerida
import pandas as pd
from pathlib import Path
from fastapi import FastAPI, HTTPException

# Importamos nuestros módulos locales
from src.api.schemas import BankTransaction
# Importamos los transformers para que joblib los encuentre al cargar el modelo
from src.features.transformers import drop_columns, MathTransformations

# MLflow
import mlflow
import mlflow.sklearn

app = FastAPI(
    title="AML Fraud Detection API",
    description="API MLOps to fraud detection using XGBoost",
    version="1.0.1"
)


# --- Configuración de MLflow ---
# Construcción de ruta relativa segura (funciona en Windows, Mac, Linux, Docker)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
MLFLOW_DB_PATH = BASE_DIR / "mlflow.db"
TRACKING_URI = f"sqlite:///{MLFLOW_DB_PATH}"

# 2. Definimos el modelo que queremos pedirle al Model Registry
MODEL_NAME = "ModeloFraude_Pipeline_Primero"
MODEL_VERSION = "1"  # También puedes usar latest
MODEL_URI = f"models:/{MODEL_NAME}/{MODEL_VERSION}"

# Variable global del modelo
model = None


@app.on_event("startup")
def load_model():
    """Carga el modelo una sola vez al iniciar el servidor desde MLflow"""
    global model
    try:
        # Le indicamos a la API dónde está el servidor/base de datos de MLflow
        print(f"🔄 Conectando a MLflow en: {TRACKING_URI}")
        mlflow.set_tracking_uri(TRACKING_URI)

        # MLflow se encarga de descargar el artefacto, resolver dependencias y deserializar
        print(f"📥 Descargando modelo: {MODEL_URI}")
        model = mlflow.sklearn.load_model(MODEL_URI)

        print("✅ Modelo cargado exitosamente desde el Model Registry")
    except Exception as e:
        print(f"❌ Error al cargar el modelo desde MLflow: {e}")


@app.get("/")
def health_check():
    """
    Endpoint de estado para comprobar que la API está funcionando.
    """
    return {
        "status": "online",
        "message": "Bienvenido a la API de Detección de fraude (MLOps)",
        "model_loaded": model is not None
    }


@app.post("/predict/")
def predict_fraud(transaction: BankTransaction):
    try:
        # Regla de negocio: Sólo TRANSFER y CASH_OUT pueden ser fraude
        if transaction.type not in ["TRANSFER", "CASH_OUT"]:
            return {
                "status": "SUCCESS",
                "decision": "NOT FRAUD",
                "probability": 0.0,
                "note": "Tipo de transacción inherentemente segura según reglas de negocio"
            }

        # Inferencia
        # Convertimos el esquema Pydantic a un DataFrame de una sola fila
        df_input = pd.DataFrame([transaction.model_dump()])

        # El pipeline de scikit-learn descargado de MLflow ya inclye la limpieza
        probability = round(float(model.predict_proba(df_input)[:, 1][0]), 6)
        prediction = int(probability > 0.5)

        return {
            "status": "SUCCESS",
            "decision": "FRAUD" if prediction == 1 else "NOT FRAUD",
            "probability": probability,
            "prediction": prediction
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en predicción: {str(e)}")
