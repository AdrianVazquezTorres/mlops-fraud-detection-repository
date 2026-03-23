import joblib
import pandas as pd
from pathlib import Path
from fastapi import FastAPI, HTTPException

# Importamos nuestros módulos locales
from src.api.schemas import BankTransaction
# Importamos los transformers para que joblib los encuentre al cargar el modelo
from src.features.transformers import drop_columns, MathTransformations

app = FastAPI(
    title="AML Fraud Detection API",
    description="API MLOps to fraud detection using XGBoost",
    version="1.0.0"
)

# Construcción de ruta relativa segura (funciona en Windows, Mac, Linux, Docker)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
MODEL_PATH = BASE_DIR / "model" / "nombre_modelo.jolib"  # FIXME

# Variable global del modelo
model = None


@app.on_event("startup")
def load_model():
    """Carga el modelo una sola vez al iniciar el servidor"""
    global model
    try:
        # NOTE Idealmente el modelo guardado ya debe conocer src.features.transformers
        model = joblib.load(MODEL_PATH)
        print("✅ Modelo cargado exitosamente.")
    except Exception as e:
        print(f"❌ Error al cargar el modelo: {e}")


# @app.post("/predict/")
# def predict_fraud(transaction: BankTransaction):
