import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from xgboost import XGBClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

# NOTE Importamos desde nuestra nueve estructura
from src.features.transformers import drop_columns, MathTransformations


def create_dummy_data():
    """
    Crea un Datafram sintético pequeño para entrenar el modelo base.
    """
    data = {
        "step": [1, 1, 2, 2, 3],
        "type": ["CASH_OUT", "TRANSFER", "PAYMENT", "CASH_OUT", "TRANSFER"],
        "amount": [500.0, 15000.0, 50.0, 200.0, 50000.0],
        "oldbalanceOrg": [1000.0, 15000.0, 100.0, 200.0, 0.0],
        "newbalanceOrig": [500.0, 0.0, 50.0, 0.0, 0.0],
        "oldbalanceDest": [0.0, 0.0, 0.0, 100.0, 0.0],
        "newbalanceDest": [500.0, 15000.0, 0.0, 300.0, 50000.0],
        # Columnas que vamos a eliminar
        "nameOrig": ["C123", "C456", "C789", "C101", "C112"],
        "nameDest": ["M123", "C999", "M456", "C888", "C777"],
        "isFlaggedFraud": [0, 0, 0, 0, 0],
        # Variable objetivo real
        "isFraud": [0, 1, 0, 0, 1]
    }
    return pd.DataFrame(data=data)


def main():
    print("🚀 Iniciando entrenamiento del modelo Dummy...")

    # 1. Obtener datos
    df = create_dummy_data()
    X = df.drop(columns=["isFraud"])
    y = df["isFraud"].copy()

    # 2. Definir el Pipeline
    # Convertimos la función drop_columns en un paso del pipeline usando FunctionTransformer
    # type la borramos porque XGBoost base prefiere números, y ya filtramos por reglas de negocio
    columnas_a_borrar = ["nameOrig", "nameDest", "isFlaggedFraud", "type"]
    drop_transformer = FunctionTransformer(drop_columns, kw_args={"columns": columnas_a_borrar})

    pipeline = Pipeline(steps=[
        ("drop_cols", drop_transformer),
        ("math_features", MathTransformations()),
        ("classifier", XGBClassifier(n_estimators=10, max_depth=3, random_state=42))
    ])

    # 3. Entrenar
    print("🧠 Entrenando el Pipeline...")
    pipeline.fit(X, y)

    # 4. Guardar el modelo
    # Resolvemos la ruta para guardarlo en la carpeta 'model' de la raíz
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    model_dir = BASE_DIR / "model"
    model_dir.mkdir(exist_ok=True)  # Crea la carpeta si no existe

    model_path = model_dir / "modelo_aml_dummy.joblib"
    joblib.dump(pipeline, model_path)

    print(f"✅ Modelo guardado exitosamente en: {model_path}")


if __name__ == "__main__":
    main()
