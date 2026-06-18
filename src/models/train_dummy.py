# Librerías base para únicamente el modelo
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from xgboost import XGBClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
import os

# Librerías para MLflow
from sklearn.metrics import fbeta_score, average_precision_score, recall_score, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

# NOTE Importamos desde nuestra nueva estructura
from src.features.transformers import drop_columns, MathTransformations

model_info_global = None


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
    print("🚀 Iniciando entrenamiento del modelo Dummy con MLflow...")

    # 1. Obtener datos
    df = create_dummy_data()
    X = df.drop(columns=["isFraud"])
    y = df["isFraud"].copy()

    # --- CONFIGURACIÓN DE MLFLOW ---
    # Debe apuntar a una base de datos (local al inicio, a una de AWS ya en producción)
    # mlflow.set_tracking_uri("sqlite:///mlflow.db")  # Se creó en la raíz al ejecutar por primera vez el servidor de MLflow
    # mlflow.set_tracking_uri("http://localhost:5000")
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://host.docker.internal:5000")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("Fraud_PaysimEC2_2026_06_18_V1")

    # Habilitamos el autologging para capturar parámetros del pipline automáticamente
    mlflow.sklearn.autolog()

    # Iniciamos el run
    with mlflow.start_run(run_name="first_run_in_EC2"):

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

        # 4. Predicciones y métricas (Usamos el mismo X por ser datos dummy)
        y_pred = pipeline.predict(X)
        y_prob = pipeline.predict_proba(X)[:, 1]

        f2 = fbeta_score(y, y_pred, beta=2)
        auc_pr = average_precision_score(y, y_prob)
        recall = recall_score(y, y_pred)

        # Registramos las métricas personalizadas
        mlflow.log_metrics({
            "f2_score_custom": f2,
            "auc_pr_custom": auc_pr,
            "recall_custom": recall
        })

        # 5. Artefactos: Matriz de confusión
        cm = confusion_matrix(y, y_pred)
        plt.figure(figsize=(6, 4))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
        plt.title("Matriz de Confusión Dummy", size=14, weight="bold")
        plt.savefig("Confusion_matrix.png")
        mlflow.log_artifact("Confusion_matrix.png")
        plt.close()

        # 6. Model registry
        # Guardamos el pipeline completo, no sólo el clasificador
        # Esto sustituye el uso de joblib.dump() y el guardarlo de forma local
        # en la carpeta model/

        # Para cargar la versión del modelo automáticamente guardamos el log_model
        # en una variable.
        # mlflow.sklearn.log_model(
        #    sk_model=pipeline,
        #    artifact_path="pipeline_fraude_artifact_primero",
        #    registered_model_name="ModeloFraude_Pipeline_Primero",
        # )
        model_info = mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="modelo_dummy_xgboost",
            registered_model_name="Deteccion_Fraude_PaySim_DUMMY",
        )

        global model_info_global
        model_info_global = model_info

        print(f"💾 Modelo registrado exitosamente con la versión: {model_info.registered_model_version}")

    # 4. Guardar el modelo
    # Resolvemos la ruta para guardarlo en la carpeta 'model' de la raíz
    # BASE_DIR = Path(__file__).resolve().parent.parent.parent
    # model_dir = BASE_DIR / "model"
    # model_dir.mkdir(exist_ok=True)  # Crea la carpeta si no existe

    # model_path = model_dir / "modelo_aml_dummy.joblib"
    # joblib.dump(pipeline, model_path)

    # print(f"✅ Modelo guardado exitosamente en: {model_path}")
    print("✅ Entrenamiento finalizado y registrado en MLflow.")


if __name__ == "__main__":
    main()

    client = MlflowClient()

    client.set_registered_model_alias(
        name="Deteccion_Fraude_PaySim_DUMMY",
        alias="produccion",
        version=model_info_global.registered_model_version)
    print("🔃 ¡Alias asignado con éxito en el servidor!")
