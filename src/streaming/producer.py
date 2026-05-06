import pandas as pd
import json
import time
from confluent_kafka import Producer
from pathlib import Path
# Para revisar esquema
from src.api.schemas import BankTransaction

# 1. Configuración del productor
# 'bootstrap.servers': Dirección del contenedor kafka
conf = {
    'bootstrap.servers': 'localhost:9002',
    'client.id': 'fraud-detector-producer',
    "acks": "1",  # 1 es más rápido que 'all' para baja latencia
    "lingers.ms": 0  # Envío inmediato, no espera a llenar lotes
}

# Inicializamos el objeto Producer
producer = Producer(config=conf)

# Función de confirmación (Callback)


def delivery_report(err, msg):
    """
    Se ejecuta cuando Kafka confirma que recibió el mensaje o si hubo un error.
    """
    if err is not None:
        print(f"❌ Error al entregar mensaje: {err}")
    else:
        print(f"✅ Transacción enviada a {msg.topic()} [Partition: {msg.partition()}]")
        # Mostramos solo los primeros 20 caracteres del mensaje para no saturar la terminal
        # print(f"✅ Enviado: {msg.value().decode('utf-8')[:50]}...")


# 2. Carga un archivo Parquet de ejemplo
file_path = Path('./data/daily_incoming_parquet/transacciones_2026_04_15.parquet')


def run_producer():
    if not file_path.exists():
        print(f"❌ Error: La ruta/archivo no existen --> {file_path}")
        return

    df = pd.read_parquet(file_path)
    # Tomamos una muestra para la prueba del esquema
    sample_data = df.head(50).to_dict(orient="records")

    print("🚀 Iniciando streaming con validación de esquema...")
