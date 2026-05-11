import pandas as pd
import json
import time
import os
from confluent_kafka import Producer
from pathlib import Path
# Para revisar esquema
from src.api.schemas import BankTransaction

broker = os.getenv("KAFKA_BROKER", "kafka:9092")

# 1. Configuración del productor
# 'bootstrap.servers': Dirección del contenedor kafka
conf = {
    'bootstrap.servers': broker,  # "Dirección" para acceder desde afuera (Windows)
    'client.id': 'fraud-detector-producer',
    "acks": "1",  # 1 es más rápido que 'all' para baja latencia
    "linger.ms": 0  # Envío inmediato, no espera a llenar lotes
}

# Inicializamos el objeto Producer
producer = Producer(conf)

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


# 2. Producer
def run_producer():
    # Ruta dentro del contenedor (montada vía volumen en Docker-compose)
    file_path = Path("data/daily_incoming_parquet/data_2026_04_15.parquet")

    if not file_path.exists():
        print(f"🚨 No se encontró el archivo en {file_path}")
        return

    df = pd.read_parquet(file_path)
    # Tomamos una muestra para la prueba del esquema
    sample_data = df.head(50).to_dict(orient="records")

    print("🚀 Iniciando streaming con validación de esquema...")

    try:
        for row in sample_data:
            try:
                # Validamos que la fila cumpla con BankTransaction
                valid_tx = BankTransaction(**row)
                # Convertimos a JSON (usamos .dict() o .model_dump() según la versión de Pydantic)
                payload = valid_tx.model_dump_json().encode('utf-8')

                producer.produce(
                    topic='transactions',
                    value=payload,
                    callback=delivery_report
                )
                producer.poll(0)
                time.sleep(1)  # delay
            except Exception as e:
                print(f"⚠️ Fila inválida saltada. Error: {e}")
                continue
    except KeyboardInterrupt:
        print("\n⛔ Streaming detenido ⛔")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_producer()
