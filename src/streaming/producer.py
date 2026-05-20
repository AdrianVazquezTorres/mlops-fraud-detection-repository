import pandas as pd
import json
import time
import os
import logging
from confluent_kafka import Producer
from pathlib import Path
# Para revisar esquema
from src.api.schemas import BankTransaction

# Configuración de los logs para producción (reemplaza los print convencionales)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dirección del broker ()
broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic_name = "fraud-transaction-stream"  # Nombre del tópico de Kafka

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
    Se ejecuta de forma asíncrona por cada mensaje cuando kafka confirma su recepción
    """
    if err is not None:
        print(f"❌ Error al entregar mensaje: {err}")
    # else:
        # print(f"✅ Transacción enviada a {msg.topic()} [Partition: {msg.partition()}]")
        # Mostramos solo los primeros 20 caracteres del mensaje para no saturar la terminal
        # print(f"✅ Enviado: {msg.value().decode('utf-8')[:50]}...")


# 2. Producer
def run_producer():
    # Ruta dentro del contenedor (montada vía volumen en Docker-compose)
    file_path = Path("data/daily_incoming_parquet/data_2026_04_15.parquet")

    if not file_path.exists():
        print(f"🚨 No se encontró el archivo/ruta en {file_path}")
        return

    logging.info('Cargando datos históricos en memoria...')
    df = pd.read_parquet(file_path)
    # Tomamos una muestra para la prueba del esquema

    logging.info('🚀 Iniciando simulación de ingesta: 50 transacciones por segundo...')

    try:
        # Bucle infinito para simular un flujo 24/7
        while True:
            for index, row in df.iterrows():
                transaction_dict = row.to_dict()
                json_data = json.dumps(transaction_dict)

                # Enviar mensaje al tópico
                producer.produce(
                    topic=topic_name,
                    value=json_data.encode('utf-8'),
                    callback=delivery_report
                )

                # Permite al producer manejar eventos en segundo plano (como el delivery)
                producer.poll(0)

                # Control de Tasa (Rate Limiting)
                # 1 segundo / 50 = 0.02 segundos de espera entra cada transacción
                time.sleep(0.02)
            logging.info('🔄 Fin del lote alcanzado. Reiniciando el archivo para mantener el streaming...')

    except KeyboardInterrupt:
        logging.warning('🛑 Simulación detenida manualmente por el usuario (KeyboardInterrupt).')
    except Exception as e:
        logging.error(f'⚠️ Error inesperado: {e}')
    finally:
        # Este paso es vital para no perder mensajes que quedaron en memoria antes de apagar
        logging.info('Vaciando la cola de mensajes restantes (flush)...')
        producer.flush()
        logging.info("Productor apagado de forma segura.")


if __name__ == "__main__":
    run_producer()
