import os
import boto3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def probar_conexion_s3():
    # 1. Leer el nombre del bucket desde las variables de entorno
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
    if not bucket_name:
        print("❌ Error: No se encontró la variable AWS_S3_BUCKET_NAME")
        return

    print(f"Iniciando la conexión con el bucket S3: {bucket_name}")

    # 2. Inicializar el cliente de AWS S3
    # Boto3 es "mágico": buscará automáticamente las variables AWS_ACCESS_KEY_ID
    # y AWS_SECRET_ACCESS_KEY en el entorno para autenticarse
    s3_client = boto3.client("s3")

    # 3. Definir qué archivo vamos a subir (usaremos uno de tus parquets existentes)
    # Asegúrate de que esta ruta exista en tu entorno local
    local_file_path = Path("data/daily_incoming_parquet/data_2026_04_15.parquet")
    if not Path(local_file_path).exists():
        print(f"❌ Error: No se encontró la ruta/archivo: {local_file_path}")
        return

    # 4. Definiar cómo se llamará el archivo dentro de S3 (S3 KEY)
    # Podemos simular carpetas usando diagonales
    s3_path = "bronce/2026_04_15.parquet"

    # 5. Ejecutar la inyección del archivo al bucket
    try:
        print(f"Subiendo el archivo: {local_file_path}")
        print(f"Nombre del bucket S3: {bucket_name}")
        print(f"Nombre del archivo dentro del bucket: {s3_path}")
        print(f"Ruta del archivo en el bucket: s3://{bucket_name}/{s3_path} ...")

        s3_client.upload_file(
            Filename=local_file_path,
            Bucket=bucket_name,
            Key=s3_path
        )

        print("✅ ¡ÉXITO! El archivo se subió a AWS S3 correctamente.")
    except Exception as e:
        print(f"❌ Falló la subida a S3.\nERROR: {e}")


if __name__ == "__main__":
    probar_conexion_s3()
