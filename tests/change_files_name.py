from pathlib import Path
from os import listdir

BASE_DIR = Path(__file__).resolve().parent.parent
source_directory = BASE_DIR / "data" / "daily_incoming_parquet"

# Recorriendo todos los archivos
for item in source_directory.iterdir():
    # Verificnado archivo parquet
    if item.is_file() and item.suffix == ".parquet":
        # Getting the new name and the new path
        new_file_name = item.stem[5:]
        new_file_path = item.with_name(f"{new_file_name}.parquet")

        # Renaming the file
        print(f"Former file name: {item.stem} | New file name: {new_file_name}")
        item.rename(new_file_path)
