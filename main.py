import yaml
from ingest.registry import get_reader
from ingest.load_plugins import load_all_readers
from storage.parquet_writer import save_to_parquet
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
config_path = BASE_DIR / "config" / "settings.yaml"

load_all_readers()

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

    for api in config['apis']:
        reader = get_reader(api["type"], url=api["url"])
        df = reader.fetch()
        print(f"Data storing from {api['name']}\n")
        save_to_parquet(df, source_name=api["name"], partition_by_month=api['partition'])