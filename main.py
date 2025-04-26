import yaml
from ingest.registry import get_reader
from ingest.load_plugins import load_all_readers
from storage.parquet_writer import save_to_parquet
from pathlib import Path
from hudi.producer import hudi_writer

def get_short_path_name(long_name):
    import ctypes
    from ctypes import wintypes, windll

    _GetShortPathNameW = windll.kernel32.GetShortPathNameW
    _GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
    _GetShortPathNameW.restype = wintypes.DWORD

    output_buf_size = 0
    while True:
        output_buf = ctypes.create_unicode_buffer(output_buf_size)
        needed = _GetShortPathNameW(long_name, output_buf, output_buf_size)

        if output_buf_size >= needed:
            return output_buf.value
        else:
            output_buf_size = needed

BASE_DIR = Path(__file__).resolve().parent
config_path = BASE_DIR / "config" / "settings.yaml"

load_all_readers()

with open(config_path, 'r') as f:
    config = yaml.safe_load(f)
    # spark = SparkSession.builder.appName("hudi").getOrCreate()
    spark = hudi_writer.init_spark()
    for api in config['apis']:
        reader = get_reader(api["type"], url=api["url"])
        df = reader.fetch()
        print(f"Data storing from {api['name']}\n")
        parquet_path = save_to_parquet(df, source_name=api["name"], partition_by_month=api['partition'])
        hudi_path = f"hudi_tables/{api['name']}"
        hudi_writer.write_hudi_table(spark,str(parquet_path.parent), hudi_path, table_name = api["name"])

        spark.sql("SHOW TABLES").show()