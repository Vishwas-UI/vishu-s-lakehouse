import uuid
from pathlib import Path
import pandas as pd
from datetime import datetime

def save_to_parquet(df: pd.DataFrame, source_name: str, base_dir: str="data", partition_by_month:bool=True) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    now = datetime.now()

    if "id" not in df.columns:
        df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    if "timestamp" not in df.columns:
        df["timestamp"] = ts

    if partition_by_month:
        folder = Path(base_dir)/source_name/f"year={now.year}/month={now.month:02d}"
    else:
        folder = Path(base_dir)/source_name
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder/f"{ts}.parquet"
    df.to_parquet(file_path, index=False, compression="snappy")

    return file_path