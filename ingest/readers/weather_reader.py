import requests
import pandas as pd
from ingest.base import BaseAPIReader
from ingest.registry import register_reader
from utils.datetimeutils.strtotime import strtotime

@register_reader("open_meteo")
class WeatherAPIReader(BaseAPIReader):
    def __init__(self, url:str):
        self.url = url

    def fetch(self) -> pd.DataFrame:
        try:
            response = requests.get(self.url, timeout=5)
            response.raise_for_status()

            data = response.json()

            current = data.get("current_weather",{})

            record = {
                "timestamp": strtotime(current.get("time")),
                "temperature": current.get("temperature"),
                "windspeed": current.get("windspeed"),
                "winddirection": current.get("winddirection"),
                "is_day": current.get("is_day"),
                "weathercode": current.get("weathercode")
            }

            return pd.DataFrame([record])
        except Exception as e:
            print(f"[ERROR] Weather API Request Error: {e}")
            return pd.DataFrame()