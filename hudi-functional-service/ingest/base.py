from abc import ABC, abstractmethod
import pandas as pd

class BaseAPIReader(ABC):
    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        pass
