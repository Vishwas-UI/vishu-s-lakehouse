from typing import Dict, Type
from ingest.base import BaseAPIReader

READERS: Dict[str, Type[BaseAPIReader]] = {}

def register_reader(name: str):
    def wrapper(cls):
        if not issubclass(cls, BaseAPIReader):
            raise ValueError("Registered reader must be a subclass of BaseAPIReader")
        READERS[name] = cls
        return cls
    return wrapper

def get_reader(name: str, **kwargs) -> BaseAPIReader:
    if name not in READERS:
        raise ValueError("Reader not registered")
    return READERS[name](**kwargs)