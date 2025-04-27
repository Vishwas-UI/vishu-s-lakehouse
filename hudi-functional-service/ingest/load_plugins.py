import importlib
import pkgutil
import ingest.readers

def load_all_readers():
    package = ingest.readers
    for _, modname, _ in pkgutil.iter_modules(package.__path__):
        importlib.import_module(f"{package.__name__}.{modname}")