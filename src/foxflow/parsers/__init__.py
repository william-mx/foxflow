# foxflow/parsers/__init__.py
from __future__ import annotations
import pkgutil
import importlib

def load_plugins() -> None:
    """Recursively import all parser modules so they can register themselves."""
    for module in pkgutil.walk_packages(__path__, prefix=__name__ + "."):
        importlib.import_module(module.name)
