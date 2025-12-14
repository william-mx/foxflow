# foxflow/registry.py
from __future__ import annotations
from typing import Callable, Dict, Iterable, Tuple, Any

ParserFn = Callable[[Iterable[Tuple[str, Any, Any]]], Any]  # yields (topic, record, message)

_REGISTRY: Dict[str, ParserFn] = {}

def register(schema: str) -> Callable[[ParserFn], ParserFn]:
    """Decorator to register a parser for a given schema name."""
    def _decorator(fn: ParserFn) -> ParserFn:
        if schema in _REGISTRY:
            raise ValueError(f"Parser already registered for schema '{schema}'")
        _REGISTRY[schema] = fn
        return fn
    return _decorator

def get_parser(schema: str) -> ParserFn:
    if schema not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise KeyError(f"No parser for schema '{schema}'. Available: {available}")
    return _REGISTRY[schema]

def list_schemas() -> list[str]:
    return sorted(_REGISTRY.keys())
