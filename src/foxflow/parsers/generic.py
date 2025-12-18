# foxflow/parsers/generic.py

import pandas as pd


def _is_primitive(x):
    return x is None or isinstance(x, (bool, int, float, str, bytes))


def _flatten(obj, prefix="", out=None):
    if out is None:
        out = {}

    if _is_primitive(obj):
        out[prefix] = obj
        return out

    # Lists / tuples → store as-is (do NOT recurse)
    if isinstance(obj, (list, tuple)):
        out[prefix] = list(obj)
        return out

    # Dicts
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}_{k}" if prefix else str(k)
            _flatten(v, key, out)
        return out

    # ROS messages (via __slots__)
    slots = getattr(obj, "__slots__", None)
    if slots:
        for name in slots:
            key = f"{prefix}_{name}" if prefix else name
            _flatten(getattr(obj, name), key, out)
        return out

    # Fallback
    out[prefix] = repr(obj)
    return out


def parse_generic(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        row = {"timestamp_ns": t_ns}
        _flatten(ros_message, out=row)
        rows.append(row)

    return {"df": pd.DataFrame(rows)}
