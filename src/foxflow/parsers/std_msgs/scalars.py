# foxflow/parsers/std_msgs/scalars.py
import pandas as pd
from foxflow.registry import register

_SCALARS = [
    "std_msgs/Bool",
    "std_msgs/Byte",
    "std_msgs/Char",
    "std_msgs/Float32",
    "std_msgs/Float64",
    "std_msgs/Int8",
    "std_msgs/Int16",
    "std_msgs/Int32",
    "std_msgs/Int64",
    "std_msgs/UInt8",
    "std_msgs/UInt16",
    "std_msgs/UInt32",
    "std_msgs/UInt64",
    "std_msgs/String",
]

def _parse_std_scalar(messages):
    rows = []
    for topic, record, msg in messages:
        # prefer MCAP publish_time if available (ns), else fallback
        t_ns = getattr(record, "publish_time", None)
        if t_ns is None:
            # last-resort: no timestamp found
            t_ns = None
        rows.append({"timestamp_ns": t_ns, "data": msg.data})
    return {"df": pd.DataFrame(rows)}

for schema in _SCALARS:
    register(schema)(_parse_std_scalar)
