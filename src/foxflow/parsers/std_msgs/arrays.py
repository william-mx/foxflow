# foxflow/parsers/std_msgs/arrays.py
import pandas as pd
from foxflow.registry import register

_ARRAYS = [
    "std_msgs/ByteMultiArray",
    "std_msgs/Float32MultiArray",
    "std_msgs/Float64MultiArray",
    "std_msgs/Int8MultiArray",
    "std_msgs/Int16MultiArray",
    "std_msgs/Int32MultiArray",
    "std_msgs/Int64MultiArray",
    "std_msgs/UInt8MultiArray",
    "std_msgs/UInt16MultiArray",
    "std_msgs/UInt32MultiArray",
    "std_msgs/UInt64MultiArray",
]

def _parse_std_multiarray(message_iter):
    rows = []
    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:

        try:
            dims = [d.size for d in ros_message.layout.dim]
        except Exception:
            dims = []

        rows.append({
            "timestamp_ns": t_ns,
            "data": list(ros_message.data),  # flat
            "dims": dims,                    # optional: helps reconstruct later
        })
    return {"df": pd.DataFrame(rows)}

for schema in _ARRAYS:
    register(schema)(_parse_std_multiarray)
