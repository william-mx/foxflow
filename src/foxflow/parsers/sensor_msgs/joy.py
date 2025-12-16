# foxflow/parsers/sensor_msgs/joy.py
from __future__ import annotations

import pandas as pd
from foxflow.registry import register


@register("sensor_msgs/Joy")
def parse(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        rows.append({
            "timestamp_ns": t_ns,
            "axes": list(ros_message.axes),
            "buttons": list(ros_message.buttons),
        })

    return {"df": pd.DataFrame(rows)}
