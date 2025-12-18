# foxflow/parsers/geometry_msgs/transform_stamped.py
from __future__ import annotations

import pandas as pd
from foxflow.registry import register


@register("geometry_msgs/TransformStamped")
def parse(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        tr = ros_message.transform.translation
        qr = ros_message.transform.rotation

        rows.append({
            "timestamp_ns": t_ns,
            "frame_id": ros_message.header.frame_id,
            "child_frame_id": ros_message.child_frame_id,
            "tx": tr.x,
            "ty": tr.y,
            "tz": tr.z,
            "qx": qr.x,
            "qy": qr.y,
            "qz": qr.z,
            "qw": qr.w,
        })

    return {"df": pd.DataFrame(rows)}
