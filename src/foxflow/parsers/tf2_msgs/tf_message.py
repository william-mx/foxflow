# foxflow/parsers/tf2_msgs/tf_message.py
from __future__ import annotations

import pandas as pd
from foxflow.registry import register


@register("tf2_msgs/TFMessage")
def parse(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        for tf in ros_message.transforms:
            tr = tf.transform.translation
            qr = tf.transform.rotation

            rows.append({
                "timestamp_ns": t_ns,
                "frame_id": tf.header.frame_id,
                "child_frame_id": tf.child_frame_id,
                "tx": tr.x,
                "ty": tr.y,
                "tz": tr.z,
                "qx": qr.x,
                "qy": qr.y,
                "qz": qr.z,
                "qw": qr.w,
            })

    return {"df": pd.DataFrame(rows)}
