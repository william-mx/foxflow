# foxflow/parsers/sensor_msgs/magnetic_field.py
from __future__ import annotations

import numpy as np
import pandas as pd
from foxflow.registry import register


@register("sensor_msgs/MagneticField")
def parse(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        mag = ros_message.magnetic_field
        cov_flat = ros_message.magnetic_field_covariance

        # reshape covariance if provided, else None
        cov = None
        if cov_flat and len(cov_flat) == 9:
            cov = np.asarray(cov_flat, dtype=float).reshape(3, 3)

        rows.append({
            "timestamp_ns": t_ns,
            "mag_x": mag.x,
            "mag_y": mag.y,
            "mag_z": mag.z,
            "covariance": cov,
        })

    return {"df": pd.DataFrame(rows)}
