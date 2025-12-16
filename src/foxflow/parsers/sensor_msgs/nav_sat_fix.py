# foxflow/parsers/sensor_msgs/nav_sat_fix.py
from __future__ import annotations

import numpy as np
import pandas as pd
from foxflow.registry import register


@register("sensor_msgs/NavSatFix")
def parse(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        cov_flat = list(getattr(ros_message, "position_covariance", []))
        cov = None
        if len(cov_flat) == 9:
            cov = np.asarray(cov_flat, dtype=float).reshape(3, 3)

        status = getattr(ros_message, "status", None)

        rows.append({
            "timestamp_ns": t_ns,
            "latitude_deg": ros_message.latitude,
            "longitude_deg": ros_message.longitude,
            "altitude_m": ros_message.altitude,
            "status": getattr(status, "status", None),
            "service": getattr(status, "service", None),
            "covariance": cov,
            "covariance_type": getattr(ros_message, "position_covariance_type", None),
        })

    return {"df": pd.DataFrame(rows)}
