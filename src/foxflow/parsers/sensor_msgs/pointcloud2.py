# foxflow/parsers/sensor_msgs/pointcloud2.py
from __future__ import annotations

import struct
import numpy as np
import pandas as pd

from foxflow.registry import register


@register("sensor_msgs/PointCloud2")
def parse(message_iter, *, include_intensity: bool = True):
    """
    Parse sensor_msgs/PointCloud2 into a DataFrame with one row per message.

    Returns:
        {"df": pd.DataFrame} with columns:
            - timestamp_ns
            - points: np.ndarray shape (N, 3) or (N, 4) float32
    """
    rows = []
    
    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:

        data = ros_message.data
        point_step = ros_message.point_step
        num_points = ros_message.width * ros_message.height

        pts = []
        for i in range(num_points):
            offset = i * point_step

            # assumes x, y, z, intensity packed as 4 little-endian float32 at the start
            x, y, z, intensity = struct.unpack_from("<ffff", data, offset)

            if include_intensity:
                pts.append((x, y, z, intensity))
            else:
                pts.append((x, y, z))

        pts_arr = np.asarray(pts, dtype=np.float32)

        rows.append({
            "timestamp_ns": t_ns,
            "points": pts_arr,
        })

    return {"df": pd.DataFrame(rows)}
