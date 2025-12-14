# foxflow/parsers/sensor_msgs/pointcloud2.py
from __future__ import annotations

import struct
import numpy as np
import pandas as pd

from foxflow.registry import register
from foxflow.utils import get_timestamp_ns


@register("sensor_msgs/PointCloud2")
def parse(messages_iter, *, include_intensity: bool = True):
    """
    Parse sensor_msgs/PointCloud2 into a DataFrame with one row per message.

    Returns:
        {"df": pd.DataFrame} with columns:
            - timestamp_ns
            - points: np.ndarray shape (N, 3) or (N, 4) float32
    """
    rows = []

    for topic, record, msg in messages_iter:
        t_ns = get_timestamp_ns(record, msg)

        data = msg.data
        point_step = msg.point_step
        num_points = msg.width * msg.height

        pts = []
        for i in range(num_points):
            offset = i * point_step

            # This assumes x,y,z,intensity are packed as 4 little-endian float32 at the start
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

    df = pd.DataFrame(rows)
    return {"df": df}
