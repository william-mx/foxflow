# foxflow/parsers/geometry_msgs/pose.py
from __future__ import annotations

import pandas as pd
from scipy.spatial.transform import Rotation as R
from foxflow.registry import register


@register("geometry_msgs/Pose")
def parse_pose(message_iter) -> dict:
    """
    Parst einen geometry_msgs/Pose Stream und fügt direkt berechnete 
    Euler-Winkel (Roll, Pitch, Yaw) in Radiant hinzu.
    """
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        pos = ros_message.position
        ori = ros_message.orientation

        # 1. Extrahiere das Quaternion in der von SciPy erwarteten Reihenfolge [x, y, z, w]
        q_list = [ori.x, ori.y, ori.z, ori.w]
        
        # 2. Umrechnung in Euler-Winkel (Reihenfolge 'xyz' entspricht Roll, Pitch, Yaw)
        # Falls die Ausrichtung extrem flach/ungültig ist, fängt Scipy das mathematisch ab.
        rotation = R.from_quat(q_list)
        roll, pitch, yaw = rotation.as_euler("xyz", degrees=False)

        rows.append({
            "timestamp_ns": t_ns,
            # Position
            "tx": pos.x,
            "ty": pos.y,
            "tz": pos.z,
            # Quaternionen
            "qx": ori.x,
            "qy": ori.y,
            "qz": ori.z,
            "qw": ori.w,
            # Berechnete Euler-Winkel (in Radiant)
            "roll": roll,
            "pitch": pitch,
            "yaw": yaw,
        })

    return {"df": pd.DataFrame(rows)}