# foxflow/parsers/nav_msgs/path.py
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.spatial.transform import Rotation as R
from foxflow.registry import register


@register("nav_msgs/Path")
def parse_path(message_iter) -> dict:
    """
    Returns:
        {
            "df": DataFrame with the message log timestamp_ns,
            "paths": list[np.ndarray] — one Nx10 array per Path message.
                     Columns per path array: 
                     [tx, ty, tz, qx, qy, qz, qw, roll, pitch, yaw]
        }
    """
    rows = []
    paths = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        current_path_poses = []

        # Iterate through all poses inside this specific Path message
        for pose_stamped in ros_message.poses:
            pos = pose_stamped.pose.position
            ori = pose_stamped.pose.orientation

            # Convert quaternion [x, y, z, w] to Euler angles (Roll, Pitch, Yaw)
            q_list = [ori.x, ori.y, ori.z, ori.w]
            rotation = R.from_quat(q_list)
            roll, pitch, yaw = rotation.as_euler("xyz", degrees=False)

            # Store spatial elements for this specific waypoint
            current_path_poses.append([
                pos.x, pos.y, pos.z,
                ori.x, ori.y, ori.z, ori.w,
                roll, pitch, yaw
            ])

        # If the path message happens to be empty, append an empty array with the right column shape
        if current_path_poses:
            paths.append(np.array(current_path_poses, dtype=np.float64))
        else:
            paths.append(np.empty((0, 10), dtype=np.float64))

        # Append to the main dataframe rows using the loop's t_ns
        rows.append({
            "timestamp_ns": t_ns,
            "frame_id": ros_message.header.frame_id,
            "num_poses": len(current_path_poses)
        })

    return {
        "df": pd.DataFrame(rows),
        "paths": paths,
    }