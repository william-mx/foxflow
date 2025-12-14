# foxflow/parsers/imu.py
from __future__ import annotations
import pandas as pd
from foxflow.registry import register
from foxflow.utils import get_timestamp_ns

@register("sensor_msgs/Imu")
def parse(messages):
    rows = []
    for topic, record, msg in messages:
        t_ns = get_timestamp_ns(record, msg)

        rows.append({
            "timestamp_ns": t_ns,

            # orientation (quaternion)
            "ori_x": msg.orientation.x,
            "ori_y": msg.orientation.y,
            "ori_z": msg.orientation.z,
            "ori_w": msg.orientation.w,

            # angular velocity (rad/s)
            "gyro_x": msg.angular_velocity.x,
            "gyro_y": msg.angular_velocity.y,
            "gyro_z": msg.angular_velocity.z,

            # linear acceleration (m/s^2)
            "accel_x": msg.linear_acceleration.x,
            "accel_y": msg.linear_acceleration.y,
            "accel_z": msg.linear_acceleration.z,
        })
    
    return {"df": pd.DataFrame(rows)}
