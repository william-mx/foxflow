# foxflow/parsers/imu.py
from __future__ import annotations
import pandas as pd
from foxflow.registry import register

@register("sensor_msgs/Imu")
def parse(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:

        rows.append({
            "timestamp_ns": t_ns,

            # orientation (quaternion)
            "ori_x": ros_message.orientation.x,
            "ori_y": ros_message.orientation.y,
            "ori_z": ros_message.orientation.z,
            "ori_w": ros_message.orientation.w,

            # angular velocity (rad/s)
            "gyro_x": ros_message.angular_velocity.x,
            "gyro_y": ros_message.angular_velocity.y,
            "gyro_z": ros_message.angular_velocity.z,

            # linear acceleration (m/s^2)
            "accel_x": ros_message.linear_acceleration.x,
            "accel_y": ros_message.linear_acceleration.y,
            "accel_z": ros_message.linear_acceleration.z,
        })

    return {"df": pd.DataFrame(rows)}
