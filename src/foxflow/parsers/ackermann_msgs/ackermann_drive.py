# foxflow/parsers/ackermann_msgs/ackermann_drive.py
from __future__ import annotations

import pandas as pd
from foxflow.registry import register


@register("ackermann_msgs/AckermannDriveStamped")
def parse_ackermann_drive_stamped(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        drive = ros_message.drive

        rows.append({
            "timestamp_ns": t_ns,

            "steering_angle": drive.steering_angle,
            "steering_angle_velocity": drive.steering_angle_velocity,
            "speed": drive.speed,
            "acceleration": drive.acceleration,
            "jerk": drive.jerk,
        })

    return {"df": pd.DataFrame(rows)}


@register("ackermann_msgs/AckermannDrive")
def parse_ackermann_drive(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        rows.append({
            "timestamp_ns": t_ns,

            "steering_angle": ros_message.steering_angle,
            "steering_angle_velocity": ros_message.steering_angle_velocity,
            "speed": ros_message.speed,
            "acceleration": ros_message.acceleration,
            "jerk": ros_message.jerk,
        })

    return {"df": pd.DataFrame(rows)}