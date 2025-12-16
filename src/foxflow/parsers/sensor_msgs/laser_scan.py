# foxflow/parsers/laser_scan.py

import pandas as pd
from foxflow.registry import register

@register("sensor_msgs/LaserScan")
def parse(message_iter):
    rows = []
    
    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:

        rows.append({
            "timestamp_ns": t_ns,
            "angle_min": ros_message.angle_min,
            "angle_max": ros_message.angle_max,
            "angle_increment": ros_message.angle_increment,
            "time_increment": ros_message.time_increment,
            "scan_time": ros_message.scan_time,
            "range_min": ros_message.range_min,
            "range_max": ros_message.range_max,
            "ranges": ros_message.ranges,
            "intensities": ros_message.intensities,
        })

    return {"df": pd.DataFrame(rows)}
