# foxflow/parsers/laser_scan.py

import pandas as pd
from foxflow.registry import register
from foxflow.utils import get_timestamp_ns

@register("sensor_msgs/LaserScan")
def parse(messages):
    rows = []

    for topic, record, msg in messages:
        t_ns = get_timestamp_ns(record, msg)

        rows.append({
            "timestamp_ns": t_ns,
            "angle_min": msg.angle_min,
            "angle_max": msg.angle_max,
            "angle_increment": msg.angle_increment,
            "time_increment": msg.time_increment,
            "scan_time": msg.scan_time,
            "range_min": msg.range_min,
            "range_max": msg.range_max,
            "ranges": msg.ranges,
            "intensities": msg.intensities,
        })

    return {"df": pd.DataFrame(rows)}
