def get_timestamp_ns(record, msg) -> int:
    """
    Return timestamp in nanoseconds.

    Priority:
    1) msg.header.stamp (ROS1 / ROS2)
    2) record.publish_time (MCAP fallback)
    """
    # 1) Try ROS header stamp
    if msg is not None and hasattr(msg, "header"):
        stamp = getattr(msg.header, "stamp", None)
        if stamp is not None:
            # ROS1
            if hasattr(stamp, "secs"):
                return int(stamp.secs * 1e9 + stamp.nsecs)
            # ROS2
            if hasattr(stamp, "sec"):
                return int(stamp.sec * 1e9 + stamp.nanosec)

    # 2) Fallback: MCAP publish_time
    if record is not None and hasattr(record, "publish_time"):
        return int(record.publish_time)

    raise ValueError("No valid timestamp source found")
