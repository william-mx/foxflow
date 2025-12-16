# foxflow/utils/time.py

def get_timestamp_ns(item) -> int:
    schema, channel, message, decoded_message = item  # len == 4

    # 1) ROS header stamp (if available)
    header = getattr(decoded_message, "header", None)
    if header is not None:
        stamp = getattr(header, "stamp", None)
        if stamp is not None:
            if hasattr(stamp, "secs"):  # ROS1
                return int(stamp.secs * 1_000_000_000 + stamp.nsecs)
            if hasattr(stamp, "sec"):  # ROS2
                return int(stamp.sec * 1_000_000_000 + stamp.nanosec)

    # 2) MCAP message time
    if hasattr(message, "log_time"):
        return int(message.log_time)
    if hasattr(message, "publish_time"):
        return int(message.publish_time)

    raise ValueError("No valid timestamp found")
