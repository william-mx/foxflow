# foxflow/parsers/vision_msgs/detection2d.py
from __future__ import annotations

import pandas as pd
from ros2_pydata.types import Detection2DResult
from foxflow.registry import register


def _parse_single_detection(detection, seq: int | None, t_ns: int) -> Detection2DResult:
    if detection.results:
        label = detection.results[0].hypothesis.class_id
        score = float(detection.results[0].hypothesis.score)
    else:
        label = ""
        score = 0.0

    center = detection.bbox.center
    if hasattr(center, "position"):
        cx, cy = center.position.x, center.position.y
    else:
        cx, cy = center.x, center.y

    return Detection2DResult(
        seq=seq,
        timestamp=t_ns / 1e9,
        label=label,
        score=score,
        x=cx,
        y=cy,
        width=detection.bbox.size_x,
        height=detection.bbox.size_y,
    )


@register("vision_msgs/Detection2D")
def parse_detection2d(message_iter, seq: int | None = None) -> dict:
    """
    Returns:
        {
            "df": DataFrame with timestamp_ns, header_stamp_ns, frame_id — for sync,
            "detections": list[Detection2DResult] — for working with the data,
        }
    """
    rows = []
    detections = []

    for t_ns, (_, _, _, ros_message) in message_iter:
        header_stamp_ns = (
            ros_message.header.stamp.sec * 1_000_000_000
            + ros_message.header.stamp.nanosec
        )

        det = _parse_single_detection(ros_message, seq=seq, t_ns=t_ns)
        detections.append(det)

        rows.append({
            "timestamp_ns": t_ns,
            "header_stamp_ns": header_stamp_ns,
            "frame_id": ros_message.header.frame_id,
        })

    return {
        "df": pd.DataFrame(rows),
        "detections": detections,
    }


@register("vision_msgs/Detection2DArray")
def parse_detection2d_array(message_iter, seq: int | None = None) -> dict:
    """
    Returns:
        {
            "df": DataFrame with timestamp_ns, header_stamp_ns, frame_id — one row per array message,
            "detections": list[Detection2DResult] — all detections across all messages,
        }
    """
    rows = []
    detections = []

    for t_ns, (_, _, _, ros_message) in message_iter:
        header_stamp_ns = (
            ros_message.header.stamp.sec * 1_000_000_000
            + ros_message.header.stamp.nanosec
        )

        rows.append({
            "timestamp_ns": t_ns,
            "header_stamp_ns": header_stamp_ns,
            "frame_id": ros_message.header.frame_id,
        })

        for detection in ros_message.detections:
            det = _parse_single_detection(detection, seq=seq, t_ns=t_ns)
            detections.append(det)

    return {
        "df": pd.DataFrame(rows),
        "detections": detections,
    }