# foxflow/parsers/detection2d.py
from __future__ import annotations

import pandas as pd
from foxflow.registry import register


@register("vision_msgs/Detection2D")
def parse_detection2d(message_iter, id2label=None):
    """
    Parses a single Detection2D. If id2label is provided, 'class_id' is still
    stored as the raw id, but you can optionally map it yourself downstream.
    """
    # Normalize id2label so both int and str keys work
    if id2label:
        id2label = {str(k): v for k, v in dict(id2label).items()}

    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        # header timestamp
        header_stamp_ns = ros_message.header.stamp.sec * 1_000_000_000 + ros_message.header.stamp.nanosec

        # Get the top hypothesis (highest score)
        top_result = None
        if ros_message.results and len(ros_message.results) > 0:
            top_result = max(
                ros_message.results,
                key=lambda x: getattr(getattr(x, "hypothesis", x), "score", 0.0),
            )

        row = {
            "timestamp_ns": t_ns,
            "header_stamp_ns": header_stamp_ns,
            "frame_id": ros_message.header.frame_id,
        }

        # bounding box - center is actually center.position not center.x
        center = ros_message.bbox.center
        if hasattr(center, "position"):
            center_x = getattr(center.position, "x", 0.0)
            center_y = getattr(center.position, "y", 0.0)
        else:
            center_x = getattr(center, "x", 0.0)
            center_y = getattr(center, "y", 0.0)

        center_theta = getattr(center, "theta", 0.0)

        row["bbox_center_x"] = center_x
        row["bbox_center_y"] = center_y
        row["bbox_center_theta"] = center_theta

        # Bounding box size
        row["bbox_size_x"] = ros_message.bbox.size_x
        row["bbox_size_y"] = ros_message.bbox.size_y

        # Add top detection result if available
        if top_result and hasattr(top_result, "hypothesis"):
            class_id = getattr(top_result.hypothesis, "class_id", "")
            row["class_id"] = class_id
            row["score"] = getattr(top_result.hypothesis, "score", 0.0)

            # Optional: also include mapped label (doesn't affect column names here)
            if id2label and class_id != "":
                row["class_label"] = id2label.get(str(class_id), "")

            # Add pose data if available
            if hasattr(top_result, "pose") and hasattr(top_result.pose, "pose"):
                pose = top_result.pose.pose
                if hasattr(pose, "position"):
                    row["detection_pose_x"] = pose.position.x
                    row["detection_pose_y"] = pose.position.y
                    row["detection_pose_z"] = pose.position.z
                if hasattr(pose, "orientation"):
                    row["detection_ori_x"] = pose.orientation.x
                    row["detection_ori_y"] = pose.orientation.y
                    row["detection_ori_z"] = pose.orientation.z
                    row["detection_ori_w"] = pose.orientation.w

        rows.append(row)

    return {"df": pd.DataFrame(rows)}


@register("vision_msgs/Detection2DArray")
def parse_detection2d_array(message_iter, id2label=None):
    # Normalize id2label so both int and str keys work
    if id2label:
        id2label = {str(k): v for k, v in dict(id2label).items()}

    # Collect data indexed by timestamp
    data = {}

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        # header timestamp
        header_stamp_ns = ros_message.header.stamp.sec * 1_000_000_000 + ros_message.header.stamp.nanosec
        frame_id = ros_message.header.frame_id

        if t_ns not in data:
            data[t_ns] = {
                "timestamp_ns": t_ns,
                "header_stamp_ns": header_stamp_ns,
                "frame_id": frame_id,
            }

        # Group detections by class, keeping highest score for each class
        class_detections = {}  # {class_id: detection_data}

        for detection in ros_message.detections:
            # Get the top hypothesis (highest score) for this detection
            top_result = None
            if detection.results and len(detection.results) > 0:
                top_result = max(
                    detection.results,
                    key=lambda x: getattr(getattr(x, "hypothesis", x), "score", 0.0),
                )

            if not top_result or not hasattr(top_result, "hypothesis"):
                continue

            # Extract class_id from hypothesis
            class_id_raw = getattr(top_result.hypothesis, "class_id", None)
            if class_id_raw is None:
                continue

            # Convert class_id to int if it's a string (for stable grouping)
            try:
                class_id = int(class_id_raw) if isinstance(class_id_raw, str) else class_id_raw
            except (ValueError, TypeError):
                continue

            score = getattr(top_result.hypothesis, "score", 0.0)

            # If we already have this class, only keep if this score is higher
            if class_id in class_detections and score <= class_detections[class_id]["score"]:
                continue

            # Extract bounding box data
            center = detection.bbox.center
            if hasattr(center, "position"):
                center_x = getattr(center.position, "x", 0.0)
                center_y = getattr(center.position, "y", 0.0)
            else:
                center_x = getattr(center, "x", 0.0)
                center_y = getattr(center, "y", 0.0)
            center_theta = getattr(center, "theta", 0.0)

            # Extract pose data if available
            pose_data = {}
            if hasattr(top_result, "pose") and hasattr(top_result.pose, "pose"):
                pose = top_result.pose.pose
                if hasattr(pose, "position"):
                    pose_data["pose_x"] = pose.position.x
                    pose_data["pose_y"] = pose.position.y
                    pose_data["pose_z"] = pose.position.z
                if hasattr(pose, "orientation"):
                    pose_data["ori_x"] = pose.orientation.x
                    pose_data["ori_y"] = pose.orientation.y
                    pose_data["ori_z"] = pose.orientation.z
                    pose_data["ori_w"] = pose.orientation.w

            # Store this detection for this class
            class_detections[class_id] = {
                "score": score,
                "bbox_center_x": center_x,
                "bbox_center_y": center_y,
                "bbox_center_theta": center_theta,
                "bbox_size_x": detection.bbox.size_x,
                "bbox_size_y": detection.bbox.size_y,
                **pose_data,
            }

        # Now add all class detections to the data row
        for class_id, detection_data in class_detections.items():
            # Use label as the column header (fallback to class_<id>)
            label = id2label.get(str(class_id), f"class_{class_id}") if id2label else f"class_{class_id}"

            for field, value in detection_data.items():
                data[t_ns][(label, field)] = value

    # Convert to DataFrame
    df = pd.DataFrame.from_dict(data, orient="index")

    # Separate metadata columns (no multi-level) from detection columns (multi-level)
    metadata_cols = ["timestamp_ns", "header_stamp_ns", "frame_id"]
    present_meta = [c for c in metadata_cols if c in df.columns]
    metadata_df = df[present_meta] if present_meta else pd.DataFrame(index=df.index)

    # Get detection columns (tuples)
    detection_cols = [col for col in df.columns if col not in metadata_cols]
    detection_df = df[detection_cols]

    # Create proper multi-index for detection columns
    if len(detection_df.columns) > 0:
        detection_df.columns = pd.MultiIndex.from_tuples(detection_df.columns)

    # Add empty top level to metadata columns
    if len(metadata_df.columns) > 0:
        metadata_df.columns = pd.MultiIndex.from_tuples([("", col) for col in metadata_df.columns])

    # Combine metadata and detection columns
    df = pd.concat([metadata_df, detection_df], axis=1)

    return {"df": df}
