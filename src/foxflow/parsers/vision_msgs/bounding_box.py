# foxflow/parsers/bounding_box.py
from __future__ import annotations
import pandas as pd
from foxflow.registry import register


@register("vision_msgs/BoundingBox2D")
def parse_bbox2d(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        rows.append({
            "timestamp_ns": t_ns,
            # center pose (x, y in image coordinates)
            "center_x": ros_message.center.x,
            "center_y": ros_message.center.y,
            "center_theta": ros_message.center.theta,
            # size (width, height in pixels)
            "size_x": ros_message.size_x,
            "size_y": ros_message.size_y,
        })

    return {"df": pd.DataFrame(rows)}


@register("vision_msgs/BoundingBox2DArray")
def parse_bbox2d_array(message_iter, id2label=None):
    # Collect data indexed by timestamp and box_id
    data = {}

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        # header timestamp
        header_stamp_ns = ros_message.header.stamp.sec * 1_000_000_000 + ros_message.header.stamp.nsec
        frame_id = ros_message.header.frame_id

        if t_ns not in data:
            data[t_ns] = {
                "timestamp_ns": t_ns,
                "header_stamp_ns": header_stamp_ns,
                "frame_id": frame_id,
            }

        for idx, bbox in enumerate(ros_message.boxes):
            # Get label for this box ID
            label = id2label.get(idx, f"box_{idx}") if id2label else f"box_{idx}"
            
            # Store bbox data with (label, field) as key
            data[t_ns][(label, "center_x")] = bbox.center.x
            data[t_ns][(label, "center_y")] = bbox.center.y
            data[t_ns][(label, "center_theta")] = bbox.center.theta
            data[t_ns][(label, "size_x")] = bbox.size_x
            data[t_ns][(label, "size_y")] = bbox.size_y

    # Convert to DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')
    
    # Separate metadata columns (no multi-level) from bbox columns (multi-level)
    metadata_cols = ["timestamp_ns", "header_stamp_ns", "frame_id"]
    metadata_df = df[metadata_cols] if any(col in df.columns for col in metadata_cols) else pd.DataFrame(index=df.index)
    
    # Get bbox columns (tuples)
    bbox_cols = [col for col in df.columns if col not in metadata_cols]
    bbox_df = df[bbox_cols]
    
    # Create proper multi-index for bbox columns
    bbox_df.columns = pd.MultiIndex.from_tuples(bbox_df.columns)
    
    # Add empty top level to metadata columns
    metadata_df.columns = pd.MultiIndex.from_tuples([("", col) for col in metadata_df.columns])
    
    # Combine metadata and bbox columns
    df = pd.concat([metadata_df, bbox_df], axis=1)
    
    return {"df": df}


@register("vision_msgs/BoundingBox3D")
def parse_bbox3d(message_iter):
    rows = []

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        rows.append({
            "timestamp_ns": t_ns,
            # center pose - position
            "center_pos_x": ros_message.center.position.x,
            "center_pos_y": ros_message.center.position.y,
            "center_pos_z": ros_message.center.position.z,
            # center pose - orientation (quaternion)
            "center_ori_x": ros_message.center.orientation.x,
            "center_ori_y": ros_message.center.orientation.y,
            "center_ori_z": ros_message.center.orientation.z,
            "center_ori_w": ros_message.center.orientation.w,
            # size (x, y, z dimensions)
            "size_x": ros_message.size.x,
            "size_y": ros_message.size.y,
            "size_z": ros_message.size.z,
        })

    return {"df": pd.DataFrame(rows)}


@register("vision_msgs/BoundingBox3DArray")
def parse_bbox3d_array(message_iter, id2label=None):
    # Collect data indexed by timestamp and box_id
    data = {}

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        # header timestamp
        header_stamp_ns = ros_message.header.stamp.sec * 1_000_000_000 + ros_message.header.stamp.nsec
        frame_id = ros_message.header.frame_id

        if t_ns not in data:
            data[t_ns] = {
                "timestamp_ns": t_ns,
                "header_stamp_ns": header_stamp_ns,
                "frame_id": frame_id,
            }

        for idx, bbox in enumerate(ros_message.boxes):
            # Get label for this box ID
            label = id2label.get(idx, f"box_{idx}") if id2label else f"box_{idx}"
            
            # Store bbox data with (label, field) as key
            data[t_ns][(label, "center_pos_x")] = bbox.center.position.x
            data[t_ns][(label, "center_pos_y")] = bbox.center.position.y
            data[t_ns][(label, "center_pos_z")] = bbox.center.position.z
            data[t_ns][(label, "center_ori_x")] = bbox.center.orientation.x
            data[t_ns][(label, "center_ori_y")] = bbox.center.orientation.y
            data[t_ns][(label, "center_ori_z")] = bbox.center.orientation.z
            data[t_ns][(label, "center_ori_w")] = bbox.center.orientation.w
            data[t_ns][(label, "size_x")] = bbox.size.x
            data[t_ns][(label, "size_y")] = bbox.size.y
            data[t_ns][(label, "size_z")] = bbox.size.z

    # Convert to DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')
    
    # Separate metadata columns (no multi-level) from bbox columns (multi-level)
    metadata_cols = ["timestamp_ns", "header_stamp_ns", "frame_id"]
    metadata_df = df[metadata_cols] if any(col in df.columns for col in metadata_cols) else pd.DataFrame(index=df.index)
    
    # Get bbox columns (tuples)
    bbox_cols = [col for col in df.columns if col not in metadata_cols]
    bbox_df = df[bbox_cols]
    
    # Create proper multi-index for bbox columns
    bbox_df.columns = pd.MultiIndex.from_tuples(bbox_df.columns)
    
    # Add empty top level to metadata columns
    metadata_df.columns = pd.MultiIndex.from_tuples([("", col) for col in metadata_df.columns])
    
    # Combine metadata and bbox columns
    df = pd.concat([metadata_df, bbox_df], axis=1)
    
    return {"df": df}