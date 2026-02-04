# foxflow/parsers/label_info.py
from __future__ import annotations
import pandas as pd
from foxflow.registry import register


@register("vision_msgs/LabelInfo")
def parse_label_info(message_iter):
    """
    Parse LabelInfo messages which contain class ID to name mappings.
    
    This parser returns two outputs:
    1. df: A DataFrame with one row per message containing header info and threshold
    2. id2label: A dictionary mapping class IDs to class names (from the latest message)
    """
    rows = []
    id2label_dict = {}

    for t_ns, (schema, channel, mcap_message, ros_message) in message_iter:
        # header timestamp
        header_stamp_ns = ros_message.header.stamp.sec * 1_000_000_000 + ros_message.header.stamp.nanosec
        
        row = {
            "timestamp_ns": t_ns,
            "header_stamp_ns": header_stamp_ns,
            "frame_id": ros_message.header.frame_id,
            "threshold": ros_message.threshold,
            "num_classes": len(ros_message.class_map) if hasattr(ros_message, 'class_map') else 0,
        }
        
        # Extract class mappings from this message
        if hasattr(ros_message, 'class_map') and ros_message.class_map:
            for vision_class in ros_message.class_map:
                class_id = getattr(vision_class, 'class_id', getattr(vision_class, 'id', None))
                class_name = getattr(vision_class, 'class_name', getattr(vision_class, 'name', ''))
                
                if class_id is not None:
                    id2label_dict[class_id] = class_name
                    # Also add individual class info to the row
                    row[f"class_{class_id}_name"] = class_name
        
        rows.append(row)
    
    df = pd.DataFrame(rows)
    
    return {
        "df": df,
        "id2label": id2label_dict
    }