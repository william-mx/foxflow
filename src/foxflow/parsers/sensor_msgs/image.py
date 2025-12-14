from __future__ import annotations

import os
import pandas as pd
import cv2
from typing import Iterable, Tuple, Any

from foxflow.registry import register
from foxflow.utils import get_timestamp_ns
from foxflow.utils import decode_ros_image, decode_jpeg


@register("sensor_msgs/Image")
@register("sensor_msgs/CompressedImage")
def parse_image(
    messages: Iterable[Tuple[str, Any, Any]],
    *,
    export: bool = False,
    export_dir: str | None = None,
    return_images: bool = False,
):
    rows = []
    images = [] if return_images else None

    if export:
        if export_dir is None:
            raise ValueError("export_dir must be provided if export=True")
        os.makedirs(export_dir, exist_ok=True)

    for i, (topic, record, msg) in enumerate(messages):
        t_ns = get_timestamp_ns(record, msg)

        rows.append({"timestamp_ns": t_ns})

        if export or return_images:
            if msg.__class__.__name__ == "CompressedImage":
                img, _ = decode_jpeg(record.data)
            else:
                img = decode_ros_image(msg)

            if return_images:
                images.append(img)

            if export:
                path = os.path.join(export_dir, f"{t_ns}.jpg")
                cv2.imwrite(path, img)

    return {'df': pd.DataFrame(rows), 'images': images}

