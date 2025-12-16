# foxflow/utils/utils.py

import cv2
import struct
import numpy as np


def decode_ros_image(ros_image):
    """
    Decode sensor_msgs/Image into a NumPy array.
    Supports mono8/16, y8/16, rgb8, bgr8, rgba8, bgra8.
    """

    enc = ros_image.encoding
    h, w = ros_image.height, ros_image.width

    dtype = np.uint16 if enc.endswith("16") else np.uint8
    np_arr = np.frombuffer(ros_image.data, dtype=dtype)

    # 1-channel grayscale
    if enc in ("mono8", "mono16", "y8", "y16"):
        img = np_arr.reshape((h, w))

    # 3-channel color
    elif enc in ("rgb8", "bgr8", "rgb16", "bgr16"):
        img = np_arr.reshape((h, w, 3))

    # 4-channel color (with alpha)
    elif enc in ("rgba8", "bgra8", "rgba16", "bgra16"):
        img = np_arr.reshape((h, w, 4))

    else:
        raise ValueError(f"Unsupported image encoding: {enc}")

    return img

def decode_jpeg(ros_message):
    """
    Decode a sensor_msgs/CompressedImage ROS message into a NumPy image.

    Args:
        ros_message: sensor_msgs/CompressedImage

    Returns:
        numpy.ndarray: Decoded image (BGR, OpenCV format)

    Raises:
        ValueError: If the image cannot be decoded.
    """
    data = ros_message.data

    np_arr = np.frombuffer(data, np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    if img is None:
        raise ValueError("Failed to decode JPEG image from CompressedImage message")

    return img