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

def decode_jpeg(data):
    """
    Extract JPEG data from a custom format and decode it into an image.

    This function handles a custom data format where JPEG data is preceded by a metadata header.
    It extracts the JPEG data, decodes it into an image, and returns both the image and metadata.

    Args:
    data (bytes): Raw data containing both custom header and JPEG data.

    Returns:
    tuple: (decoded_image, metadata)
        decoded_image (numpy.ndarray): The decoded image as a NumPy array, or None if decoding fails.
        metadata (dict): A dictionary containing extracted metadata.

    Raises:
    ValueError: If the JPEG start marker is not found in the data.
    """
    # Find the start of the JPEG data (FFD8 marker)
    # The FFD8 marker, represented as b'\xFF\xD8' in bytes, indicates the Start of Image (SOI) in JPEG format.
    # This marker is always present at the beginning of a standard JPEG file.
    # The `find()` method searches for the first occurrence of this byte sequence in the data.
    # It returns the index where the sequence starts, or -1 if not found.
    # This approach allows us to handle cases where the JPEG data is preceded by custom metadata or headers.
    jpeg_start = data.find(b'\xFF\xD8')
    if jpeg_start == -1:
        raise ValueError("JPEG start marker (FFD8) not found. The data may not contain a valid JPEG image.")

    # Extract metadata from the custom header
    custom_header = data[:jpeg_start]
    metadata = {
        'header_size': len(custom_header),
        'jpeg_size': struct.unpack('>I', custom_header[16:20])[0],  # Unpack 4 bytes as big-endian unsigned int
        'format': custom_header[20:24].decode('ascii')  # Decode 4 bytes as ASCII
    }

    # Extract the JPEG data
    jpeg_data = data[jpeg_start:]

    # Decode the JPEG data into an image
    np_arr = np.frombuffer(jpeg_data, np.uint8)
    decoded_image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    return decoded_image, metadata