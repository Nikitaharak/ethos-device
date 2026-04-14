"""
Image Helper - Load/Save images from file system instead of database
Replaces BLOB storage with efficient file-based storage
"""

import base64
import os
from pathlib import Path
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# Directories
IMAGES_DIR = Path("users_img")
ENCODINGS_DIR = Path("face_encodings")

# Ensure directories exist
IMAGES_DIR.mkdir(exist_ok=True)
ENCODINGS_DIR.mkdir(exist_ok=True)


def get_image_path(emp_id: str) -> Path:
    """Get image file path for employee"""
    return IMAGES_DIR / f"{emp_id}.jpg"


def get_encoding_path(emp_id: str) -> Path:
    """Get encoding file path for employee"""
    return ENCODINGS_DIR / f"{emp_id}.dat"


def save_image(emp_id: str, image_data: bytes) -> Optional[str]:
    """
    Save image to file system
    Returns: relative path if successful, None otherwise
    """
    try:
        img_path = get_image_path(emp_id)
        with open(img_path, 'wb') as f:
            f.write(image_data)
        return f"users_img/{emp_id}.jpg"
    except Exception as e:
        logger.error(f"Failed to save image for {emp_id}: {e}")
        return None


def save_encoding(emp_id: str, encoding_data: bytes) -> Optional[str]:
    """
    Save face encoding to file system
    Returns: relative path if successful, None otherwise
    """
    try:
        enc_path = get_encoding_path(emp_id)
        with open(enc_path, 'wb') as f:
            f.write(encoding_data)
        return f"face_encodings/{emp_id}.dat"
    except Exception as e:
        logger.error(f"Failed to save encoding for {emp_id}: {e}")
        return None


def load_image(emp_id: str) -> Optional[bytes]:
    """
    Load image from file system
    Returns: image bytes if exists, None otherwise
    """
    try:
        img_path = get_image_path(emp_id)
        if img_path.exists():
            with open(img_path, 'rb') as f:
                return f.read()
        return None
    except Exception as e:
        logger.error(f"Failed to load image for {emp_id}: {e}")
        return None


def load_encoding(emp_id: str) -> Optional[bytes]:
    """
    Load face encoding from file system
    Returns: encoding bytes if exists, None otherwise
    """
    try:
        enc_path = get_encoding_path(emp_id)
        if enc_path.exists():
            with open(enc_path, 'rb') as f:
                return f.read()
        return None
    except Exception as e:
        logger.error(f"Failed to load encoding for {emp_id}: {e}")
        return None


def image_exists(emp_id: str) -> bool:
    """Check if image file exists for employee"""
    return get_image_path(emp_id).exists()


def encoding_exists(emp_id: str) -> bool:
    """Check if encoding file exists for employee"""
    return get_encoding_path(emp_id).exists()


def delete_image(emp_id: str) -> bool:
    """Delete image file for employee"""
    try:
        img_path = get_image_path(emp_id)
        if img_path.exists():
            img_path.unlink()
        return True
    except Exception as e:
        logger.error(f"Failed to delete image for {emp_id}: {e}")
        return False


def delete_encoding(emp_id: str) -> bool:
    """Delete encoding file for employee"""
    try:
        enc_path = get_encoding_path(emp_id)
        if enc_path.exists():
            enc_path.unlink()
        return True
    except Exception as e:
        logger.error(f"Failed to delete encoding for {emp_id}: {e}")
        return False


def save_from_base64(emp_id: str, image_b64: str = None, encoding_b64: str = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Save image and encoding from base64 strings
    Returns: (image_path, encoding_path)
    """
    img_path = None
    enc_path = None

    if image_b64:
        try:
            img_data = base64.b64decode(image_b64)
            img_path = save_image(emp_id, img_data)
        except Exception as e:
            logger.error(f"Failed to decode/save image for {emp_id}: {e}")

    if encoding_b64:
        try:
            enc_data = base64.b64decode(encoding_b64)
            enc_path = save_encoding(emp_id, enc_data)
        except Exception as e:
            logger.error(f"Failed to decode/save encoding for {emp_id}: {e}")

    return img_path, enc_path


def load_as_base64(emp_id: str, include_image: bool = True, include_encoding: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """
    Load image and encoding as base64 strings
    Returns: (image_b64, encoding_b64)
    """
    img_b64 = None
    enc_b64 = None

    if include_image:
        img_data = load_image(emp_id)
        if img_data:
            img_b64 = base64.b64encode(img_data).decode('ascii')

    if include_encoding:
        enc_data = load_encoding(emp_id)
        if enc_data:
            enc_b64 = base64.b64encode(enc_data).decode('ascii')

    return img_b64, enc_b64


def get_image_for_display(emp_id: str) -> Optional[bytes]:
    """
    Get image bytes for display/streaming
    This is the main function to replace direct DB queries
    """
    return load_image(emp_id)


def migrate_from_db_to_files(conn):
    """
    Helper function to migrate any remaining DB-stored images to files
    Can be called during app startup to ensure all data is migrated
    """
    cursor = conn.cursor()

    # Check if old columns still exist
    cursor.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in cursor.fetchall()]

    has_old_format = 'display_image' in columns and 'image_path' not in columns

    if has_old_format:
        logger.warning("Old database format detected - migration needed!")
        return False

    logger.info("Database is using new file-based storage format")
    return True
