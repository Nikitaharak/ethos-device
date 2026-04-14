"""
Fingerprint Template Helper - Load/Save fingerprint templates from file system
Similar to image_helper.py but for fingerprint templates
"""

import base64
import os
import json
from pathlib import Path
from typing import Optional, Tuple, Dict
import logging

logger = logging.getLogger(__name__)

# Directory for fingerprint templates
FINGERPRINT_ENCODINGS_DIR = Path("fingerprint_encodings")

# Ensure directory exists
FINGERPRINT_ENCODINGS_DIR.mkdir(exist_ok=True)


def get_fingerprint_template_path(emp_id: str) -> Path:
    """Get fingerprint template file path for employee"""
    return FINGERPRINT_ENCODINGS_DIR / f"{emp_id}.dat"


def get_fingerprint_metadata_path(emp_id: str) -> Path:
    """Get fingerprint metadata file path for employee"""
    return FINGERPRINT_ENCODINGS_DIR / f"{emp_id}.json"


def save_fingerprint_template(emp_id: str, template_data: bytes, template_id: int, name: str = None) -> Optional[str]:
    """
    Save fingerprint template to file system
    Args:
        emp_id: Employee ID
        template_data: Raw template bytes from sensor
        template_id: Template slot ID (1-3000)
        name: Optional employee name
    Returns: relative path if successful, None otherwise
    """
    try:
        # Save template data
        template_path = get_fingerprint_template_path(emp_id)
        with open(template_path, 'wb') as f:
            f.write(template_data)

        # Save metadata
        metadata = {
            "emp_id": emp_id,
            "template_id": template_id,
            "name": name or "",
            "file": f"{emp_id}.dat"
        }
        metadata_path = get_fingerprint_metadata_path(emp_id)
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        return f"fingerprint_encodings/{emp_id}.dat"
    except Exception as e:
        logger.error(f"Failed to save fingerprint template for {emp_id}: {e}")
        return None


def load_fingerprint_template(emp_id: str) -> Optional[bytes]:
    """
    Load fingerprint template from file system
    Returns: template bytes if exists, None otherwise
    """
    try:
        template_path = get_fingerprint_template_path(emp_id)
        if template_path.exists():
            with open(template_path, 'rb') as f:
                return f.read()
        return None
    except Exception as e:
        logger.error(f"Failed to load fingerprint template for {emp_id}: {e}")
        return None


def load_fingerprint_metadata(emp_id: str) -> Optional[Dict]:
    """
    Load fingerprint metadata from file system
    Returns: metadata dict if exists, None otherwise
    """
    try:
        metadata_path = get_fingerprint_metadata_path(emp_id)
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                return json.load(f)
        return None
    except Exception as e:
        logger.error(f"Failed to load fingerprint metadata for {emp_id}: {e}")
        return None


def fingerprint_template_exists(emp_id: str) -> bool:
    """Check if fingerprint template file exists for employee"""
    return get_fingerprint_template_path(emp_id).exists()


def delete_fingerprint_template(emp_id: str) -> bool:
    """Delete fingerprint template and metadata files for employee"""
    try:
        template_path = get_fingerprint_template_path(emp_id)
        metadata_path = get_fingerprint_metadata_path(emp_id)

        if template_path.exists():
            template_path.unlink()
        if metadata_path.exists():
            metadata_path.unlink()

        return True
    except Exception as e:
        logger.error(f"Failed to delete fingerprint template for {emp_id}: {e}")
        return False


def save_fingerprint_from_base64(emp_id: str, template_b64: str, template_id: int, name: str = None) -> Optional[str]:
    """
    Save fingerprint template from base64 string
    Returns: template_path if successful, None otherwise
    """
    try:
        template_data = base64.b64decode(template_b64)
        return save_fingerprint_template(emp_id, template_data, template_id, name)
    except Exception as e:
        logger.error(f"Failed to decode/save fingerprint template for {emp_id}: {e}")
        return None


def load_fingerprint_as_base64(emp_id: str) -> Optional[str]:
    """
    Load fingerprint template as base64 string
    Returns: template_b64 if exists, None otherwise
    """
    try:
        template_data = load_fingerprint_template(emp_id)
        if template_data:
            return base64.b64encode(template_data).decode('ascii')
        return None
    except Exception as e:
        logger.error(f"Failed to load/encode fingerprint template for {emp_id}: {e}")
        return None


def get_all_fingerprint_templates() -> Dict[str, Dict]:
    """
    Get all fingerprint templates with metadata
    Returns: dict of {emp_id: metadata}
    """
    templates = {}
    try:
        for metadata_file in FINGERPRINT_ENCODINGS_DIR.glob("*.json"):
            emp_id = metadata_file.stem
            metadata = load_fingerprint_metadata(emp_id)
            if metadata:
                templates[emp_id] = metadata
    except Exception as e:
        logger.error(f"Failed to get all fingerprint templates: {e}")

    return templates


def get_template_id_from_metadata(emp_id: str) -> Optional[int]:
    """
    Get template ID for an employee from metadata
    Returns: template_id if exists, None otherwise
    """
    metadata = load_fingerprint_metadata(emp_id)
    if metadata and 'template_id' in metadata:
        return int(metadata['template_id'])
    return None
