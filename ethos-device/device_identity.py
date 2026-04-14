#!/usr/bin/env python3
"""
Lightweight helpers to provide stable device identity for OTA, heartbeats, and control plane calls.
Keeps a persisted UUID on first run and reads optional device/group overrides from env or files.
"""
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Dict, Optional

APP_DIR = Path(__file__).resolve().parent
IDENTITY_FILE = APP_DIR / "device_identity.json"
UUID_FILE = APP_DIR / "device_uuid.txt"
GROUP_FILE = APP_DIR / "device_group.txt"
DEVICE_ID_FILE = APP_DIR / "device_id.txt"


def _get_cpu_serial() -> Optional[str]:
    """Return Raspberry Pi CPU serial if available."""
    try:
        out = subprocess.check_output(["cat", "/proc/cpuinfo"], text=True)
        for line in out.splitlines():
            if line.startswith("Serial"):
                return line.split(":")[1].strip() or None
    except Exception:
        return None
    return None


def _get_or_create_uuid() -> str:
    """Persist a UUID so the device has a stable identity even after reimage."""
    if UUID_FILE.exists():
        try:
            val = UUID_FILE.read_text().strip()
            if val:
                return val
        except Exception:
            pass
    new_val = uuid.uuid4().hex
    try:
        UUID_FILE.write_text(new_val)
    except Exception:
        pass  # do not block if filesystem is read-only at runtime
    return new_val


def _load_file_value(path: Path) -> Optional[str]:
    try:
        if path.exists():
            val = path.read_text().strip()
            return val or None
    except Exception:
        return None
    return None


def load_identity() -> Dict[str, Optional[str]]:
    """
    Build a device identity payload used by OTA + heartbeat:
    - device_id: override via DEVICE_ID env, device_id.txt, or identity file; fallback to hostname
    - group: override via DEVICE_GROUP env, device_group.txt, or identity file
    - serial: Pi CPU serial if available
    - uuid: persisted stable UUID
    """
    base: Dict[str, Optional[str]] = {
        "device_id": os.getenv("DEVICE_ID"),
        "group": os.getenv("DEVICE_GROUP"),
        "serial": None,
        "uuid": None,
    }

    # Merge persisted identity first if present
    if IDENTITY_FILE.exists():
        try:
            stored = json.loads(IDENTITY_FILE.read_text())
            base.update({k: stored.get(k) for k in ("device_id", "group", "serial", "uuid")})
        except Exception:
            pass

    # File overrides
    base["device_id"] = base["device_id"] or _load_file_value(DEVICE_ID_FILE)
    base["group"] = base["group"] or _load_file_value(GROUP_FILE)

    # Hardware identifiers
    base["serial"] = base["serial"] or _get_cpu_serial()
    base["uuid"] = base["uuid"] or _get_or_create_uuid()

    # Last-resort device_id
    if not base["device_id"]:
        base["device_id"] = os.getenv("HOSTNAME") or os.uname().nodename

    return base


def persist_identity(identity: Dict[str, Optional[str]]) -> None:
    """Write identity to disk so subsequent services share the same values."""
    try:
        IDENTITY_FILE.write_text(json.dumps(identity, indent=2))
    except Exception:
        pass

