import os
import io
import re
import json
import time
import base64
import socket
import sqlite3
import zipfile
import hashlib
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable

import requests
from flask import request, jsonify, render_template, send_file, abort


class DeviceConsoleService:
    DEFAULT_TOKEN = os.environ.get("ETHOS_TOKEN", "CHANGE_ME_STRONG_TOKEN")
    HEARTBEAT_SEC = int(os.environ.get("DEVICE_CONSOLE_HEARTBEAT_SEC", "5"))
    HTTP_TIMEOUT_SEC = int(os.environ.get("DEVICE_CONSOLE_HTTP_TIMEOUT_SEC", "5"))

    CFG_ENABLED = "device_console_enabled"
    CFG_SERVER_IP = "device_console_server_ip"
    CFG_SERVER_PORT = "device_console_server_port"
    CFG_GROUP = "device_console_group"
    CFG_NAME = "device_console_name"
    CFG_AUTORECONNECT = "device_console_auto_reconnect"

    def __init__(
        self,
        app,
        get_setting: Callable[[str, Optional[str]], Optional[str]],
        set_setting: Callable[[str, str], None],
        get_self_ip: Callable[[], str],
        get_device_id: Callable[[], str],
        db_path: str,
        app_port: int,
        post_import_reload: Optional[Callable[[], None]] = None,
    ):
        self.app = app
        self.get_setting = get_setting
        self.set_setting = set_setting
        self.get_self_ip = get_self_ip
        self.get_device_id = get_device_id
        self.db_path = str(db_path)
        self.app_port = int(app_port)
        self.post_import_reload = post_import_reload
        # Deferred callback for fingerprint sensor injection
        # Set via set_fingerprint_inject_callback() after app.py defines the function
        # Signature: callback(emp_id: str, fp_bytes: bytes, username: str) -> (bool, int|None, str)
        self._fingerprint_inject_cb: Optional[Callable] = None

        self.root_dir = Path(self.db_path).resolve().parent
        self.asset_slots = self._build_asset_slots()

        self._stop_evt = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._worker_lock = threading.Lock()

        self._status_lock = threading.Lock()
        self._last_ok = False
        self._last_message = "Not announced yet"
        self._last_success_ts = 0
        self._last_attempt_ts = 0
        self._worker_started = False

        # Hardware UUID: read once at init, never changes
        self._hw_uuid: str = self._read_hardware_uuid()

        # Track whether we have sent the full registration (with UUID)
        # to the server since this process started.  Reset on every reboot.
        self._registered_this_boot = False

        self.register_routes()
        self._restore_worker_if_needed()

    # ---------------------------------------------------------------------
    # Generic helpers
    # ---------------------------------------------------------------------
    def _clean(self, v: Any) -> str:
        return str(v or "").strip()

    def _bool(self, v: Any, default=False) -> bool:
        if v is None:
            return bool(default)
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "on")

    def _now_ts(self) -> int:
        return int(time.time())

    def _is_valid_port(self, v: Any) -> bool:
        try:
            p = int(str(v).strip())
            return 1 <= p <= 65535
        except Exception:
            return False

    def _is_valid_ipv4(self, v: str) -> bool:
        v = self._clean(v)
        if not v:
            return False
        if v.lower() == "localhost":
            return True
        parts = v.split(".")
        if len(parts) != 4:
            return False
        try:
            return all(x.isdigit() and 0 <= int(x) <= 255 for x in parts)
        except Exception:
            return False

    def _machine_id(self) -> str:
        for p in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
            try:
                if os.path.exists(p):
                    with open(p, "r", encoding="utf-8") as f:
                        return f.read().strip()
            except Exception:
                pass
        return ""

    def _primary_mac(self) -> str:
        try:
            for iface in os.listdir("/sys/class/net"):
                if iface == "lo":
                    continue
                p = f"/sys/class/net/{iface}/address"
                if os.path.exists(p):
                    with open(p, "r", encoding="utf-8") as f:
                        mac = f.read().strip().lower()
                    if mac and mac != "00:00:00:00:00:00":
                        return mac
        except Exception:
            pass

        try:
            import uuid
            mac = uuid.getnode()
            return ":".join([f"{(mac >> ele) & 0xff:02x}" for ele in range(40, -8, -8)])
        except Exception:
            return "00:00:00:00:00:00"

    @staticmethod
    def _rpi_serial() -> str:
        """
        Read the Raspberry Pi CPU serial number.
        RPi5: /sys/firmware/devicetree/base/serial-number
        RPi4/3: /proc/cpuinfo -> Serial line
        Returns empty string if not on a Raspberry Pi.
        """
        # Method 1: device-tree (RPi5 preferred)
        try:
            dt_path = "/sys/firmware/devicetree/base/serial-number"
            if os.path.exists(dt_path):
                with open(dt_path, "rb") as f:
                    raw = f.read().rstrip(b"\x00").decode("ascii", errors="ignore").strip()
                if raw:
                    return raw
        except Exception:
            pass

        # Method 2: /proc/cpuinfo (RPi3/4)
        try:
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if line.startswith("Serial"):
                        return line.split(":")[-1].strip()
        except Exception:
            pass

        return ""

    def _read_hardware_uuid(self) -> str:
        """
        Build a permanent hardware UUID for this device.
        Priority:
          1. RPi CPU serial (truly unique per board, survives SD card swaps)
          2. /etc/machine-id (unique per OS install)
          3. Fallback: SHA-256 of MAC + hostname
        The UUID is computed ONCE at startup and cached for the process lifetime.
        """
        rpi_serial = self._rpi_serial()
        mac = self._primary_mac()
        machine_id = self._machine_id()

        # Best: RPi serial exists — hash it with MAC for a clean fixed-length ID
        if rpi_serial:
            raw = f"rpi:{rpi_serial}|{mac}"
            hw_uuid = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
            return f"RPI-{hw_uuid.upper()}"

        # Good: /etc/machine-id exists
        if machine_id:
            raw = f"mid:{machine_id}|{mac}"
            hw_uuid = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
            return f"LNX-{hw_uuid.upper()}"

        # Fallback: MAC + hostname
        raw = f"mac:{mac}|{socket.gethostname()}"
        hw_uuid = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return f"DEV-{hw_uuid.upper()}"

    def get_hw_uuid(self) -> str:
        """Return the hardware UUID (read once at init, never changes)."""
        return self._hw_uuid

    def _stable_uuid(self) -> str:
        """
        Returns a stable device identifier.
        Prefers the hardware UUID; falls back to device_id from settings.
        """
        if self._hw_uuid:
            return self._hw_uuid

        try:
            val = self._clean(self.get_device_id())
            if val:
                return val
        except Exception:
            pass

        base = f"{self._machine_id()}|{self._primary_mac()}|{socket.gethostname()}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()[:20]

    def _json_safe_value(self, v: Any) -> Any:
        if isinstance(v, (bytes, bytearray)):
            return {"__b64__": base64.b64encode(v).decode("ascii")}
        return v

    def _user_to_json_safe(self, user: Dict[str, Any]) -> Dict[str, Any]:
        return {k: self._json_safe_value(v) for k, v in user.items()}

    def _json_safe_to_user(self, data: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        for k, v in data.items():
            if isinstance(v, dict) and "__b64__" in v:
                try:
                    out[k] = base64.b64decode(v["__b64__"])
                except Exception:
                    out[k] = None
            else:
                out[k] = v
        return out

    def _safe_json_dumps(self, obj: Any) -> str:
        return json.dumps(obj, indent=2, ensure_ascii=False, default=str)

    def _sanitize_relpath(self, p: str) -> str:
        p = str(p or "").replace("\\", "/").strip("/")
        parts = []
        for x in p.split("/"):
            if x in ("", ".", ".."):
                continue
            parts.append(x)
        return "/".join(parts)

    # ---------------------------------------------------------------------
    # Asset slots
    # ---------------------------------------------------------------------
    def _build_asset_slots(self) -> List[Dict[str, Any]]:
        """
        Slot-based layout so export/import stays stable across devices.

        Slot 0 -> face encodings
        Slot 1 -> user images
        Slot 2 -> fingerprint encodings
        Slot 3 -> alternate fingerprint folder
        """
        slots = [
            {"name": "face_encodings", "dir": self.root_dir / "face_encodings", "patterns": ["{emp_id}.dat"]},
            {"name": "users_img", "dir": self.root_dir / "users_img", "patterns": ["{emp_id}.jpg", "{emp_id}.jpeg", "{emp_id}.png"]},
            {"name": "fingerprint_encodings", "dir": self.root_dir / "fingerprint_encodings", "patterns": ["{emp_id}.dat", "{emp_id}.json"]},
            {"name": "finger_encodings", "dir": self.root_dir / "finger_encodings", "patterns": ["{emp_id}.dat", "{emp_id}.json"]},
        ]

        out = []
        for s in slots:
            p = Path(s["dir"])
            try:
                p.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            out.append({"name": s["name"], "dir": p.resolve(), "patterns": s["patterns"]})
        return out

    # ---------------------------------------------------------------------
    # Config and status
    # ---------------------------------------------------------------------
    def get_config(self) -> Dict[str, Any]:
        enabled = self._bool(self.get_setting(self.CFG_ENABLED, "0"), False)
        server_ip = self._clean(self.get_setting(self.CFG_SERVER_IP, ""))
        server_port = self._clean(self.get_setting(self.CFG_SERVER_PORT, "9000")) or "9000"
        device_group = self._clean(self.get_setting(self.CFG_GROUP, ""))
        device_name = self._clean(self.get_setting(self.CFG_NAME, socket.gethostname()))
        auto_reconnect = self._bool(self.get_setting(self.CFG_AUTORECONNECT, "0"), False)

        configured = bool(server_ip and server_port and device_group and device_name)

        with self._status_lock:
            last_ok = self._last_ok
            last_message = self._last_message
            last_success_ts = self._last_success_ts
            last_attempt_ts = self._last_attempt_ts
            worker_started = self._worker_started

        return {
            "enabled": enabled,
            "server_ip": server_ip,
            "server_port": server_port,
            "device_group": device_group,
            "device_name": device_name,
            "auto_reconnect": auto_reconnect,
            "configured": configured,
            "self_ip": self.get_self_ip(),
            "hw_uuid": self._hw_uuid,
            "device_id": self._stable_uuid(),
            "device_mac": self._primary_mac(),
            "device_api_port": self.app_port,
            "registered_this_boot": self._registered_this_boot,
            "last_ok": last_ok,
            "last_message": last_message,
            "last_success_ts": last_success_ts,
            "last_attempt_ts": last_attempt_ts,
            "worker_started": worker_started,
            "db_path": self.db_path,
            "asset_dirs": [str(x["dir"]) for x in self.asset_slots],
        }

    def save_config(self, enabled, server_ip, server_port, device_group, device_name) -> Dict[str, Any]:
        enabled = self._bool(enabled, False)
        server_ip = self._clean(server_ip)
        server_port = self._clean(server_port) or "9000"
        device_group = self._clean(device_group)
        device_name = self._clean(device_name)

        if enabled:
            if not self._is_valid_ipv4(server_ip):
                raise ValueError("Please enter a valid Console Server IP")
            if not self._is_valid_port(server_port):
                raise ValueError("Please enter a valid Console Server Port")
            if not device_group:
                raise ValueError("Please enter Device Group")
            if not device_name:
                raise ValueError("Please enter Device Name")

        self.set_setting(self.CFG_ENABLED, "1" if enabled else "0")
        self.set_setting(self.CFG_SERVER_IP, server_ip)
        self.set_setting(self.CFG_SERVER_PORT, server_port)
        self.set_setting(self.CFG_GROUP, device_group)
        self.set_setting(self.CFG_NAME, device_name)

        if not enabled:
            self.set_setting(self.CFG_AUTORECONNECT, "0")

        # Compatibility keys
        self.set_setting("console_enabled", "1" if enabled else "0")
        self.set_setting("console_server_ip", server_ip)
        self.set_setting("console_server_port", server_port)
        self.set_setting("console_device_group", device_group)
        self.set_setting("console_device_name", device_name)

        return self.get_config()

    # ---------------------------------------------------------------------
    # Worker lifecycle
    # ---------------------------------------------------------------------
    def _restore_worker_if_needed(self):
        cfg = self.get_config()
        if cfg["enabled"] and cfg["configured"] and cfg["auto_reconnect"]:
            self.ensure_worker_started()

    def ensure_worker_started(self):
        with self._worker_lock:
            if self._worker_thread and self._worker_thread.is_alive():
                return
            self._stop_evt.clear()
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="DeviceConsoleReconnectWorker",
                daemon=True,
            )
            self._worker_thread.start()
            with self._status_lock:
                self._worker_started = True

    def stop_worker(self):
        self.set_setting(self.CFG_AUTORECONNECT, "0")
        self._stop_evt.set()
        with self._status_lock:
            self._worker_started = False
            self._last_ok = False
            self._last_message = "Reconnect thread stopped"

    def _worker_loop(self):
        while not self._stop_evt.is_set():
            try:
                cfg = self.get_config()
                if cfg["enabled"] and cfg["configured"] and cfg["auto_reconnect"]:
                    if not self._registered_this_boot:
                        # First announce after reboot: full registration with hw_uuid
                        ok, _ = self.announce_once(event="register")
                        if ok:
                            self._registered_this_boot = True
                    else:
                        # Subsequent: lightweight heartbeat
                        self.announce_once(event="heartbeat")
            except Exception as e:
                # Never let console worker crash or affect other services
                try:
                    with self._status_lock:
                        self._last_ok = False
                        self._last_message = f"Worker error: {e}"
                except Exception:
                    pass
            self._stop_evt.wait(self.HEARTBEAT_SEC)

    # ---------------------------------------------------------------------
    # Console communication
    # ---------------------------------------------------------------------
    def _user_count(self) -> Optional[int]:
        try:
            conn = self.db_connect()
            _, rows = self.get_users(conn)
            conn.close()
            return len(rows)
        except Exception:
            return None

    def _biometric_counts(self) -> Dict[str, Optional[int]]:
        """Get face, fingerprint, and RFID counts from database."""
        counts = {"face_count": None, "fingerprint_count": None, "rfid_count": None}
        try:
            conn = self.db_connect()

            # Fingerprint count from fingerprint_map
            try:
                row = conn.execute("SELECT COUNT(*) as cnt FROM fingerprint_map").fetchone()
                counts["fingerprint_count"] = row["cnt"] if row else 0
            except Exception:
                counts["fingerprint_count"] = 0

            # Face count from face_encodings directory
            try:
                face_dir = self.root_dir / "face_encodings"
                if face_dir.is_dir():
                    counts["face_count"] = len(list(face_dir.glob("*.dat")))
                else:
                    counts["face_count"] = 0
            except Exception:
                counts["face_count"] = 0

            # RFID count from rfid_card_map
            try:
                row = conn.execute("SELECT COUNT(*) as cnt FROM rfid_card_map").fetchone()
                counts["rfid_count"] = row["cnt"] if row else 0
            except Exception:
                counts["rfid_count"] = 0

            conn.close()
        except Exception:
            pass
        return counts

    def _register_payload(self, event="heartbeat") -> Dict[str, Any]:
        cfg = self.get_config()
        bio = self._biometric_counts()
        return {
            "type": "beacon",
            "event": event,
            "hw_uuid": self._hw_uuid,
            "device_id": cfg["device_id"],
            "mac": cfg["device_mac"],
            "device_name": cfg["device_name"],
            "section": cfg["device_group"],
            "ip": cfg["self_ip"],
            "api_port": cfg["device_api_port"],
            "user_count": self._user_count(),
            "face_count": bio["face_count"],
            "fingerprint_count": bio["fingerprint_count"],
            "rfid_count": bio["rfid_count"],
            "source": "deviceconsole.py",
            "ts": self._now_ts(),
        }

    def test_console_server(self, server_ip: str, server_port: str) -> Tuple[bool, str]:
        server_ip = self._clean(server_ip)
        server_port = self._clean(server_port)

        if not self._is_valid_ipv4(server_ip):
            return False, "Invalid Console Server IP"
        if not self._is_valid_port(server_port):
            return False, "Invalid Console Server Port"

        try:
            s = socket.create_connection((server_ip, int(server_port)), timeout=self.HTTP_TIMEOUT_SEC)
            s.close()
        except Exception as e:
            return False, f"Could not connect to {server_ip}:{server_port} - {e}"

        try:
            r = requests.get(f"http://{server_ip}:{server_port}/api/state", timeout=self.HTTP_TIMEOUT_SEC)
            if r.status_code == 200:
                return True, f"Connected successfully to console server {server_ip}:{server_port}"
            return True, f"TCP connected, but /api/state returned HTTP {r.status_code}"
        except Exception:
            return True, f"TCP connected successfully to {server_ip}:{server_port}"

    def announce_once(self, event="heartbeat") -> Tuple[bool, str]:
        cfg = self.get_config()

        with self._status_lock:
            self._last_attempt_ts = self._now_ts()

        if not cfg["enabled"]:
            with self._status_lock:
                self._last_ok = False
                self._last_message = "Device Console is disabled"
            return False, "Device Console is disabled"

        if not cfg["configured"]:
            with self._status_lock:
                self._last_ok = False
                self._last_message = "Device Console configuration is incomplete"
            return False, "Device Console configuration is incomplete"

        url = f"http://{cfg['server_ip']}:{cfg['server_port']}/api/device_console/register"
        payload = self._register_payload(event=event)

        try:
            r = requests.post(url, json=payload, timeout=self.HTTP_TIMEOUT_SEC)
            if 200 <= r.status_code < 300:
                msg = f"Announced successfully to {cfg['server_ip']}:{cfg['server_port']}"
                with self._status_lock:
                    self._last_ok = True
                    self._last_message = msg
                    self._last_success_ts = self._now_ts()
                return True, msg

            msg = f"Console server returned HTTP {r.status_code}"
            with self._status_lock:
                self._last_ok = False
                self._last_message = msg
            return False, msg

        except Exception as e:
            msg = f"Announce failed: {e}"
            with self._status_lock:
                self._last_ok = False
                self._last_message = msg
            return False, msg

    # ---------------------------------------------------------------------
    # Security for sync APIs
    # ---------------------------------------------------------------------
    def _request_client_ip(self) -> str:
        xf = request.headers.get("X-Forwarded-For", "")
        if xf:
            return xf.split(",")[0].strip()
        return (request.remote_addr or "").strip()

    def require_token_and_console_ip(self):
        token = request.headers.get("X-ETHOS-TOKEN", "")
        if token != self.DEFAULT_TOKEN:
            abort(401, description="Invalid X-ETHOS-TOKEN")

        cfg = self.get_config()
        client_ip = self._request_client_ip()

        allowed = {
            "127.0.0.1",
            "::1",
            "localhost",
            self.get_self_ip(),
        }
        if cfg["server_ip"]:
            allowed.add(cfg["server_ip"])

        if client_ip and client_ip not in allowed:
            abort(403, description=f"Client not allowed: {client_ip}")

    # ---------------------------------------------------------------------
    # DB helpers
    # ---------------------------------------------------------------------
    def db_connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=20, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def detect_users_table(self, conn: sqlite3.Connection) -> str:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        candidates = []

        for r in tables:
            t = r["name"]
            try:
                cols = [c["name"].lower() for c in conn.execute(f"PRAGMA table_info({t})").fetchall()]
            except Exception:
                continue

            if any(x in cols for x in ["emp_id", "employee_id", "id"]) and any(
                x in cols for x in ["name", "emp_name", "employee_name"]
            ):
                candidates.append(t)

        for preferred in ("users", "employee", "employees"):
            for c in candidates:
                if c.lower() == preferred:
                    return c

        if candidates:
            return candidates[0]
        if tables:
            return tables[0]["name"]

        raise RuntimeError("No SQLite tables found")

    def get_table_columns(self, conn: sqlite3.Connection, table: str) -> List[str]:
        return [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]

    def find_key_column(self, cols: List[str]) -> Optional[str]:
        for c in ("emp_id", "employee_id", "id"):
            if c in cols:
                return c
        return None

    def get_users(self, conn: sqlite3.Connection) -> Tuple[str, List[Dict[str, Any]]]:
        table = self.detect_users_table(conn)
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        return table, [dict(r) for r in rows]

    def get_user_key(self, user: Dict[str, Any]) -> str:
        return str(user.get("emp_id") or user.get("employee_id") or user.get("id") or "").strip()

    def upsert_user(self, conn: sqlite3.Connection, table: str, user: Dict[str, Any]) -> str:
        cols = self.get_table_columns(conn, table)
        filtered = {k: v for k, v in user.items() if k in cols}
        if not filtered:
            raise RuntimeError("No matching columns found for import")

        key_col = self.find_key_column(cols)
        if key_col and filtered.get(key_col) not in (None, ""):
            key_val = filtered[key_col]
            exists = conn.execute(f"SELECT 1 FROM {table} WHERE {key_col}=? LIMIT 1", (key_val,)).fetchone()
            if exists:
                set_cols = [k for k in filtered.keys() if k != key_col]
                if set_cols:
                    set_sql = ", ".join([f"{k}=?" for k in set_cols])
                    vals = [filtered[k] for k in set_cols] + [key_val]
                    conn.execute(f"UPDATE {table} SET {set_sql} WHERE {key_col}=?", vals)
                return "updated"

            col_sql = ", ".join(filtered.keys())
            q_sql = ", ".join(["?"] * len(filtered))
            conn.execute(f"INSERT INTO {table} ({col_sql}) VALUES ({q_sql})", list(filtered.values()))
            return "inserted"

        col_sql = ", ".join(filtered.keys())
        q_sql = ", ".join(["?"] * len(filtered))
        conn.execute(f"INSERT INTO {table} ({col_sql}) VALUES ({q_sql})", list(filtered.values()))
        return "inserted(no_key)"

    # ---------------------------------------------------------------------
    # Asset helpers
    # ---------------------------------------------------------------------
    def find_user_assets(self, user: Dict[str, Any]) -> List[Tuple[int, str, str]]:
        emp_id = self.get_user_key(user)
        if not emp_id:
            return []

        found: List[Tuple[int, str, str]] = []
        seen = set()

        for slot_idx, slot in enumerate(self.asset_slots):
            base_dir = Path(slot["dir"])
            try:
                base_dir.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            for pat in slot["patterns"]:
                p = base_dir / pat.format(emp_id=emp_id)
                if p.is_file():
                    rel = self._sanitize_relpath(p.name)
                    key = (slot_idx, str(p.resolve()))
                    if key not in seen:
                        seen.add(key)
                        found.append((slot_idx, str(p.resolve()), rel))

            # per-user folder support if present
            subdir = base_dir / emp_id
            if subdir.is_dir():
                for root, _, files in os.walk(subdir):
                    for fn in files:
                        fp = Path(root) / fn
                        rel = os.path.relpath(str(fp), str(base_dir))
                        rel = self._sanitize_relpath(rel)
                        key = (slot_idx, str(fp.resolve()))
                        if key not in seen:
                            seen.add(key)
                            found.append((slot_idx, str(fp.resolve()), rel))

        return found

    def write_asset_to_slot(self, slot_idx: int, rel: str, blob: bytes):
        if slot_idx < 0 or slot_idx >= len(self.asset_slots):
            slot_idx = 0

        base = Path(self.asset_slots[slot_idx]["dir"])
        base.mkdir(parents=True, exist_ok=True)

        rel = self._sanitize_relpath(rel)
        full = base / rel
        full.parent.mkdir(parents=True, exist_ok=True)

        with open(full, "wb") as f:
            f.write(blob)

    def delete_user_assets(self, user: Dict[str, Any]) -> Dict[str, Any]:
        emp_id = self.get_user_key(user)
        if not emp_id:
            return {"deleted": 0, "missing": 0, "errors": ["No user key"]}

        deleted = 0
        missing = 0
        errors: List[str] = []

        for slot in self.asset_slots:
            base = Path(slot["dir"])

            for pat in slot["patterns"]:
                p = base / pat.format(emp_id=emp_id)
                try:
                    if p.exists():
                        p.unlink()
                        deleted += 1
                    else:
                        missing += 1
                except Exception as e:
                    errors.append(f"{p}: {e}")

            subdir = base / emp_id
            try:
                if subdir.is_dir():
                    for root, dirs, files in os.walk(subdir, topdown=False):
                        for fn in files:
                            try:
                                (Path(root) / fn).unlink()
                                deleted += 1
                            except Exception as e:
                                errors.append(f"{Path(root) / fn}: {e}")
                        for d in dirs:
                            try:
                                (Path(root) / d).rmdir()
                            except Exception:
                                pass
                    try:
                        subdir.rmdir()
                    except Exception:
                        pass
            except Exception as e:
                errors.append(f"{subdir}: {e}")

        return {"deleted": deleted, "missing": missing, "errors": errors[:50]}

    # ---------------------------------------------------------------------
    # Runtime reload after import
    # ---------------------------------------------------------------------
    def trigger_post_import_reload(self):
        if not self.post_import_reload:
            return

        def _run():
            try:
                self.post_import_reload()
            except Exception as e:
                print(f"[DEVICECONSOLE] post-import reload failed: {e}")

        threading.Thread(
            target=_run,
            daemon=True,
            name="DeviceConsolePostImportReload",
        ).start()

    # ---------------------------------------------------------------------
    # Fingerprint sensor injection callback (deferred registration)
    # ---------------------------------------------------------------------
    def set_fingerprint_inject_callback(self, cb: Callable):
        """
        Register the callback that injects a fingerprint template into the
        physical sensor hardware.  Called from app.py AFTER
        receive_fingerprint_template() is defined.

        Expected signature:
            cb(emp_id: str, fp_bytes: bytes, username: str)
                -> (success: bool, template_id: int|None, message: str)
        """
        self._fingerprint_inject_cb = cb
        print("[DEVICECONSOLE] Fingerprint sensor inject callback registered")

    def _inject_fingerprint_into_sensor(self, emp_id: str, template_b64: str,
                                         name: str = "") -> Dict[str, Any]:
        """
        Decode template and call the app-level receive_fingerprint_template()
        which handles slot assignment, DB tables, AND sensor injection.

        IMPORTANT: receive_fingerprint_template() checks fingerprint_map first
        and SKIPS if the emp_id already has an entry (assumes already injected).
        When copying via Device Console, the user/files may have been imported
        first (via /api/users/import) which creates a fingerprint_map entry
        WITHOUT injecting into the sensor. So we MUST clear the fingerprint_map
        entry before calling the callback to force a fresh sensor injection.
        """
        if not self._fingerprint_inject_cb:
            return {
                "ok": False,
                "error": "Fingerprint sensor inject callback not registered "
                         "(device may not have a fingerprint sensor)",
            }

        try:
            fp_bytes = base64.b64decode(template_b64)
        except Exception as e:
            return {"ok": False, "error": f"Invalid base64 template: {e}"}

        # Clear existing fingerprint_map entry so receive_fingerprint_template()
        # does NOT skip this employee. The callback will re-create the entry
        # with a proper sensor slot assignment.
        try:
            conn = self.db_connect()
            conn.execute("DELETE FROM fingerprint_map WHERE emp_id=?", (emp_id,))
            conn.execute("DELETE FROM user_finger_map WHERE emp_id=?", (emp_id,))
            conn.commit()
            conn.close()
            print(f"[DEVICECONSOLE] Cleared fingerprint_map for {emp_id} before sensor inject")
        except Exception as e:
            # Not fatal — the table may not exist yet on a fresh device
            print(f"[DEVICECONSOLE] fingerprint_map clear warning: {e}")

        # Call synchronously so the caller gets the real result
        try:
            success, tid, msg = self._fingerprint_inject_cb(emp_id, fp_bytes, name)
            return {"ok": success, "template_id": tid, "message": msg}
        except Exception as e:
            return {"ok": False, "error": f"Sensor inject exception: {e}"}

    # ---------------------------------------------------------------------
    # Biometric helpers (fingerprint, face, rfid)
    # ---------------------------------------------------------------------
    def _fp_encodings_dir(self) -> Path:
        return self.root_dir / "fingerprint_encodings"

    def _fp_bins_dir(self) -> Path:
        return self.root_dir / "fingerprint_bins"

    def _face_encodings_dir(self) -> Path:
        return self.root_dir / "face_encodings"

    def _users_img_dir(self) -> Path:
        return self.root_dir / "users_img"

    def _get_fingerprint_data(self, emp_id: str) -> Dict[str, Any]:
        """Read fingerprint template + metadata for an employee."""
        fp_dir = self._fp_encodings_dir()
        dat_path = fp_dir / f"{emp_id}.dat"
        json_path = fp_dir / f"{emp_id}.json"

        result = {"ok": False, "emp_id": emp_id}

        if not dat_path.is_file():
            result["error"] = f"No fingerprint template file for emp_id {emp_id}"
            return result

        try:
            with open(dat_path, "rb") as f:
                template_bytes = f.read()
        except Exception as e:
            result["error"] = f"Failed to read fingerprint template: {e}"
            return result

        template_b64 = base64.b64encode(template_bytes).decode("ascii")

        # Read metadata if available
        metadata = {}
        if json_path.is_file():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except Exception:
                pass

        # Also try fingerprint_map DB table for name and template_id
        name = metadata.get("name", "")
        template_id = metadata.get("template_id")
        try:
            conn = self.db_connect()
            row = conn.execute(
                "SELECT template_id, name FROM fingerprint_map WHERE emp_id=? LIMIT 1",
                (emp_id,),
            ).fetchone()
            conn.close()
            if row:
                template_id = row["template_id"] if template_id is None else template_id
                name = row["name"] or name
        except Exception:
            pass

        # Get user name from users table if still empty
        if not name:
            try:
                conn = self.db_connect()
                table = self.detect_users_table(conn)
                row = conn.execute(
                    f"SELECT * FROM {table} WHERE emp_id=? LIMIT 1", (emp_id,)
                ).fetchone()
                conn.close()
                if row:
                    name = str(dict(row).get("name") or "")
            except Exception:
                pass

        result["ok"] = True
        result["template_b64"] = template_b64
        result["template_size"] = len(template_bytes)
        result["template_id"] = template_id
        result["name"] = name
        return result

    def _save_fingerprint_data(self, emp_id: str, template_b64: str,
                                template_id: Optional[int] = None,
                                name: str = "") -> Dict[str, Any]:
        """Save fingerprint template + metadata + DB record for an employee."""
        try:
            template_bytes = base64.b64decode(template_b64)
        except Exception as e:
            return {"ok": False, "error": f"Invalid base64 template data: {e}"}

        fp_dir = self._fp_encodings_dir()
        fp_dir.mkdir(parents=True, exist_ok=True)

        # Save .dat
        dat_path = fp_dir / f"{emp_id}.dat"
        try:
            with open(dat_path, "wb") as f:
                f.write(template_bytes)
        except Exception as e:
            return {"ok": False, "error": f"Failed to write template file: {e}"}

        # Save .json metadata
        json_path = fp_dir / f"{emp_id}.json"
        meta = {
            "emp_id": emp_id,
            "template_id": template_id,
            "name": name,
            "file": f"{emp_id}.dat",
        }
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
        except Exception:
            pass

        # Also save to fingerprint_bins for backup
        bins_dir = self._fp_bins_dir()
        try:
            bins_dir.mkdir(parents=True, exist_ok=True)
            bin_path = bins_dir / f"{emp_id}.bin"
            with open(bin_path, "wb") as f:
                f.write(template_bytes)
        except Exception:
            pass

        # Upsert fingerprint_map in DB
        try:
            conn = self.db_connect()
            now_str = time.strftime("%Y-%m-%d %H:%M:%S")

            # Determine template_id if not provided
            if template_id is None:
                # Check if already exists
                row = conn.execute(
                    "SELECT template_id FROM fingerprint_map WHERE emp_id=? LIMIT 1",
                    (emp_id,),
                ).fetchone()
                if row:
                    template_id = row["template_id"]
                else:
                    # Find next available template_id
                    max_row = conn.execute(
                        "SELECT MAX(template_id) as m FROM fingerprint_map"
                    ).fetchone()
                    template_id = (max_row["m"] or 0) + 1 if max_row else 1

            # Get name from users table if not provided
            if not name:
                try:
                    table = self.detect_users_table(conn)
                    urow = conn.execute(
                        f"SELECT * FROM {table} WHERE emp_id=? LIMIT 1", (emp_id,)
                    ).fetchone()
                    if urow:
                        name = str(dict(urow).get("name") or "")
                except Exception:
                    pass

            conn.execute(
                """INSERT INTO fingerprint_map (emp_id, template_id, name, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(emp_id) DO UPDATE SET
                       template_id=excluded.template_id,
                       name=excluded.name,
                       updated_at=excluded.updated_at""",
                (emp_id, template_id, name, now_str, now_str),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[DEVICECONSOLE] fingerprint_map upsert warning: {e}")

        return {
            "ok": True,
            "emp_id": emp_id,
            "template_size": len(template_bytes),
            "template_id": template_id,
        }

    def _delete_fingerprint_data(self, emp_id: str) -> Dict[str, Any]:
        """Delete fingerprint template files and DB record."""
        deleted_files = 0
        errors = []

        # Delete from fingerprint_encodings
        fp_dir = self._fp_encodings_dir()
        for ext in (".dat", ".json"):
            p = fp_dir / f"{emp_id}{ext}"
            try:
                if p.is_file():
                    p.unlink()
                    deleted_files += 1
            except Exception as e:
                errors.append(str(e))

        # Delete from fingerprint_bins
        bins_dir = self._fp_bins_dir()
        bin_path = bins_dir / f"{emp_id}.bin"
        try:
            if bin_path.is_file():
                bin_path.unlink()
                deleted_files += 1
        except Exception as e:
            errors.append(str(e))

        # Delete from DB
        db_deleted = False
        try:
            conn = self.db_connect()
            conn.execute("DELETE FROM fingerprint_map WHERE emp_id=?", (emp_id,))
            conn.commit()
            conn.close()
            db_deleted = True
        except Exception as e:
            errors.append(f"DB delete: {e}")

        return {
            "ok": True,
            "emp_id": emp_id,
            "files_deleted": deleted_files,
            "db_deleted": db_deleted,
            "errors": errors[:10],
        }

    def _get_face_data(self, emp_id: str) -> Dict[str, Any]:
        """Read face encoding + image for an employee."""
        result = {"ok": False, "emp_id": emp_id}

        enc_dir = self._face_encodings_dir()
        img_dir = self._users_img_dir()

        # Read face encoding
        enc_path = enc_dir / f"{emp_id}.dat"
        encoding_b64 = None
        if enc_path.is_file():
            try:
                with open(enc_path, "rb") as f:
                    encoding_b64 = base64.b64encode(f.read()).decode("ascii")
            except Exception:
                pass

        # Read face image (try jpg, jpeg, png)
        image_b64 = None
        for ext in (".jpg", ".jpeg", ".png"):
            img_path = img_dir / f"{emp_id}{ext}"
            if img_path.is_file():
                try:
                    with open(img_path, "rb") as f:
                        image_b64 = base64.b64encode(f.read()).decode("ascii")
                    break
                except Exception:
                    pass

        if not encoding_b64 and not image_b64:
            result["error"] = f"No face data found for emp_id {emp_id}"
            return result

        # Get name
        name = ""
        try:
            conn = self.db_connect()
            table = self.detect_users_table(conn)
            row = conn.execute(
                f"SELECT * FROM {table} WHERE emp_id=? LIMIT 1", (emp_id,)
            ).fetchone()
            conn.close()
            if row:
                name = str(dict(row).get("name") or "")
        except Exception:
            pass

        result["ok"] = True
        result["encoding_b64"] = encoding_b64
        result["image_b64"] = image_b64
        result["name"] = name
        return result

    def _save_face_data(self, emp_id: str, encoding_b64: Optional[str] = None,
                         image_b64: Optional[str] = None) -> Dict[str, Any]:
        """Save face encoding and/or image for an employee."""
        saved = []

        if encoding_b64:
            enc_dir = self._face_encodings_dir()
            enc_dir.mkdir(parents=True, exist_ok=True)
            try:
                enc_bytes = base64.b64decode(encoding_b64)
                enc_path = enc_dir / f"{emp_id}.dat"
                with open(enc_path, "wb") as f:
                    f.write(enc_bytes)
                saved.append("encoding")

                # Update users table encoding_path
                try:
                    conn = self.db_connect()
                    table = self.detect_users_table(conn)
                    cols = self.get_table_columns(conn, table)
                    if "encoding_path" in cols:
                        conn.execute(
                            f"UPDATE {table} SET encoding_path=? WHERE emp_id=?",
                            (f"face_encodings/{emp_id}.dat", emp_id),
                        )
                        conn.commit()
                    conn.close()
                except Exception:
                    pass
            except Exception as e:
                return {"ok": False, "error": f"Failed to save face encoding: {e}"}

        if image_b64:
            img_dir = self._users_img_dir()
            img_dir.mkdir(parents=True, exist_ok=True)
            try:
                img_bytes = base64.b64decode(image_b64)
                img_path = img_dir / f"{emp_id}.jpg"
                with open(img_path, "wb") as f:
                    f.write(img_bytes)
                saved.append("image")

                # Update users table image_path
                try:
                    conn = self.db_connect()
                    table = self.detect_users_table(conn)
                    cols = self.get_table_columns(conn, table)
                    if "image_path" in cols:
                        conn.execute(
                            f"UPDATE {table} SET image_path=? WHERE emp_id=?",
                            (f"users_img/{emp_id}.jpg", emp_id),
                        )
                        conn.commit()
                    conn.close()
                except Exception:
                    pass
            except Exception as e:
                return {"ok": False, "error": f"Failed to save face image: {e}"}

        if not saved:
            return {"ok": False, "error": "No face data provided (encoding_b64 or image_b64)"}

        return {"ok": True, "emp_id": emp_id, "saved": saved}

    def _delete_face_data(self, emp_id: str) -> Dict[str, Any]:
        """Delete face encoding and image for an employee."""
        deleted = 0

        # Delete encoding
        enc_path = self._face_encodings_dir() / f"{emp_id}.dat"
        try:
            if enc_path.is_file():
                enc_path.unlink()
                deleted += 1
        except Exception:
            pass

        # Delete image (try all extensions)
        img_dir = self._users_img_dir()
        for ext in (".jpg", ".jpeg", ".png"):
            p = img_dir / f"{emp_id}{ext}"
            try:
                if p.is_file():
                    p.unlink()
                    deleted += 1
            except Exception:
                pass

        # Clear paths in DB
        try:
            conn = self.db_connect()
            table = self.detect_users_table(conn)
            cols = self.get_table_columns(conn, table)
            updates = []
            if "encoding_path" in cols:
                updates.append("encoding_path=NULL")
            if "image_path" in cols:
                updates.append("image_path=NULL")
            if updates:
                conn.execute(
                    f"UPDATE {table} SET {', '.join(updates)} WHERE emp_id=?",
                    (emp_id,),
                )
                conn.commit()
            conn.close()
        except Exception:
            pass

        return {"ok": True, "emp_id": emp_id, "files_deleted": deleted}

    def _get_rfid_data(self, emp_id: str) -> Dict[str, Any]:
        """Get RFID card data for an employee."""
        try:
            conn = self.db_connect()
            row = conn.execute(
                "SELECT rfid_card, name FROM rfid_card_map WHERE emp_id=? LIMIT 1",
                (emp_id,),
            ).fetchone()
            conn.close()
            if row:
                return {"ok": True, "emp_id": emp_id, "rfid_card": row["rfid_card"], "name": row["name"]}
            return {"ok": False, "error": f"No RFID card found for emp_id {emp_id}"}
        except Exception as e:
            return {"ok": False, "error": f"Failed to read RFID data: {e}"}

    def _save_rfid_data(self, emp_id: str, rfid_card: str, name: str = "") -> Dict[str, Any]:
        """Save RFID card data for an employee."""
        if not rfid_card:
            return {"ok": False, "error": "rfid_card is required"}

        # Get name from users table if not provided
        if not name:
            try:
                conn = self.db_connect()
                table = self.detect_users_table(conn)
                row = conn.execute(
                    f"SELECT * FROM {table} WHERE emp_id=? LIMIT 1", (emp_id,)
                ).fetchone()
                conn.close()
                if row:
                    name = str(dict(row).get("name") or "")
            except Exception:
                pass

        try:
            conn = self.db_connect()
            now_str = time.strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                """INSERT INTO rfid_card_map (emp_id, rfid_card, name, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(emp_id) DO UPDATE SET
                       rfid_card=excluded.rfid_card,
                       name=excluded.name,
                       updated_at=excluded.updated_at""",
                (emp_id, rfid_card, name, now_str, now_str),
            )
            conn.commit()
            conn.close()
            return {"ok": True, "emp_id": emp_id, "rfid_card": rfid_card}
        except Exception as e:
            return {"ok": False, "error": f"Failed to save RFID data: {e}"}

    def _delete_rfid_data(self, emp_id: str) -> Dict[str, Any]:
        """Delete RFID card data for an employee."""
        try:
            conn = self.db_connect()
            conn.execute("DELETE FROM rfid_card_map WHERE emp_id=?", (emp_id,))
            conn.commit()
            conn.close()
            return {"ok": True, "emp_id": emp_id}
        except Exception as e:
            return {"ok": False, "error": f"Failed to delete RFID data: {e}"}

    def _list_all_fingerprints(self) -> List[Dict[str, Any]]:
        """List all registered fingerprints with employee details."""
        results = []

        # Get fingerprint_map records from DB
        fp_map = {}
        try:
            conn = self.db_connect()
            rows = conn.execute("SELECT * FROM fingerprint_map").fetchall()
            for r in rows:
                d = dict(r)
                fp_map[str(d.get("emp_id", "")).strip()] = d
            conn.close()
        except Exception:
            pass

        # Get user names from users table
        user_names = {}
        try:
            conn = self.db_connect()
            table = self.detect_users_table(conn)
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            for r in rows:
                d = dict(r)
                eid = str(d.get("emp_id") or d.get("employee_id") or d.get("id") or "").strip()
                if eid:
                    user_names[eid] = str(d.get("name") or "")
            conn.close()
        except Exception:
            pass

        # Combine: iterate fingerprint_map keys and also scan files
        all_emp_ids = set(fp_map.keys())

        # Also scan fingerprint_encodings dir for .dat files not in DB
        fp_dir = self._fp_encodings_dir()
        if fp_dir.is_dir():
            for dat_file in fp_dir.glob("*.dat"):
                all_emp_ids.add(dat_file.stem)

        for eid in sorted(all_emp_ids):
            entry = {
                "emp_id": eid,
                "name": "",
                "template_id": None,
                "has_template_file": False,
                "template_size": 0,
                "created_at": None,
                "updated_at": None,
            }

            # From DB
            if eid in fp_map:
                rec = fp_map[eid]
                entry["template_id"] = rec.get("template_id")
                entry["name"] = rec.get("name") or ""
                entry["created_at"] = rec.get("created_at")
                entry["updated_at"] = rec.get("updated_at")

            # From user names
            if not entry["name"] and eid in user_names:
                entry["name"] = user_names[eid]

            # Check template file
            dat_path = fp_dir / f"{eid}.dat"
            if dat_path.is_file():
                entry["has_template_file"] = True
                try:
                    entry["template_size"] = dat_path.stat().st_size
                except Exception:
                    pass

            results.append(entry)

        return results

    # ---------------------------------------------------------------------
    # Routes
    # ---------------------------------------------------------------------
    def register_routes(self):
        svc = self
        app = self.app

        # ------------------------------
        # Device Console UI routes
        # ------------------------------
        @app.route("/device-console")
        def device_console_page():
            return render_template("device_console.html")

        @app.route("/api/device-console/config", methods=["GET"])
        def api_device_console_config_get():
            return jsonify({"success": True, "config": svc.get_config()})

        @app.route("/api/device-console/config", methods=["POST"])
        def api_device_console_config_save():
            data = request.get_json(silent=True) or {}
            try:
                cfg = svc.save_config(
                    enabled=data.get("enabled", False),
                    server_ip=data.get("server_ip", ""),
                    server_port=data.get("server_port", "9000"),
                    device_group=data.get("device_group", ""),
                    device_name=data.get("device_name", ""),
                )
                return jsonify({
                    "success": True,
                    "message": "Device Console configuration saved successfully",
                    "config": cfg,
                })
            except ValueError as e:
                return jsonify({"success": False, "message": str(e)}), 400
            except Exception as e:
                return jsonify({"success": False, "message": f"Failed to save configuration: {e}"}), 500

        @app.route("/api/device-console/test", methods=["POST"])
        def api_device_console_test():
            data = request.get_json(silent=True) or {}
            ok, msg = svc.test_console_server(
                data.get("server_ip", ""),
                data.get("server_port", "9000"),
            )
            return jsonify({"success": ok, "message": msg})

        @app.route("/api/device-console/announce", methods=["POST"])
        def api_device_console_announce():
            cfg = svc.get_config()
            if not cfg["enabled"]:
                return jsonify({"success": False, "message": "Enable Device Console first"}), 400
            if not cfg["configured"]:
                return jsonify({"success": False, "message": "Complete the configuration first"}), 400

            svc.set_setting(svc.CFG_AUTORECONNECT, "1")
            svc.ensure_worker_started()
            ok, msg = svc.announce_once(event="announce_now")
            return jsonify({"success": ok, "message": msg, "config": svc.get_config()})

        @app.route("/api/device-console/stop", methods=["POST"])
        def api_device_console_stop():
            svc.stop_worker()
            return jsonify({"success": True, "message": "Reconnect thread stopped", "config": svc.get_config()})

        @app.route("/api/device-console/status", methods=["GET"])
        def api_device_console_status():
            return jsonify({"success": True, "status": svc.get_config()})

        # ------------------------------
        # Replacement for old api.py
        # ------------------------------
        @app.route("/api/health", methods=["GET"])
        def api_health():
            cfg = svc.get_config()
            bio = svc._biometric_counts()
            return jsonify({
                "ok": True,
                "device_id": cfg["device_id"],
                "mac": cfg["device_mac"],
                "device_name": cfg["device_name"],
                "section": cfg["device_group"],
                "ip": cfg["self_ip"],
                "api_port": cfg["device_api_port"],
                "db_path": cfg["db_path"],
                "db_exists": os.path.exists(cfg["db_path"]),
                "asset_dirs": cfg["asset_dirs"],
                "assets_exist": [os.path.isdir(x) for x in cfg["asset_dirs"]],
                "face_count": bio["face_count"],
                "fingerprint_count": bio["fingerprint_count"],
                "rfid_count": bio["rfid_count"],
                "ts": svc._now_ts(),
            })

        @app.route("/api/users", methods=["GET"])
        def api_list_users():
            svc.require_token_and_console_ip()

            try:
                conn = svc.db_connect()
                table, users = svc.get_users(conn)
            except Exception as e:
                return jsonify({"detail": f"DB open failed: {e}"}), 500

            # Build lookup maps for biometric status
            fp_set = set()
            rfid_set = set()
            try:
                for r in conn.execute("SELECT emp_id FROM fingerprint_map").fetchall():
                    fp_set.add(str(r["emp_id"]).strip())
            except Exception:
                pass
            try:
                for r in conn.execute("SELECT emp_id FROM rfid_card_map").fetchall():
                    rfid_set.add(str(r["emp_id"]).strip())
            except Exception:
                pass
            conn.close()

            face_enc_dir = svc._face_encodings_dir()

            out = []
            for u in users:
                uu = dict(u)
                eid = svc.get_user_key(uu)
                uu["_key"] = eid

                # Add biometric flags
                uu["has_fingerprint"] = eid in fp_set
                uu["has_rfid"] = eid in rfid_set
                uu["has_face"] = (face_enc_dir / f"{eid}.dat").is_file()

                out.append(uu)

            return jsonify({"table": table, "count": len(out), "users": out})

        @app.route("/api/users/export", methods=["POST"])
        def api_export_users():
            svc.require_token_and_console_ip()

            payload = request.get_json(silent=True) or {}
            user_keys = payload.get("user_keys") or []
            if not isinstance(user_keys, list) or not user_keys:
                return jsonify({"detail": "user_keys list required"}), 400

            try:
                conn = svc.db_connect()
                table, users = svc.get_users(conn)
                conn.close()
            except Exception as e:
                return jsonify({"detail": f"DB open failed: {e}"}), 500

            wanted = {str(x).strip() for x in user_keys}
            selected = [u for u in users if svc.get_user_key(u) in wanted]
            if not selected:
                return jsonify({"detail": "No matching users found"}), 404

            mem = io.BytesIO()
            with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr(
                    "manifest.json",
                    svc._safe_json_dumps({
                        "export_from": {
                            "device_id": svc._stable_uuid(),
                            "mac": svc._primary_mac(),
                            "device_name": svc.get_config()["device_name"],
                            "section": svc.get_config()["device_group"],
                            "ip": svc.get_self_ip(),
                            "api_port": svc.app_port,
                        },
                        "export_ts": svc._now_ts(),
                        "table": table,
                        "count": len(selected),
                        "asset_dirs_slots": [
                            {"slot": i, "path": str(svc.asset_slots[i]["dir"])}
                            for i in range(len(svc.asset_slots))
                        ],
                    }),
                )

                for u in selected:
                    uk = svc.get_user_key(u)
                    z.writestr(
                        f"users/{uk}/user.json",
                        svc._safe_json_dumps(svc._user_to_json_safe(u)),
                    )

                    assets = svc.find_user_assets(u)
                    for slot_idx, full_path, rel in assets:
                        arc = f"users/{uk}/assets/{slot_idx}/{svc._sanitize_relpath(rel)}"
                        try:
                            with open(full_path, "rb") as f:
                                z.writestr(arc, f.read())
                        except Exception:
                            pass

            mem.seek(0)
            filename = f"ethos_export_{svc._stable_uuid()}_{svc._now_ts()}.zip"
            return send_file(
                mem,
                mimetype="application/zip",
                as_attachment=True,
                download_name=filename,
            )

        @app.route("/api/users/import", methods=["POST"])
        def api_import_users():
            svc.require_token_and_console_ip()

            up = request.files.get("file")
            if not up:
                return jsonify({"detail": "file is required"}), 400

            data = up.read()
            mem = io.BytesIO(data)

            try:
                conn = svc.db_connect()
            except Exception as e:
                return jsonify({"detail": f"DB open failed: {e}"}), 500

            try:
                table = svc.detect_users_table(conn)
                results = []

                with zipfile.ZipFile(mem, "r") as z:
                    user_keys = set()
                    for n in z.namelist():
                        if n.startswith("users/") and n.endswith("/user.json"):
                            parts = n.split("/")
                            if len(parts) >= 3:
                                user_keys.add(parts[1])

                    for uk in sorted(user_keys):
                        ujson = z.read(f"users/{uk}/user.json").decode("utf-8", errors="ignore")
                        user = json.loads(ujson)
                        user = svc._json_safe_to_user(user)

                        status = svc.upsert_user(conn, table, user)

                        written = 0
                        for n in z.namelist():
                            if not n.startswith(f"users/{uk}/assets/"):
                                continue
                            if n.endswith("/"):
                                continue

                            parts = n.split("/")
                            if len(parts) < 5:
                                continue

                            try:
                                slot_idx = int(parts[3])
                            except Exception:
                                slot_idx = 0

                            rel = "/".join(parts[4:])
                            rel = svc._sanitize_relpath(rel)
                            blob = z.read(n)

                            svc.write_asset_to_slot(slot_idx, rel, blob)
                            written += 1

                        results.append({"user_key": uk, "db": status, "assets_written": written})

                conn.commit()
                conn.close()

                # This fixes copied users verifying only after app restart
                svc.trigger_post_import_reload()
                svc.announce_once(event="users_imported")

                return jsonify({"ok": True, "imported": len(results), "results": results})

            except zipfile.BadZipFile:
                conn.rollback()
                conn.close()
                return jsonify({"detail": "Invalid ZIP file"}), 400
            except Exception as e:
                conn.rollback()
                conn.close()
                return jsonify({"detail": f"Import failed: {e}"}), 500

        @app.route("/api/users/delete", methods=["POST"])
        def api_delete_users():
            svc.require_token_and_console_ip()

            payload = request.get_json(silent=True) or {}
            user_keys = payload.get("user_keys") or []
            delete_assets_flag = bool(payload.get("delete_assets", True))

            if not isinstance(user_keys, list) or not user_keys:
                return jsonify({"detail": "user_keys list required"}), 400

            try:
                conn = svc.db_connect()
                table = svc.detect_users_table(conn)
                cols = svc.get_table_columns(conn, table)
                key_col = svc.find_key_column(cols)
                if not key_col:
                    raise RuntimeError("Could not detect user key column")

                rows = conn.execute(f"SELECT * FROM {table}").fetchall()
                row_map = {}
                for r in rows:
                    d = dict(r)
                    row_map[svc.get_user_key(d)] = d

                results = []
                deleted_db = 0
                total_assets_deleted = 0
                total_assets_missing = 0
                asset_errors: List[str] = []

                for k in [str(x).strip() for x in user_keys]:
                    row = row_map.get(k)
                    if not row:
                        results.append({"user_key": k, "status": "not_found"})
                        continue

                    conn.execute(f"DELETE FROM {table} WHERE {key_col}=?", (row.get(key_col),))
                    deleted_db += 1

                    asset_info = {"deleted": 0, "missing": 0, "errors": []}
                    if delete_assets_flag:
                        asset_info = svc.delete_user_assets(row)
                        total_assets_deleted += int(asset_info.get("deleted") or 0)
                        total_assets_missing += int(asset_info.get("missing") or 0)
                        asset_errors.extend(asset_info.get("errors") or [])

                    results.append({"user_key": k, "status": "deleted", "assets": asset_info})

                conn.commit()
                conn.close()

                svc.announce_once(event="users_deleted")

                return jsonify({
                    "ok": True,
                    "table": table,
                    "requested": len(user_keys),
                    "deleted_db": deleted_db,
                    "assets_deleted": total_assets_deleted,
                    "assets_missing": total_assets_missing,
                    "asset_errors": asset_errors[:50],
                    "results": results,
                })

            except Exception as e:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass
                return jsonify({"detail": f"Delete failed: {e}"}), 500

        @app.route("/api/notify", methods=["POST"])
        def api_notify():
            svc.require_token_and_console_ip()
            payload = request.get_json(silent=True) or {}
            event_name = svc._clean(payload.get("event") or "custom")
            ok, msg = svc.announce_once(event=event_name)
            return jsonify({"ok": ok, "message": msg})

        # ==============================================================
        # BIOMETRIC ENDPOINTS - Fingerprint fetch/inject/delete/list
        # Called by the Device Console Server for template transfer
        # ==============================================================

        @app.route("/api/device-console/fingerprint/fetch", methods=["POST"])
        def api_dc_fingerprint_fetch():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400
            result = svc._get_fingerprint_data(emp_id)
            return jsonify(result), 200 if result.get("ok") else 404

        @app.route("/api/device-console/fingerprint/inject", methods=["POST"])
        def api_dc_fingerprint_inject():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            template_b64 = data.get("template_b64", "")
            name = str(data.get("name", "")).strip()

            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400
            if not template_b64:
                return jsonify({"ok": False, "error": "template_b64 is required"}), 400

            # Use the sensor inject callback (receive_fingerprint_template)
            # which handles: slot assignment, files, DB, AND sensor injection
            if svc._fingerprint_inject_cb:
                result = svc._inject_fingerprint_into_sensor(emp_id, template_b64, name)
            else:
                # Fallback: save files/DB only (no sensor — e.g. sensor not connected)
                template_id = data.get("template_id")
                if template_id is not None:
                    try:
                        template_id = int(template_id)
                    except (ValueError, TypeError):
                        template_id = None
                result = svc._save_fingerprint_data(emp_id, template_b64, template_id, name)
                result["warning"] = "Saved to files/DB only — sensor inject callback not registered"

            if result.get("ok"):
                svc.trigger_post_import_reload()

            return jsonify(result), 200 if result.get("ok") else 500

        @app.route("/api/device-console/fingerprint/delete", methods=["POST"])
        def api_dc_fingerprint_delete():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400
            result = svc._delete_fingerprint_data(emp_id)
            return jsonify(result)

        @app.route("/api/device-console/fingerprints", methods=["GET"])
        def api_dc_fingerprints_list():
            """List all registered fingerprints with employee ID, name, and details."""
            svc.require_token_and_console_ip()
            fingerprints = svc._list_all_fingerprints()
            return jsonify({
                "ok": True,
                "count": len(fingerprints),
                "fingerprints": fingerprints,
            })

        @app.route("/api/device-console/fingerprints/download", methods=["GET"])
        def api_dc_fingerprints_download():
            """Download all fingerprint templates as a ZIP bundle with employee details."""
            svc.require_token_and_console_ip()

            fingerprints = svc._list_all_fingerprints()
            fp_dir = svc._fp_encodings_dir()
            cfg = svc.get_config()

            mem = io.BytesIO()
            with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
                # Write manifest
                z.writestr("manifest.json", svc._safe_json_dumps({
                    "export_type": "fingerprint_templates",
                    "device_id": cfg["device_id"],
                    "device_name": cfg["device_name"],
                    "section": cfg["device_group"],
                    "ip": cfg["self_ip"],
                    "export_ts": svc._now_ts(),
                    "count": len(fingerprints),
                }))

                # Write summary CSV for easy viewing
                csv_lines = ["emp_id,name,template_id,template_size,created_at,updated_at"]
                for fp in fingerprints:
                    csv_lines.append(
                        f'{fp["emp_id"]},{fp["name"]},{fp.get("template_id") or ""},'
                        f'{fp["template_size"]},{fp.get("created_at") or ""},{fp.get("updated_at") or ""}'
                    )
                z.writestr("fingerprints_summary.csv", "\n".join(csv_lines))

                # Write each template
                for fp in fingerprints:
                    eid = fp["emp_id"]
                    dat_path = fp_dir / f"{eid}.dat"
                    json_path = fp_dir / f"{eid}.json"

                    if dat_path.is_file():
                        try:
                            z.write(str(dat_path), f"templates/{eid}.dat")
                        except Exception:
                            pass
                    if json_path.is_file():
                        try:
                            z.write(str(json_path), f"templates/{eid}.json")
                        except Exception:
                            pass

            mem.seek(0)
            filename = f"fingerprints_{cfg['device_id']}_{svc._now_ts()}.zip"
            return send_file(
                mem,
                mimetype="application/zip",
                as_attachment=True,
                download_name=filename,
            )

        # ==============================================================
        # BIOMETRIC ENDPOINTS - Face fetch/inject/delete
        # ==============================================================

        @app.route("/api/device-console/face/fetch", methods=["POST"])
        def api_dc_face_fetch():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400
            result = svc._get_face_data(emp_id)
            return jsonify(result), 200 if result.get("ok") else 404

        @app.route("/api/device-console/face/inject", methods=["POST"])
        def api_dc_face_inject():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            encoding_b64 = data.get("encoding_b64")
            image_b64 = data.get("image_b64")

            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400

            result = svc._save_face_data(emp_id, encoding_b64, image_b64)

            if result.get("ok"):
                svc.trigger_post_import_reload()

            return jsonify(result), 200 if result.get("ok") else 500

        @app.route("/api/device-console/face/delete", methods=["POST"])
        def api_dc_face_delete():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400
            result = svc._delete_face_data(emp_id)

            if result.get("ok"):
                svc.trigger_post_import_reload()

            return jsonify(result)

        # ==============================================================
        # BIOMETRIC ENDPOINTS - RFID inject/delete
        # ==============================================================

        @app.route("/api/device-console/rfid/inject", methods=["POST"])
        def api_dc_rfid_inject():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            rfid_card = str(data.get("rfid_card", "")).strip()
            name = str(data.get("name", "")).strip()

            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400
            if not rfid_card:
                return jsonify({"ok": False, "error": "rfid_card is required"}), 400

            result = svc._save_rfid_data(emp_id, rfid_card, name)
            return jsonify(result), 200 if result.get("ok") else 500

        @app.route("/api/device-console/rfid/delete", methods=["POST"])
        def api_dc_rfid_delete():
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400
            result = svc._delete_rfid_data(emp_id)
            return jsonify(result)

        # ==============================================================
        # BIOMETRIC ENDPOINTS - Employee bulk inject/delete
        # ==============================================================

        @app.route("/api/device-console/employee/inject", methods=["POST"])
        def api_dc_employee_inject():
            """Inject all available biometric data for an employee at once."""
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400

            results = {}

            # Fingerprint — use sensor inject callback if available
            if data.get("template_b64"):
                if svc._fingerprint_inject_cb:
                    results["fingerprint"] = svc._inject_fingerprint_into_sensor(
                        emp_id, data["template_b64"], data.get("name", "")
                    )
                else:
                    tid = data.get("template_id")
                    if tid is not None:
                        try:
                            tid = int(tid)
                        except (ValueError, TypeError):
                            tid = None
                    results["fingerprint"] = svc._save_fingerprint_data(
                        emp_id, data["template_b64"], tid, data.get("name", "")
                    )

            # Face
            if data.get("encoding_b64") or data.get("image_b64"):
                results["face"] = svc._save_face_data(
                    emp_id, data.get("encoding_b64"), data.get("image_b64")
                )

            # RFID
            if data.get("rfid_card"):
                results["rfid"] = svc._save_rfid_data(
                    emp_id, data["rfid_card"], data.get("name", "")
                )

            if not results:
                return jsonify({"ok": False, "error": "No biometric data provided"}), 400

            any_ok = any(r.get("ok") for r in results.values())
            if any_ok:
                svc.trigger_post_import_reload()

            return jsonify({"ok": any_ok, "emp_id": emp_id, "results": results})

        @app.route("/api/device-console/employee/delete", methods=["POST"])
        def api_dc_employee_delete():
            """Delete biometric data for an employee. Optionally specify modalities."""
            svc.require_token_and_console_ip()
            data = request.get_json(silent=True) or {}
            emp_id = str(data.get("emp_id", "")).strip()
            if not emp_id:
                return jsonify({"ok": False, "error": "emp_id is required"}), 400

            modalities = data.get("modalities")
            if not modalities:
                modalities = ["fingerprint", "face", "rfid"]

            results = {}
            if "fingerprint" in modalities:
                results["fingerprint"] = svc._delete_fingerprint_data(emp_id)
            if "face" in modalities:
                results["face"] = svc._delete_face_data(emp_id)
            if "rfid" in modalities:
                results["rfid"] = svc._delete_rfid_data(emp_id)

            any_ok = any(r.get("ok") for r in results.values())
            if any_ok:
                svc.trigger_post_import_reload()

            return jsonify({"ok": any_ok, "emp_id": emp_id, "results": results})


def init_device_console(
    app,
    get_setting,
    set_setting,
    get_self_ip,
    get_device_id,
    db_path,
    app_port,
    post_import_reload=None,
):
    return DeviceConsoleService(
        app=app,
        get_setting=get_setting,
        set_setting=set_setting,
        get_self_ip=get_self_ip,
        get_device_id=get_device_id,
        db_path=db_path,
        app_port=app_port,
        post_import_reload=post_import_reload,
    )
