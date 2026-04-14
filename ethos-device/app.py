import os
import io
import glob
import sqlite3
from sqlite3 import OperationalError
import threading
import time
import subprocess
import hashlib
import cv2
import numpy as np
import base64
import concurrent.futures
import json
import zipfile
import logging
from datetime import datetime, date, timedelta
from threading import Event, Lock
import qrcode
import pandas as pd  # XLSX import/export
import uuid, json
import sqlite3
import time
import psycopg2
import psycopg2.extras

# ── Fingerprint transfer dedicated logger ─────────────────────────────────
# Writes detailed logs to fp_transfer.log for debugging template transfer
fp_log = logging.getLogger("fp_transfer")
fp_log.setLevel(logging.DEBUG)
_fp_log_handler = logging.FileHandler("fp_transfer.log", encoding="utf-8")
_fp_log_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
fp_log.addHandler(_fp_log_handler)
fp_log.info("=== Fingerprint transfer logger initialized ===")

from flask import (
    Flask,
    render_template,
    render_template_string,
    request,
    jsonify,
    Response,
    send_from_directory,
    send_file,
    abort,
    session,
    redirect,
    url_for,
)

from rbac import (
     rbac_bp,
     init_rbac,
     get_rbac,
     require_permission,
     require_page_permission,
     require_role,
     require_login,
     get_session_user,
     can_access_page,
     get_user_allowed_pages,
     ROLE_SUPER_ADMIN,
     ROLE_ADMIN,
     ROLE_USER,
)

from werkzeug.utils import secure_filename

# MSSQL support
try:
    import pymssql
    MSSQL_AVAILABLE = True
except ImportError:
    try:
        import pyodbc
        MSSQL_AVAILABLE = True
    except ImportError:
        MSSQL_AVAILABLE = False
        print("[MSSQL] No MSSQL library available (pymssql or pyodbc). Install with: pip install pymssql")

import inspect
import uuid
import re
import socket
from pathlib import Path
from device_sync_manager import DeviceSyncManager
import device_agent

# --- Optional Pi camera (falls back to OpenCV if not available) ---
try:
    from picamera2 import Picamera2
except Exception:
    Picamera2 = None


from flask import Flask, render_template_string, request, jsonify
from gpiozero import OutputDevice
import os

# ====== CONFIGURE THESE ======
RELAY_GPIO = 26          
LOW_LEVEL_TRIGGER = True # Set True if your relay triggers on LOW; False if HIGH-trigger.

# gpiozero OutputDevice: active_high controls logic inversion in software
relay = OutputDevice(RELAY_GPIO, active_high=(not LOW_LEVEL_TRIGGER), initial_value=False)


# --- Local modules ---
from face_recognizer import FaceRecognizer
from fingerprint import Fingerprint
import rfid

# --- New optimized modules ---
from image_helper import (
    load_image, load_encoding,
    save_image, save_encoding,
    save_from_base64, load_as_base64,
    image_exists, encoding_exists,
    delete_image, delete_encoding,
    IMAGES_DIR as IMAGE_DIR,        # alias to match app.py usage
    ENCODINGS_DIR as ENCODING_DIR,  # alias to match app.py usage
)
from fingerprint_helper import (
    save_fingerprint_template, load_fingerprint_template,
    save_fingerprint_from_base64, load_fingerprint_as_base64,
    fingerprint_template_exists, delete_fingerprint_template,
    get_all_fingerprint_templates, get_template_id_from_metadata,
    load_fingerprint_metadata
)
from face_quality_checker import get_quality_checker
from deviceconsole import init_device_console

def _add_permissions_route(app):
    @app.route("/permissions")
    @require_page_permission("permissions")
    def permissions_page():
        return render_template("permissions.html")

# lightweight replacement for mesh/TCP helper: provide device IP
# (original mesh code was removed per your request)
def get_self_ip():
    """Return a sensible local IP address (fallback to 127.0.0.1)."""
    try:
        # This attempts to find an outward-facing IP without sending packets.
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # connect to a public DNS (no packet is actually sent)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            # fallback to hostname resolution
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


def run_parallel(*targets):
    for fn in targets:
        t = threading.Thread(target=fn, daemon=True)
        t.start()

_sleep_lock = Lock()
_sleep_mode = False
_last_activity_ts = time.monotonic()

# Login debounce - prevent duplicate logs for same user within time window
_login_debounce_lock = Lock()
_last_login_user = {}  # {emp_id: timestamp} - track last successful login time per user
LOGIN_DEBOUNCE_SECONDS = 3.0  # Minimum 3 seconds between logs for same user

def mark_activity():
    global _last_activity_ts, _sleep_mode
    with _sleep_lock:
        _last_activity_ts = time.monotonic()
        _sleep_mode = False

def is_sleep_mode():
    with _sleep_lock:
        return _sleep_mode

# -----------------------------------------------------------------------------
# Flask app setup
# -----------------------------------------------------------------------------
AUDIO_PATH = "/home/admin/ethos-device/static/audio/"
ADMIN_PW_FILE = "admin_pw.txt"
USERS_IMG_DIR = "./users_img"
DB_PATH = os.environ.get("APP_DB_PATH", "users.db")
UDP_PORT = int(os.environ.get("UDP_PORT", "5006"))
BROADCAST_ADDR = os.environ.get("BROADCAST_ADDR", "255.255.255.255")
NETWORK_ENABLED = True
IDLE_TIMEOUT_SECONDS = 60
IDLE_CHECK_INTERVAL = 5

def _idle_watchdog():
    global _sleep_mode
    while True:
        time.sleep(IDLE_CHECK_INTERVAL)
        with _sleep_lock:
            if not _sleep_mode and (time.monotonic() - _last_activity_ts) >= IDLE_TIMEOUT_SECONDS:
                _sleep_mode = True

threading.Thread(target=_idle_watchdog, daemon=True).start()
SLEEP_FACE_CASCADE = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecret")

# ── RBAC ─────────────────────────────────────────────
#from rbac import rbac_bp, init_rbac, get_session_user, require_permission

# Branding (used in templates)
app.config.update(
    BRAND_NAME=os.environ.get("BRAND_NAME", "Canteen Kiosk"),
    BRAND_LOGO=os.environ.get("BRAND_LOGO", "/static/img/logo.png"),  # put your logo file here
    PUBLIC_BASE_URL=os.environ.get("PUBLIC_BASE_URL", ""),            # optional fixed base URL for phone
    APP_PORT=int(os.environ.get("APP_PORT", "5000")),
)

# Handoff / QR Import-Export
HANDOFF_TTL_SECONDS = int(os.environ.get("TOKEN_TTL_SECONDS", str(10 * 60)))  # 10m
HANDOFF_UPLOAD_DIR = os.path.join(os.getcwd(), "handoff_uploads")
os.makedirs(HANDOFF_UPLOAD_DIR, exist_ok=True)

# Initialize Device Sync Manager (will be initialized after get_db_connection is defined)
device_sync_manager = None

@app.context_processor
def inject_brand():
    return {
        "BRAND_NAME": app.config.get("BRAND_NAME"),
        "BRAND_LOGO": app.config.get("BRAND_LOGO"),
    }

# Optional favicon passthrough from /static
@app.route("/favicon.ico")
def favicon():
    fav_path = os.path.join(app.static_folder or "static", "favicon.ico")
    if os.path.exists(fav_path):
        return send_from_directory(app.static_folder, "favicon.ico")
    return ("", 204)




# How many times per day we are allowed to show birthday greeting per user
BIRTHDAY_MAX_GREETS_PER_DAY = 2


def ensure_birthday_table():
    """Small table to track how many times a user was greeted on a given date."""
    try:
        conn = get_db_connection()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS birthday_greets (
                    emp_id TEXT NOT NULL,
                    date   TEXT NOT NULL,
                    count  INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(emp_id, date)
                )
            """)
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print("[init] ensure_birthday_table failed:", e)


def _is_today_birthday(emp_id: str) -> bool:
    """Check if today is this user's birthday based on users.birthdate."""
    if not emp_id:
        return False

    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT birthdate FROM users WHERE emp_id=?",
            (emp_id,)
        ).fetchone()
        if not row:
            return False

        bd = (row["birthdate"] or "").strip()
        if not bd:
            return False

        # Accept formats like YYYY-MM-DD, DD-MM-YYYY, YYYY/MM/DD etc.
        parts = re.split(r"[-/]", bd)
        if len(parts) >= 3:
            p0, p1, p2 = parts[0], parts[1], parts[2]
            # If first part looks like a year (>=1900), assume YYYY-MM-DD
            try:
                y0 = int(p0)
            except Exception:
                y0 = 0

            if y0 >= 1900:
                month = int(p1)
                day   = int(p2)
            else:
                # Assume DD-MM-YYYY
                day   = int(p0)
                month = int(p1)
        elif len(parts) == 2:
            # MM-DD
            month = int(parts[0])
            day   = int(parts[1])
        else:
            return False

        today = date.today()
        return (month == today.month) and (day == today.day)
    except Exception:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def check_and_mark_birthday_greet(emp_id: str) -> bool:
    """
    Returns True if:
      - today is the user's birthday, AND
      - we have greeted them fewer than BIRTHDAY_MAX_GREETS_PER_DAY times today.
    Also increments the counter when it returns True.
    """
    if not emp_id or not _is_today_birthday(emp_id):
        return False

    ensure_birthday_table()
    today_str = date.today().strftime("%Y-%m-%d")

    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT count FROM birthday_greets WHERE emp_id=? AND date=?",
            (emp_id, today_str)
        ).fetchone()
        current = int(row["count"]) if row and row["count"] is not None else 0

        if current >= BIRTHDAY_MAX_GREETS_PER_DAY:
            # Already greeted enough times today
            return False

        if row:
            conn.execute(
                "UPDATE birthday_greets SET count = count + 1 WHERE emp_id=? AND date=?",
                (emp_id, today_str)
            )
        else:
            conn.execute(
                "INSERT INTO birthday_greets(emp_id, date, count) VALUES (?, ?, 1)",
                (emp_id, today_str)
            )
        conn.commit()
        return True
    except Exception as e:
        print("[birthday] check_and_mark error:", e)
        return False
    finally:
        conn.close()



def get_raw_sqlite_connection():
    """
    Return a plain sqlite3 connection for use with pandas read_sql_query.
    The _PersistentDBConnection wrapper is NOT compatible with pandas,
    so we open a short-lived raw connection for export queries only.
    """
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0)
    conn.row_factory = _sqlite3.Row
    return conn



# -----------------------------------------------------------------------------
# Thank-you events (anti-queue)
# -----------------------------------------------------------------------------
from collections import deque
from threading import Lock as _Lock

_THANKYOU_SEQ = 0
_THANKYOU_EVENTS = deque(maxlen=50)
_THANKYOU_LOCK = _Lock()
_TY_TTL_MS = 1500  # Only show events that are this recent to avoid queued popups
def _emit_thankyou(emp_id: str | None, name: str | None, medium: str, extra: dict | None = None):
    """Record a thank_you event and bump a global sequence counter."""
    global _THANKYOU_SEQ
    with _THANKYOU_LOCK:
        _THANKYOU_SEQ += 1
        ev = {
            "seq": _THANKYOU_SEQ,
            "ts": datetime.now().isoformat(timespec="seconds"),
            "ts_ms": int(time.time() * 1000),
            "emp_id": (emp_id or ""),
            "name": (name or ""),
            "medium": medium,
        }
        if extra:
            try:
                ev.update(extra)
            except Exception:
                pass
        _THANKYOU_EVENTS.append(ev)

# Streaming endpoint used by user.html thank-you watcher
@app.route("/api/thankyou_events")
def api_thankyou_events():
    """
    Client polls with /api/thankyou_events?since=<seq>
    Returns only the freshest recent event (TTL) and the latest global seq.
    """
    try:
        since = int(request.args.get("since", 0))
    except Exception:
        since = 0
    now_ms = int(time.time() * 1000)
    with _THANKYOU_LOCK:
        recent = [e for e in list(_THANKYOU_EVENTS)
                  if e["seq"] > since and (now_ms - e.get("ts_ms", now_ms)) <= _TY_TTL_MS]
        latest_seq = _THANKYOU_SEQ
    events = recent[-1:] if recent else []
    return jsonify({"events": events, "latest_seq": latest_seq})


# -----------------------------------------------------------------------------
# Admin / DB constants
# -----------------------------------------------------------------------------
ADMIN_PW_FILE = "admin_pw.txt"
USERS_IMG_DIR = "./users_img"
DB_PATH = globals().get("DB_PATH", "users.db")   # make sure this matches your app's DB_PATH
UDP_QUEUE_TABLE = globals().get("UDP_QUEUE_TABLE", "udp_queue")
DEFAULT_UDP_PORT = int(globals().get("UDP_PORT", 5006))
BROADCAST_ADDR = globals().get("BROADCAST_ADDR", "255.255.255.255")



# --- Prevent client/proxy caching for all JSON endpoints and streams ---
@app.after_request
def add_no_cache_headers(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


# -----------------------------------------------------------------------------
# DB initialization and schema
# -----------------------------------------------------------------------------
def create_users_table():
    """
    Create and migrate database schema.

    Key changes:
    1. Users table does NOT have template_id column (removed)
    2. created_at and updated_at have proper defaults
    3. fingerprint_map is the ONLY table storing template_id
    4. fingerprint_map has: emp_id, template_id, name, created_at, updated_at
    5. logs table now has success and created_at columns
    6. Migration logic is cleaner and more robust
    """
    from datetime import datetime

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # =========================================================================
    # USERS TABLE (WITHOUT template_id)
    # =========================================================================
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        emp_id        TEXT PRIMARY KEY,
        name          TEXT,
        created_at    TEXT DEFAULT (datetime('now', 'localtime')),
        updated_at    TEXT DEFAULT (datetime('now', 'localtime')),
        image_path    TEXT,
        encoding_path TEXT,
        role          TEXT DEFAULT 'User',
        birthdate     TEXT
    )
    """)
    conn.commit()

    # =========================================================================
    # MIGRATE USERS TABLE - Add missing columns
    # =========================================================================
    try:
        c.execute("PRAGMA table_info(users)")
        existing = {r[1] for r in c.fetchall()}

        # Columns to ensure exist (template_id intentionally excluded)
        wanted = [
            ("name",          "ALTER TABLE users ADD COLUMN name TEXT"),
            ("role",          "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'User'"),
            ("shift",         "ALTER TABLE users ADD COLUMN shift TEXT DEFAULT 'General'"),
            ("created_at",    "ALTER TABLE users ADD COLUMN created_at TEXT"),
            ("updated_at",    "ALTER TABLE users ADD COLUMN updated_at TEXT"),
            ("image_path",    "ALTER TABLE users ADD COLUMN image_path TEXT"),
            ("encoding_path", "ALTER TABLE users ADD COLUMN encoding_path TEXT"),
        ]

        changed = False
        for col, sql in wanted:
            if col not in existing:
                try:
                    c.execute(sql)
                    conn.commit()
                    print(f"[DB INIT] Added users.{col}")
                    changed = True
                except Exception as e:
                    print(f"[DB INIT] users.{col}: {e}")

        # Backfill NULL timestamps and defaults
        if changed or 'created_at' in existing:
            c.execute("UPDATE users SET created_at = ? WHERE created_at IS NULL", (now,))
            c.execute("UPDATE users SET updated_at = ? WHERE updated_at IS NULL", (now,))
            c.execute("UPDATE users SET role = 'User' WHERE role IS NULL")
            conn.commit()

        # Remove rfid_cards column from users (moved to rfid_card_map)
        if 'rfid_cards' in existing:
            print("[DB MIGRATION] Removing rfid_cards from users...")

            c.execute("""
                CREATE TABLE users_new (
                    emp_id        TEXT PRIMARY KEY,
                    name          TEXT,
                    created_at    TEXT DEFAULT (datetime('now', 'localtime')),
                    updated_at    TEXT DEFAULT (datetime('now', 'localtime')),
                    image_path    TEXT,
                    encoding_path TEXT,
                    role          TEXT DEFAULT 'User',
                    birthdate     TEXT
                )
            """)

            c.execute("""
                INSERT INTO users_new
                    (emp_id, name, created_at, updated_at,
                    image_path, encoding_path, role, birthdate)
                SELECT
                    emp_id, name, created_at, updated_at,
                    image_path, encoding_path, role, birthdate
                FROM users
            """)

            c.execute("DROP TABLE users")
            c.execute("ALTER TABLE users_new RENAME TO users")
            conn.commit()
            print("[DB MIGRATION] rfid_cards removed from users ✓")


        # REMOVE template_id if it exists (migrate to fingerprint_map)
        # Re-read columns since rfid_cards migration above may have changed the table
        current_cols = {row[1] for row in c.execute("PRAGMA table_info(users)").fetchall()}
        if 'template_id' in current_cols:
            print("[DB MIGRATION] Removing template_id from users (migrating to fingerprint_map)...")

            # Backup data with template_id before removal
            template_data = c.execute(
                "SELECT emp_id, name, template_id FROM users WHERE template_id IS NOT NULL"
            ).fetchall()

            # Create new table without template_id (and without rfid_cards which may already be gone)
            c.execute("""
                CREATE TABLE users_new (
                    emp_id        TEXT PRIMARY KEY,
                    name          TEXT,
                    created_at    TEXT DEFAULT (datetime('now', 'localtime')),
                    updated_at    TEXT DEFAULT (datetime('now', 'localtime')),
                    image_path    TEXT,
                    encoding_path TEXT,
                    role          TEXT DEFAULT 'User',
                    birthdate     TEXT
                )
            """)

            # Copy all data excluding template_id (and rfid_cards)
            c.execute("""
                INSERT INTO users_new (emp_id, name, role, birthdate, created_at, updated_at, encoding_path, image_path)
                SELECT emp_id, name, role, birthdate, created_at, updated_at, encoding_path, image_path
                FROM users
            """)

            # Replace old table
            c.execute("DROP TABLE users")
            c.execute("ALTER TABLE users_new RENAME TO users")
            conn.commit()

            print(f"[DB MIGRATION] Removed template_id, will migrate {len(template_data)} records to fingerprint_map")

            # Migrate template_id data to fingerprint_map
            for row in template_data:
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO fingerprint_map (emp_id, template_id, name, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (row['emp_id'], row['template_id'], row['name'], now, now))
                except Exception as e:
                    print(f"[DB MIGRATION] Failed to migrate {row['emp_id']}: {e}")
            conn.commit()

    except Exception as e:
        print(f"[DB MIGRATION] Error: {e}")
        import traceback
        traceback.print_exc()

    # =========================================================================
    # LOGS TABLE (added success and created_at columns)
    # =========================================================================
    c.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        emp_id     TEXT,
        name       TEXT,
        device_id  TEXT,
        mode       TEXT,
        ts         TEXT NOT NULL,
        success    INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now', 'localtime'))
    )
    """)

    # =========================================================================
    # MSSQL QUEUE TABLE
    # =========================================================================
    c.execute("""
    CREATE TABLE IF NOT EXISTS mssql_queue (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        index_id    INTEGER,
        emp_id      TEXT,
        emp_name    TEXT,
        device_id   TEXT,
        mode        TEXT,
        direction   TEXT,
        timestamp   TEXT NOT NULL,
        retries     INTEGER DEFAULT 0,
        last_sent   REAL DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now'))
    )
    """)

    # =========================================================================
    # APP SETTINGS TABLE
    # =========================================================================
    c.execute("""
    CREATE TABLE IF NOT EXISTS app_settings (
        key   TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS rfid_card_map (
        emp_id     TEXT PRIMARY KEY,
        rfid_card  TEXT UNIQUE NOT NULL,
        name       TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT DEFAULT (datetime('now','localtime'))
    )
    """)

    conn.commit()
    print("[DB INIT] rfid_card_map table ready ✓")

    # =========================================================================
    # FINGERPRINT_MAP TABLE (ONLY place for template_id)
    # =========================================================================
    c.execute("""
    CREATE TABLE IF NOT EXISTS fingerprint_map (
        emp_id      TEXT PRIMARY KEY,
        template_id INTEGER UNIQUE NOT NULL,
        name        TEXT,
        created_at  TEXT DEFAULT (datetime('now', 'localtime')),
        updated_at  TEXT DEFAULT (datetime('now', 'localtime'))
    )
    """)

    # =========================================================================
    # MIGRATE FINGERPRINT_MAP - Add missing columns
    # =========================================================================
    try:
        c.execute("PRAGMA table_info(fingerprint_map)")
        fp_cols = {r[1] for r in c.fetchall()}

        for col, sql in [
            ("name",       "ALTER TABLE fingerprint_map ADD COLUMN name TEXT"),
            ("created_at", "ALTER TABLE fingerprint_map ADD COLUMN created_at TEXT"),
            ("updated_at", "ALTER TABLE fingerprint_map ADD COLUMN updated_at TEXT"),
        ]:
            if col not in fp_cols:
                try:
                    c.execute(sql)
                    conn.commit()
                    print(f"[DB INIT] Added fingerprint_map.{col}")
                except Exception as e:
                    print(f"[DB INIT] fingerprint_map.{col}: {e}")

        # Backfill NULL timestamps in fingerprint_map
        c.execute("UPDATE fingerprint_map SET created_at = datetime('now', 'localtime') WHERE created_at IS NULL")
        c.execute("UPDATE fingerprint_map SET updated_at = datetime('now', 'localtime') WHERE updated_at IS NULL")

    except Exception as e:
        print(f"[DB MIGRATION] fingerprint_map error: {e}")

    # ─── CANTEEN HIERARCHY TABLES ──────────────────────────────────────
    c.execute("""CREATE TABLE IF NOT EXISTS shifts (
        shift_code TEXT PRIMARY KEY,
        shift_name TEXT NOT NULL,
        from_time  TEXT NOT NULL,
        to_time    TEXT NOT NULL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS time_slots (
        slot_code  TEXT PRIMARY KEY,
        shift_code TEXT NOT NULL,
        slot_name  TEXT NOT NULL,
        from_time  TEXT NOT NULL,
        to_time    TEXT NOT NULL,
        FOREIGN KEY (shift_code) REFERENCES shifts(shift_code) ON DELETE CASCADE
    )""")
 
    c.execute("""CREATE TABLE IF NOT EXISTS menu_codes (
        menu_code TEXT PRIMARY KEY,
        slot_code TEXT NOT NULL,
        menu_name TEXT NOT NULL,
        FOREIGN KEY (slot_code) REFERENCES time_slots(slot_code) ON DELETE CASCADE
    )""")
 
    c.execute("""CREATE TABLE IF NOT EXISTS items (
        item_code TEXT PRIMARY KEY,
        menu_code TEXT NOT NULL,
        item_name TEXT NOT NULL,
        FOREIGN KEY (menu_code) REFERENCES menu_codes(menu_code) ON DELETE CASCADE
    )""")
 
    c.execute("""CREATE TABLE IF NOT EXISTS item_limits (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        category   TEXT NOT NULL,
        item_name  TEXT NOT NULL,
        item_limit INTEGER NOT NULL
    )""")

    # ─── PG EVENT QUEUE (offline-first Postgres logging) ───────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS pg_event_queue (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        event_time TEXT NOT NULL,
        device_ip  TEXT,
        emp_id     TEXT,
        name       TEXT,
        role       TEXT,
        medium     TEXT NOT NULL,
        success    INTEGER NOT NULL,
        payload    TEXT
    )
    """)

    # ─── ORDERS ────────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id   TEXT UNIQUE NOT NULL,
        emp_id     TEXT NOT NULL,
        device_id  TEXT NOT NULL,
        shift_code TEXT,
        slot_code  TEXT,
        category   TEXT NOT NULL,
        item_code  TEXT NOT NULL,
        item_name  TEXT NOT NULL,
        qty        INTEGER NOT NULL DEFAULT 1,
        order_time TEXT NOT NULL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS user_slot_limits (
        emp_id          TEXT NOT NULL,
        slot_code       TEXT NOT NULL,
        per_item_max    INTEGER,
        slot_total_max  INTEGER,
        daily_total_max INTEGER,
        PRIMARY KEY(emp_id, slot_code)
    )
    """)
 
    c.execute("""
    CREATE TABLE IF NOT EXISTS default_slot_limits (
        slot_code       TEXT PRIMARY KEY,
        per_item_max    INTEGER,
        slot_total_max  INTEGER,
        daily_total_max INTEGER
    )
    """)
 
    c.execute("CREATE INDEX IF NOT EXISTS idx_orders_emp_cat_time ON orders(emp_id, category, order_time)")


    # =========================================================================
    # COMMIT AND CLOSE
    # =========================================================================
    conn.commit()
    conn.close()
    print("[DB INIT] Database schema ready ✓")


def has_column(table: str, column: str) -> bool:
    conn = get_db_connection()
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        cols = {r[1] for r in rows}  # (cid, name, type, ...)
        return column in cols
    finally:
        conn.close()

def ensure_schema_migrations():
    """
    Runs against the persistent connection after it is initialized.
    Safe for any DB state — checks columns before adding.
    NEVER adds template_id to users table.
    """
    conn = get_db_connection()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_cols(table):
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return {r[1] for r in rows}
        except Exception:
            return set()

    try:
        # ── users — only these columns, never template_id or BLOBs ───────────
        users_cols = get_cols("users")
        add_to_users = [
            ("name",          "ALTER TABLE users ADD COLUMN name TEXT"),
            ("role",          "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'User'"),
            ("shift",         "ALTER TABLE users ADD COLUMN shift TEXT DEFAULT 'General'"),
            ("created_at",    "ALTER TABLE users ADD COLUMN created_at TEXT"),
            ("updated_at",    "ALTER TABLE users ADD COLUMN updated_at TEXT"),
            ("image_path",    "ALTER TABLE users ADD COLUMN image_path TEXT"),
            ("encoding_path", "ALTER TABLE users ADD COLUMN encoding_path TEXT"),
            # template_id intentionally NOT in this list
            # face_encoding and display_image intentionally NOT in this list
        ]
        changed = False
        for col, sql in add_to_users:
            if col not in users_cols:
                try:
                    conn.execute(sql)
                    changed = True
                    print(f"[MIGRATION] Added users.{col}")
                except Exception as e:
                    print(f"[MIGRATION] users.{col}: {e}")

        if changed:
            conn.execute("UPDATE users SET created_at = ? WHERE created_at IS NULL", (now,))
            conn.execute("UPDATE users SET updated_at = ? WHERE updated_at IS NULL", (now,))
            conn.execute("UPDATE users SET role = 'User' WHERE role IS NULL")
            conn.commit()

        # ── fingerprint_map — template_id lives here only ─────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fingerprint_map (
                emp_id      TEXT PRIMARY KEY,
                template_id INTEGER UNIQUE NOT NULL,
                name        TEXT,
                created_at  TEXT DEFAULT (datetime('now', 'localtime')),
                updated_at  TEXT DEFAULT (datetime('now', 'localtime'))
            )
        """)
        conn.commit()
        print("[MIGRATION] Ensured fingerprint_map table")

       # ── rfid_card_map — single RFID per employee ─────────────────────
        conn.execute("""
        CREATE TABLE IF NOT EXISTS rfid_card_map (
            emp_id     TEXT PRIMARY KEY,
            rfid_card  TEXT UNIQUE NOT NULL,
            name       TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )
        """)

        # trigger to auto-update timestamp on update
        conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_rfid_updated_at
        AFTER UPDATE ON rfid_card_map
        FOR EACH ROW
        BEGIN
            UPDATE rfid_card_map
            SET updated_at = datetime('now','localtime')
            WHERE emp_id = OLD.emp_id;
        END;
        """)

        conn.commit()
        print("[MIGRATION] Ensured rfid_card_map table")

        fp_cols = get_cols("fingerprint_map")
        for col, sql in [
            ("name",       "ALTER TABLE fingerprint_map ADD COLUMN name TEXT"),
            ("created_at", "ALTER TABLE fingerprint_map ADD COLUMN created_at TEXT"),
            ("updated_at", "ALTER TABLE fingerprint_map ADD COLUMN updated_at TEXT"),
        ]:
            if col not in fp_cols:
                try:
                    conn.execute(sql)
                    conn.commit()
                    print(f"[MIGRATION] Added fingerprint_map.{col}")
                except Exception as e:
                    print(f"[MIGRATION] fingerprint_map.{col}: {e}")

        # ── logs ───────────────────────────────────────────────────────────────
        logs_cols = get_cols("logs")
        for col, sql in [
            ("success",    "ALTER TABLE logs ADD COLUMN success INTEGER DEFAULT 1"),
            ("created_at", "ALTER TABLE logs ADD COLUMN created_at TEXT"),
            ("device_id",  "ALTER TABLE logs ADD COLUMN device_id TEXT"),
        ]:
            if col not in logs_cols:
                try:
                    conn.execute(sql)
                    conn.commit()
                    print(f"[MIGRATION] Added logs.{col}")
                except Exception as e:
                    print(f"[MIGRATION] logs.{col}: {e}")

        # DO NOT create template_id in users table
        # DO NOT create user_finger_map table (deprecated)

        # ── Canteen tables ─────────────────────────────────────────────────
        for tbl_sql in [
            """CREATE TABLE IF NOT EXISTS shifts (
                shift_code TEXT PRIMARY KEY, shift_name TEXT NOT NULL,
                from_time TEXT NOT NULL, to_time TEXT NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS time_slots (
                slot_code TEXT PRIMARY KEY, shift_code TEXT NOT NULL,
                slot_name TEXT NOT NULL, from_time TEXT NOT NULL, to_time TEXT NOT NULL,
                FOREIGN KEY (shift_code) REFERENCES shifts(shift_code) ON DELETE CASCADE)""",
            """CREATE TABLE IF NOT EXISTS menu_codes (
                menu_code TEXT PRIMARY KEY, slot_code TEXT NOT NULL, menu_name TEXT NOT NULL,
                FOREIGN KEY (slot_code) REFERENCES time_slots(slot_code) ON DELETE CASCADE)""",
            """CREATE TABLE IF NOT EXISTS items (
                item_code TEXT PRIMARY KEY, menu_code TEXT NOT NULL, item_name TEXT NOT NULL,
                FOREIGN KEY (menu_code) REFERENCES menu_codes(menu_code) ON DELETE CASCADE)""",
            """CREATE TABLE IF NOT EXISTS item_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL,
                item_name TEXT NOT NULL, item_limit INTEGER NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS pg_event_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT, event_time TEXT NOT NULL,
                device_ip TEXT, emp_id TEXT, name TEXT, role TEXT,
                medium TEXT NOT NULL, success INTEGER NOT NULL, payload TEXT)""",
            """CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT UNIQUE NOT NULL,
                emp_id TEXT NOT NULL, device_id TEXT NOT NULL, shift_code TEXT,
                slot_code TEXT, category TEXT NOT NULL, item_code TEXT NOT NULL,
                item_name TEXT NOT NULL, qty INTEGER NOT NULL DEFAULT 1,
                order_time TEXT NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS user_slot_limits (
                emp_id TEXT NOT NULL, slot_code TEXT NOT NULL,
                per_item_max INTEGER, slot_total_max INTEGER, daily_total_max INTEGER,
                PRIMARY KEY(emp_id, slot_code))""",
            """CREATE TABLE IF NOT EXISTS default_slot_limits (
                slot_code TEXT PRIMARY KEY, per_item_max INTEGER,
                slot_total_max INTEGER, daily_total_max INTEGER)""",
        ]:
            try:
                conn.execute(tbl_sql)
                conn.commit()
            except Exception:
                pass
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_emp_cat_time ON orders(emp_id, category, order_time)")
            conn.commit()
        except Exception:
            pass
        print("[MIGRATION] Ensured canteen tables")

        conn.commit()
        print("[MIGRATION] ensure_schema_migrations complete ✓")

    finally:
        conn.close()

def load_fingerprint_templates_on_startup():
    """
    Load all fingerprint templates from fingerprint_bins/*.bin into sensor.
    Reads .bin files directly (498 bytes each) using the native GT-521F52
    two-phase SetTemplate protocol (CMD → ACK → DATA packet → ACK).
    Runs in a background thread to avoid blocking startup.
    """
    def _load_in_background():
        import serial as _serial
        import glob   as _glob
        import time   as _time

        PKT_CMD     = b"\x55\xAA"
        PKT_DATA    = b"\x5A\xA5"
        DEVICE_ID   = 0x0001
        CMD_ACK     = 0x30
        CMD_OPEN    = 0x01
        CMD_SET_TPL = 0x71
        TEMPLATE_SIZE = 498

        def chk16(d):
            return sum(d) & 0xFFFF

        def send_cmd(ser, code, param=0):
            pkt = bytearray(12)
            pkt[0:2]   = PKT_CMD
            pkt[2:4]   = DEVICE_ID.to_bytes(2, "little")
            pkt[4:8]   = (param & 0xFFFFFFFF).to_bytes(4, "little")
            pkt[8:10]  = (code & 0xFFFF).to_bytes(2, "little")
            pkt[10:12] = chk16(pkt[:10]).to_bytes(2, "little")
            ser.write(bytes(pkt))
            ser.flush()

        def read_exact(ser, n, timeout=5.0):
            end = _time.time() + timeout
            buf = bytearray()
            while len(buf) < n:
                chunk = ser.read(n - len(buf))
                if chunk:
                    buf.extend(chunk)
                elif _time.time() > end:
                    raise TimeoutError(
                        f"read_exact timeout: want={n} got={len(buf)}"
                    )
            return bytes(buf)

        def sync_to(ser, pat, timeout=5.0):
            end, win, plen = _time.time() + timeout, bytearray(), len(pat)
            while _time.time() < end:
                b = ser.read(1)
                if not b:
                    continue
                win += b
                if len(win) > plen:
                    win = win[-plen:]
                if bytes(win) == pat:
                    return
            raise TimeoutError(f"sync_to timeout {pat.hex()}")

        def read_resp(ser):
            sync_to(ser, PKT_CMD, 5.0)
            body = read_exact(ser, 10, 5.0)
            pkt  = PKT_CMD + body
            if chk16(pkt[:10]) != int.from_bytes(pkt[10:12], "little"):
                raise ValueError("Response checksum mismatch")
            return int.from_bytes(pkt[8:10], "little") == CMD_ACK

        try:
            _time.sleep(2)  # wait for system to fully start

            # ── Read fingerprint_map from DB to get emp_id → template_id ──
            conn = get_db_connection()
            try:
                fp_rows = conn.execute(
                    "SELECT emp_id, template_id FROM fingerprint_map "
                    "WHERE template_id IS NOT NULL ORDER BY template_id"
                ).fetchall()
            finally:
                conn.close()

            if not fp_rows:
                print("[STARTUP] No fingerprint_map entries — skipping sensor load")
                return

            # ── Build list of (emp_id, template_id, bin_path) to upload ───
            # Check fingerprint_bins/ first, then fingerprint_encodings/ as fallback
            bins_dir = os.path.join(os.getcwd(), "fingerprint_bins")
            enc_dir  = os.path.join(os.getcwd(), "fingerprint_encodings")
            to_upload = []
            for row in fp_rows:
                emp_id     = str(row["emp_id"])
                template_id = int(row["template_id"])
                bin_path   = os.path.join(bins_dir, f"{emp_id}.bin")
                dat_path   = os.path.join(enc_dir,  f"{emp_id}.dat")
                chosen = None
                if os.path.isfile(bin_path):
                    chosen = bin_path
                elif os.path.isfile(dat_path):
                    chosen = dat_path
                if not chosen:
                    print(f"[STARTUP] No .bin/.dat file for {emp_id} — skipping")
                    continue
                to_upload.append((emp_id, template_id, chosen))

            if not to_upload:
                print("[STARTUP] No .bin files found in fingerprint_bins/ — skipping sensor load")
                return

            print(f"[STARTUP] Loading {len(to_upload)} fingerprint .bin templates into sensor...")

            # ── Auto-detect serial port ────────────────────────────────────
            port = "/dev/ttyUSB0"
            for p in ("/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyACM0", "/dev/ttyACM1"):
                if os.path.exists(p):
                    port = p
                    break
            cands = _glob.glob("/dev/ttyUSB*") + _glob.glob("/dev/ttyACM*")
            if cands:
                port = cands[0]

            print(f"[STARTUP] Opening sensor on {port}")

            ser = _serial.Serial(port, 9600, timeout=0.1, write_timeout=2.0)
            _time.sleep(0.3)
            ser.reset_input_buffer()
            ser.reset_output_buffer()

            # Open sensor once for the whole session
            send_cmd(ser, CMD_OPEN, 0)
            if not read_resp(ser):
                print("[STARTUP] Sensor Open NACK — aborting template load")
                ser.close()
                return
            print("[STARTUP] Sensor opened OK")

            uploaded = 0
            failed   = 0

            for emp_id, template_id, bin_path in to_upload:
                try:
                    fp_bytes = open(bin_path, "rb").read()
                    if len(fp_bytes) != TEMPLATE_SIZE:
                        print(f"[STARTUP] Bad .bin size for {emp_id}: "
                              f"{len(fp_bytes)}B (expected {TEMPLATE_SIZE}B) — skipping")
                        failed += 1
                        continue

                    # SetTemplate CMD → ACK
                    send_cmd(ser, CMD_SET_TPL, template_id)
                    if not read_resp(ser):
                        print(f"[STARTUP] SetTemplate NACK: emp={emp_id} slot={template_id}")
                        failed += 1
                        _time.sleep(0.1)
                        continue

                    # DATA packet: START(2) + DeviceID(2) + Template(498) + Checksum(2)
                    dev_b    = DEVICE_ID.to_bytes(2, "little")
                    data_pkt = PKT_DATA + dev_b + fp_bytes
                    data_pkt += chk16(data_pkt).to_bytes(2, "little")
                    ser.write(data_pkt)
                    ser.flush()
                    _time.sleep(0.2)

                    # Final ACK
                    if read_resp(ser):
                        uploaded += 1
                        print(f"[STARTUP] ✓ emp={emp_id} slot={template_id}")
                    else:
                        failed += 1
                        print(f"[STARTUP] ✗ Final NACK: emp={emp_id} slot={template_id}")

                    _time.sleep(0.2)  # inter-employee delay

                except Exception as e:
                    failed += 1
                    print(f"[STARTUP] Error emp={emp_id} slot={template_id}: {e}")
                    try:
                        ser.reset_input_buffer()
                        ser.reset_output_buffer()
                    except Exception:
                        pass
                    _time.sleep(0.3)

            try:
                ser.close()
            except Exception:
                pass

            print(f"[STARTUP] Fingerprint load complete: "
                  f"{uploaded} uploaded, {failed} failed")

        except Exception as e:
            print(f"[STARTUP] Error loading fingerprint templates: {e}")
            import traceback
            traceback.print_exc()

    threading.Thread(target=_load_in_background, daemon=True).start()
    print("[STARTUP] Fingerprint template loading started in background")


def _safe_sqlite_alter(conn, sql):
    try:
        conn.execute(sql)
        conn.commit()
    except Exception:
        pass

def migrate_sqlite_schema():
    """
    Safe early migration against raw sqlite3 before persistent conn opens.
    NEVER adds face_encoding, display_image, or template_id to users.
    """
    from datetime import datetime as _dt

    if not os.path.exists(DB_PATH):
        print("[MIGRATE] No DB found — skipping early migration (create_users_table will handle it)")
        return  # Fresh install — create_users_table() handles it

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    now = _dt.now().strftime('%Y-%m-%d %H:%M:%S')

    def safe_alter(sql):
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass

    # Add ONLY the columns we want — no BLOBs, no template_id
    safe_alter("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'User'")
    safe_alter("ALTER TABLE users ADD COLUMN birthdate TEXT")
    safe_alter("ALTER TABLE users ADD COLUMN image_path TEXT")
    safe_alter("ALTER TABLE users ADD COLUMN encoding_path TEXT")
    safe_alter("ALTER TABLE users ADD COLUMN created_at TEXT")
    safe_alter("ALTER TABLE users ADD COLUMN updated_at TEXT")
    safe_alter("ALTER TABLE users ADD COLUMN name TEXT")
    safe_alter("ALTER TABLE users ADD COLUMN rfid_cards TEXT")
    # DO NOT add template_id here — fingerprint_map only
    # DO NOT add face_encoding or display_image

    # Backfill NULLs
    try:
        conn.execute("UPDATE users SET created_at = ? WHERE created_at IS NULL", (now,))
        conn.execute("UPDATE users SET updated_at = ? WHERE updated_at IS NULL", (now,))
        conn.execute("UPDATE users SET role = 'User' WHERE role IS NULL")
        conn.commit()
    except Exception:
        pass

    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_emp_id ON users(emp_id)")
        conn.commit()
    except Exception:
        pass

    conn.close()
    print("[MIGRATE] migrate_sqlite_schema complete")

migrate_sqlite_schema()

# Single persistent database connection for users.db
class _PersistentDBConnection:
    """Wrapper that ignores close() calls to keep connection alive."""
    def __init__(self, db_path):
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA cache_size=10000")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._lock = Lock()

    def execute(self, *args, **kwargs):
        with self._lock:
            return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        with self._lock:
            return self._conn.executemany(*args, **kwargs)

    def commit(self):
        with self._lock:
            return self._conn.commit()

    def rollback(self):
        with self._lock:
            return self._conn.rollback()

    def cursor(self):
        return self._conn.cursor()

    def close(self):
        pass  # Ignore close - connection stays open

    @property
    def row_factory(self):
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._conn.row_factory = value

_users_db_conn = _PersistentDBConnection(DB_PATH)
print("[DB] Single persistent users.db connection initialized with WAL mode")

def get_db_connection():
    """Return the single persistent database connection."""
    return _users_db_conn

# Initialize Device Sync Manager
def reload_face_recognizer_for_user(emp_id: str):
    """
    Callback function to reload face recognizer after receiving template.
    Called by DeviceSyncManager when a template is received.
    """
    try:
        recognizer.load_all_encodings()
        print(f"[SYNC] Reloaded face recognizer after receiving template for {emp_id}")
    except Exception as e:
        print(f"[SYNC] Error reloading face recognizer: {e}")


def init_device_sync_manager():
    """Initialize the device synchronization manager"""
    global device_sync_manager
    if device_sync_manager is None and NETWORK_ENABLED:
        device_sync_manager = DeviceSyncManager(
            get_db_conn_func=get_db_connection,
            udp_port=UDP_PORT,
            broadcast_addr=BROADCAST_ADDR,
            on_template_received=reload_face_recognizer_for_user
        )
        print("[SYNC] Device Sync Manager initialized with face recognizer reload callback")

# =============================================================================
# MSSQL DATABASE CONNECTION FUNCTIONS
# =============================================================================

def get_mssql_connection_params():
    """Get MSSQL connection parameters from app settings"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        params = {}
        for key in ['mssql_server', 'mssql_database', 'mssql_user', 'mssql_password', 'mssql_port']:
            row = cur.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
            params[key.replace('mssql_', '')] = row['value'] if row else ''
        conn.close()
        return params
    except Exception as e:
        print(f"[MSSQL] Error getting connection params: {e}")
        return {'server': '', 'database': '', 'user': '', 'password': '', 'port': '1433'}

def test_mssql_connection(server, database, user, password, port='1433'):
    """
    Test MSSQL connection and return status
    Returns: (success: bool, message: str)
    """
    if not MSSQL_AVAILABLE:
        return False, "MSSQL library not installed. Install with: pip install pymssql"

    if not server or not database or not user:
        return False, "Server, database, and user are required"

    try:
        port = int(port) if port else 1433

        # Try pymssql first
        if 'pymssql' in globals():
            conn = pymssql.connect(
                server=server,
                port=port,
                user=user,
                password=password,
                database=database,
                timeout=10,
                login_timeout=10
            )
            conn.close()
            return True, f"Connected successfully to {server}:{port}/{database}"

        # Try pyodbc
        elif 'pyodbc' in globals():
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"Timeout=10;"
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            conn.close()
            return True, f"Connected successfully to {server}:{port}/{database}"

        return False, "No MSSQL library available"

    except Exception as e:
        return False, f"Connection failed: {str(e)}"

def send_to_mssql(emp_id, name, mode, index_id=None):
    """
    Send login/attendance data to MSSQL server in the required format:
    - index_id: auto-incremental from SQLite logs
    - emp_id: employee ID
    - emp_name: employee name
    - device_id: device ID (e.g., IN_001, OUT_002)
    - mode: authentication mode (face, fingerprint, rfid)
    - direction: extracted from device_id (e.g., IN_001 -> IN)
    - timestamp: current datetime

    Returns: (success: bool, message: str)
    """
    if not MSSQL_AVAILABLE:
        return False, "MSSQL library not available"

    params = get_mssql_connection_params()
    if not params.get('server') or not params.get('database'):
        return False, "MSSQL not configured"

    try:
        port = int(params.get('port', 1433))
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Get device configuration
        device_id = get_device_id()  # e.g., IN_001, OUT_002, Canteen_001
        direction = get_device_direction()  # e.g., IN, OUT, Canteen

        # Get index_id from logs table if not provided
        if index_id is None:
            # Get the latest log entry ID for this event
            conn_sqlite = get_db_connection()
            row = conn_sqlite.execute(
                "SELECT id FROM logs WHERE emp_id=? AND mode=? ORDER BY id DESC LIMIT 1",
                (emp_id, mode)
            ).fetchone()
            index_id = row['id'] if row else None
            conn_sqlite.close()

        # Try pymssql first
        if 'pymssql' in globals():
            conn = pymssql.connect(
                server=params['server'],
                port=port,
                user=params['user'],
                password=params['password'],
                database=params['database'],
                timeout=5
            )
            cur = conn.cursor()

            # Insert attendance record with new format
            cur.execute(
                """
                INSERT INTO attendance_logs
                (index_id, emp_id, emp_name, device_id, mode, direction, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (index_id, emp_id, name, device_id, mode, direction, timestamp)
            )
            conn.commit()
            conn.close()
            return True, f"Data sent to MSSQL: {emp_id} | {device_id} | {direction}"

        # Try pyodbc
        elif 'pyodbc' in globals():
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={params['server']},{port};"
                f"DATABASE={params['database']};"
                f"UID={params['user']};"
                f"PWD={params['password']};"
            )
            conn = pyodbc.connect(conn_str, timeout=5)
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO attendance_logs
                (index_id, emp_id, emp_name, device_id, mode, direction, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (index_id, emp_id, name, device_id, mode, direction, timestamp)
            )
            conn.commit()
            conn.close()
            return True, f"Data sent to MSSQL: {emp_id} | {device_id} | {direction}"

        return False, "No MSSQL library available"

    except Exception as e:
        return False, f"MSSQL send failed: {str(e)}"


def queue_mssql_log(index_id, emp_id, emp_name, device_id, mode, direction, timestamp):
    """
    Queue a log entry for MSSQL transmission.
    Used when real-time sending fails or network is unavailable.

    Args:
        index_id: Local SQLite log ID
        emp_id: Employee ID
        emp_name: Employee name
        device_id: Device identifier
        mode: Authentication mode (face, fingerprint, rfid)
        direction: Direction (IN, OUT, etc.)
        timestamp: Timestamp string

    Returns:
        bool: True if queued successfully
    """
    try:
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO mssql_queue
            (index_id, emp_id, emp_name, device_id, mode, direction, timestamp, retries, last_sent)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (index_id, emp_id, emp_name, device_id, mode, direction, timestamp)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"[MSSQL Queue] Failed to queue log: {e}")
        return False


def send_to_mssql_with_queue(emp_id, name, mode, index_id=None):
    """
    Enhanced version of send_to_mssql that queues failed attempts.
    This provides offline-first functionality with automatic retry.

    Returns: (success: bool, message: str)
    """
    # Get device configuration
    device_id = get_device_id()
    direction = get_device_direction()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Get index_id if not provided
    if index_id is None:
        conn_sqlite = get_db_connection()
        row = conn_sqlite.execute(
            "SELECT id FROM logs WHERE emp_id=? AND mode=? ORDER BY id DESC LIMIT 1",
            (emp_id, mode)
        ).fetchone()
        index_id = row['id'] if row else None
        conn_sqlite.close()

    # Try to send immediately (real-time TCP/SQL transmission)
    success, message = send_to_mssql(emp_id, name, mode, index_id)

    if success:
        log_mssql_event("SENT", f"Real-time send: {emp_id}", {"name": name, "mode": mode})
        return True, message
    else:
        # Queue for later transmission if immediate send fails
        if queue_mssql_log(index_id, emp_id, name, device_id, mode, direction, timestamp):
            log_mssql_event("QUEUED", f"Offline queue: {emp_id}", {"name": name, "mode": mode, "reason": message})
            print(f"[MSSQL] Queued for MSSQL (offline): {emp_id}")
            return True, f"Queued for MSSQL (offline): {emp_id}"
        else:
            log_mssql_event("ERROR", f"Failed to queue: {emp_id}", {"name": name, "reason": message})
            return False, f"Failed to send and queue: {message}"


def mssql_is_available():
    """
    Check if MSSQL is configured and available.

    Returns:
        bool: True if MSSQL can be reached
    """
    if not MSSQL_AVAILABLE:
        return False

    params = get_mssql_connection_params()
    if not params.get('server') or not params.get('database'):
        return False

    success, _ = test_mssql_connection(
        server=params.get('server'),
        database=params.get('database'),
        user=params.get('user'),
        password=params.get('password', ''),
        port=str(params.get('port', '1433') or '1433')
    )
    return success


def drain_mssql_queue_once(max_batch=100, max_retries=5):
    """
    Drain up to max_batch entries from mssql_queue and send to SQL Server.
    Delete successfully sent entries, update retry count for failed ones.

    Args:
        max_batch: Maximum number of entries to process in one batch
        max_retries: Maximum retry attempts before giving up

    Returns:
        int: Number of successfully transmitted entries
    """
    if not mssql_is_available():
        raise RuntimeError("MSSQL not available")

    conn_local = get_db_connection()
    rows = conn_local.execute(
        """
        SELECT id, index_id, emp_id, emp_name, device_id, mode, direction, timestamp, retries
        FROM mssql_queue
        WHERE retries < ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (max_retries, max_batch)
    ).fetchall()

    if not rows:
        conn_local.close()
        return 0

    params = get_mssql_connection_params()
    port = int(params.get('port', 1433))

    inserted_ids = []
    failed_ids = []

    try:
        # Connect to MSSQL once for the entire batch
        if 'pymssql' in globals():
            conn_mssql = pymssql.connect(
                server=params['server'],
                port=port,
                user=params['user'],
                password=params['password'],
                database=params['database'],
                timeout=10
            )
            cur = conn_mssql.cursor()

            for r in rows:
                try:
                    cur.execute(
                        """
                        INSERT INTO attendance_logs
                        (index_id, emp_id, emp_name, device_id, mode, direction, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (r["index_id"], r["emp_id"], r["emp_name"],
                         r["device_id"], r["mode"], r["direction"], r["timestamp"])
                    )
                    inserted_ids.append(r["id"])
                except Exception as e:
                    print(f"[MSSQL Queue] Failed to insert record {r['id']}: {e}")
                    failed_ids.append(r["id"])

            conn_mssql.commit()
            conn_mssql.close()

        elif 'pyodbc' in globals():
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={params['server']},{port};"
                f"DATABASE={params['database']};"
                f"UID={params['user']};"
                f"PWD={params['password']};"
            )
            conn_mssql = pyodbc.connect(conn_str, timeout=10)
            cur = conn_mssql.cursor()

            for r in rows:
                try:
                    cur.execute(
                        """
                        INSERT INTO attendance_logs
                        (index_id, emp_id, emp_name, device_id, mode, direction, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (r["index_id"], r["emp_id"], r["emp_name"],
                         r["device_id"], r["mode"], r["direction"], r["timestamp"])
                    )
                    inserted_ids.append(r["id"])
                except Exception as e:
                    print(f"[MSSQL Queue] Failed to insert record {r['id']}: {e}")
                    failed_ids.append(r["id"])

            conn_mssql.commit()
            conn_mssql.close()

    except Exception as e:
        print(f"[MSSQL Queue] Batch processing error: {e}")
        # Mark all as failed if connection fails
        failed_ids.extend([r["id"] for r in rows if r["id"] not in inserted_ids])

    # Delete successfully sent entries
    if inserted_ids:
        conn_local.executemany(
            "DELETE FROM mssql_queue WHERE id = ?",
            [(rid,) for rid in inserted_ids]
        )
        log_mssql_event("BATCH_SENT", f"Sent {len(inserted_ids)} queued logs", {"ids": inserted_ids[:10]})

    # Update retry count and last_sent for failed entries
    if failed_ids:
        current_time = time.time()
        conn_local.executemany(
            "UPDATE mssql_queue SET retries = retries + 1, last_sent = ? WHERE id = ?",
            [(current_time, rid) for rid in failed_ids]
        )
        log_mssql_event("BATCH_RETRY", f"Retry scheduled for {len(failed_ids)} logs", {"ids": failed_ids[:10]})

    conn_local.commit()
    conn_local.close()

    return len(inserted_ids)


# Global sync info for MSSQL (similar to PG)
_mssql_sync_info = {
    "last_ok": None,
    "last_err": None,
}


def mssql_sync_worker(stop_evt: Event, poll_ok=5, poll_empty=10, poll_fail=20):
    """
    Background worker thread that continuously drains the MSSQL queue.
    Automatically retries failed transmissions when network is restored.

    Args:
        stop_evt: Threading event to signal worker shutdown
        poll_ok: Sleep time after successful batch (seconds)
        poll_empty: Sleep time when queue is empty (seconds)
        poll_fail: Sleep time after failure (seconds)
    """
    if not MSSQL_AVAILABLE:
        print("[MSSQL Worker] MSSQL library not available, worker disabled")
        return

    print("[MSSQL Worker] Started - monitoring queue for offline logs")

    while not stop_evt.is_set():
        try:
            # Check if MSSQL is configured
            params = get_mssql_connection_params()
            if not params.get('server') or not params.get('database'):
                # Not configured yet, wait longer
                time.sleep(poll_empty)
                continue

            # Try to drain queue
            count = drain_mssql_queue_once()

            if count > 0:
                _mssql_sync_info["last_ok"] = f"{datetime.now().isoformat(timespec='seconds')} (sent {count} logs)"
                print(f"[MSSQL Worker] Successfully sent {count} queued logs")
                time.sleep(poll_ok)
            else:
                _mssql_sync_info["last_ok"] = f"{datetime.now().isoformat(timespec='seconds')} (queue empty)"
                time.sleep(poll_empty)

        except Exception as e:
            _mssql_sync_info["last_err"] = f"{datetime.now().isoformat(timespec='seconds')}: {e}"
            print(f"[MSSQL Worker] Error: {e}")
            time.sleep(poll_fail)

    print("[MSSQL Worker] Stopped")


# =============================================================================
# MSSQL Queue Logging System - Writes status to file for monitoring
# =============================================================================
MSSQL_LOG_FILE = "mssql_queue.log"
MSSQL_LOG_MAX_LINES = 1000  # Keep last N lines

def log_mssql_event(event_type, message, details=None):
    """
    Log MSSQL queue events to a file for monitoring and debugging.

    Args:
        event_type: Type of event (QUEUED, SENT, ERROR, CONNECT, DISCONNECT)
        message: Brief message describing the event
        details: Optional dict with additional details
    """
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_line = f"[{timestamp}] [{event_type}] {message}"
        if details:
            log_line += f" | {json.dumps(details)}"
        log_line += "\n"

        # Append to log file
        with open(MSSQL_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_line)

        # Trim log file if too large
        _trim_mssql_log()
    except Exception as e:
        print(f"[MSSQL Log] Failed to write log: {e}")

def _trim_mssql_log():
    """Keep log file to a reasonable size by trimming old entries."""
    try:
        if not os.path.exists(MSSQL_LOG_FILE):
            return
        with open(MSSQL_LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if len(lines) > MSSQL_LOG_MAX_LINES:
            with open(MSSQL_LOG_FILE, 'w', encoding='utf-8') as f:
                f.writelines(lines[-MSSQL_LOG_MAX_LINES:])
    except Exception:
        pass

def get_mssql_queue_status():
    """
    Get current MSSQL queue status for monitoring.

    Returns:
        dict: Status information including queue count, last sync, connection status
    """
    try:
        conn = get_db_connection()
        row = conn.execute("SELECT COUNT(*) as cnt FROM mssql_queue").fetchone()
        pending_count = row['cnt'] if row else 0

        retry_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM mssql_queue WHERE retries > 0"
        ).fetchone()
        retry_count = retry_row['cnt'] if retry_row else 0

        conn.close()

        params = get_mssql_connection_params()
        is_configured = bool(params.get('server') and params.get('database'))

        return {
            "configured": is_configured,
            "server": params.get('server', ''),
            "database": params.get('database', ''),
            "pending_count": pending_count,
            "retry_count": retry_count,
            "last_ok": _mssql_sync_info.get("last_ok"),
            "last_err": _mssql_sync_info.get("last_err"),
        }
    except Exception as e:
        return {"error": str(e)}

# Credentials file for storing login configuration
CREDENTIALS_FILE = "mssql_credentials.json"

def save_mssql_credentials_to_file():
    """
    Save MSSQL credentials to a JSON file for backup/restore.
    Excludes password for security.
    """
    try:
        params = get_mssql_connection_params()
        creds = {
            "server": params.get('server', ''),
            "port": params.get('port', '1433'),
            "database": params.get('database', ''),
            "user": params.get('user', ''),
            "last_updated": datetime.now().isoformat(),
        }
        with open(CREDENTIALS_FILE, 'w', encoding='utf-8') as f:
            json.dump(creds, f, indent=2)
        log_mssql_event("CONFIG", "Credentials file updated", creds)
        return True
    except Exception as e:
        print(f"[MSSQL] Failed to save credentials file: {e}")
        return False

def load_mssql_credentials_from_file():
    """
    Load MSSQL credentials from JSON file.
    Returns dict with server/port/database/user (no password).
    """
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            return None
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[MSSQL] Failed to load credentials file: {e}")
        return None


def ensure_app_settings_schema():
    """
    Ensure app_settings exists and has `key` as PRIMARY KEY.
    If an older version exists without PK, migrate it in-place.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1) Does app_settings exist?
        cur.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='app_settings'
        """)
        row = cur.fetchone()

        if not row:
            # Create fresh table with correct schema
            cur.execute("""
                CREATE TABLE app_settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            conn.commit()
            return

        # 2) Check if `key` is already PRIMARY KEY
        cur.execute("PRAGMA table_info(app_settings)")
        cols = cur.fetchall()
        # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
        has_pk_on_key = any(c[1] == "key" and c[5] == 1 for c in cols)

        if has_pk_on_key:
            # Schema already correct
            return

        # 3) Migrate: rename old -> new with PK, copy data
        conn.execute("BEGIN")
        cur.execute("ALTER TABLE app_settings RENAME TO app_settings_old")

        cur.execute("""
            CREATE TABLE app_settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # INSERT OR IGNORE so duplicate keys don't break the migration
        cur.execute("""
            INSERT OR IGNORE INTO app_settings(key, value)
            SELECT key, value FROM app_settings_old
        """)

        cur.execute("DROP TABLE app_settings_old")
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Run app_settings schema migration once at startup
ensure_app_settings_schema()

# ========= NEW: ensure logs table =========
def ensure_logs_table():
    """Ensure logs table exists with proper schema."""
    conn = get_db_connection()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        emp_id TEXT NOT NULL,
        name TEXT,
        device_id TEXT,
        mode TEXT,
        ts TEXT NOT NULL,
        success INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now', 'localtime'))
    )
    """)
    cur = conn.execute("PRAGMA table_info(logs)")
    columns = {row[1] for row in cur.fetchall()}
    if 'success' not in columns:
        conn.execute("ALTER TABLE logs ADD COLUMN success INTEGER DEFAULT 1")
        print("[logs] Added 'success' column")
    if 'created_at' not in columns:
        conn.execute("ALTER TABLE logs ADD COLUMN created_at TEXT DEFAULT (datetime('now', 'localtime'))")
        print("[logs] Added 'created_at' column")
    if 'item_name' not in columns:
        conn.execute("ALTER TABLE logs ADD COLUMN item_name TEXT DEFAULT ''")
        print("[logs] Added 'item_name' column")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts_desc ON logs(ts DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_emp_ts ON logs(emp_id, ts DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_emp ON logs(emp_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_id ON logs(id DESC)")
    conn.commit()


def get_setting(key, default=None):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key=?",
        (key,)
    ).fetchone()
    conn.close()
    if row and row["value"] is not None:
        return row["value"]
    return default


def set_setting(key, value):
    conn = get_db_connection()
    conn.execute(
        "INSERT INTO app_settings(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    conn.commit()
    conn.close()
    
PGCFG = {}
PG_CONNECT_TIMEOUT = 3
 
def load_pgcfg_from_settings():
    """Load Postgres creds from SQLite app_settings."""
    global PGCFG
    PGCFG = {
        "host":     get_setting("pg_host",     "192.168.1.3"),
        "port":     int(get_setting("pg_port", "5432") or "5432"),
        "dbname":   get_setting("pg_dbname",   "postgres"),
        "user":     get_setting("pg_user",     "postgres"),
        "password": get_setting("pg_password", "postgres"),
    }
 
def pg_connect():
    return psycopg2.connect(
        host=PGCFG["host"], port=PGCFG["port"],
        dbname=PGCFG["dbname"], user=PGCFG["user"],
        password=PGCFG["password"],
        connect_timeout=PG_CONNECT_TIMEOUT,
    )
 
def ensure_pg_table():
    try:
        conn = pg_connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS login_events (
                    id         BIGSERIAL PRIMARY KEY,
                    event_time TIMESTAMPTZ NOT NULL DEFAULT now(),
                    device_ip  INET,
                    emp_id     TEXT,
                    name       TEXT,
                    role       TEXT,
                    medium     TEXT NOT NULL,
                    success    BOOLEAN NOT NULL,
                    payload    JSONB
                );
            """)
            cur.execute("ALTER TABLE login_events ADD COLUMN IF NOT EXISTS role TEXT;")
        conn.close()
    except Exception as e:
        print(f"[PG] ensure_pg_table error: {e}")
 
def pg_is_available() -> bool:
    try:
        c = pg_connect(); c.close(); return True
    except Exception:
        return False
 
def pg_log_event_or_queue(emp_id, name, medium, success, payload: dict):
    """Log to Postgres; if offline, queue in SQLite for later sync."""
    role = get_user_role(emp_id) if emp_id else None
    try:
        conn = pg_connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO login_events (device_ip, emp_id, name, role, medium, success, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb);
            """, (
                get_self_ip(),
                str(emp_id) if emp_id else None,
                name, role, medium, bool(success),
                json.dumps(payload or {}),
            ))
        conn.close()
        return True
    except Exception:
        try:
            conn_local = get_db_connection()
            conn_local.execute("""
                INSERT INTO pg_event_queue
                (event_time, device_ip, emp_id, name, role, medium, success, payload)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                datetime.now().isoformat(timespec="seconds"),
                get_self_ip(),
                str(emp_id) if emp_id else None,
                name, role, medium, 1 if success else 0,
                json.dumps(payload or {}),
            ))
            conn_local.commit()
            conn_local.close()
        except Exception as e:
            print(f"[PG] enqueue failed: {e}")
        return False
 
# ── Background PG Sync ────────────────────────────────────────────────
_last_pg_sync_info = {"last_ok": None, "last_err": None}
 
def drain_pg_queue_once(max_batch=200):
    if not pg_is_available():
        raise RuntimeError("PG not available")
    ensure_pg_table()
    conn_local = get_db_connection()
    rows = conn_local.execute(
        "SELECT id, event_time, device_ip, emp_id, name, role, medium, success, payload "
        "FROM pg_event_queue ORDER BY id ASC LIMIT ?", (max_batch,)
    ).fetchall()
    if not rows:
        conn_local.close(); return 0
    conn_pg = pg_connect()
    conn_pg.autocommit = True
    cur = conn_pg.cursor()
    inserted_ids = []
    try:
        for r in rows:
            cur.execute("""
                INSERT INTO login_events (event_time, device_ip, emp_id, name, role, medium, success, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
            """, (r["event_time"], r["device_ip"], r["emp_id"], r["name"], r["role"],
                  r["medium"], bool(r["success"]), r["payload"] or "{}"))
            inserted_ids.append(r["id"])
    finally:
        cur.close(); conn_pg.close()
    if inserted_ids:
        conn_local.executemany("DELETE FROM pg_event_queue WHERE id = ?",
                               [(rid,) for rid in inserted_ids])
        conn_local.commit()
    conn_local.close()
    return len(inserted_ids)
 
def pg_sync_worker(stop_evt, poll_ok=5, poll_empty=10, poll_fail=20):
    import time as _t
    while not stop_evt.is_set():
        try:
            count = drain_pg_queue_once()
            if count > 0:
                _last_pg_sync_info["last_ok"] = f"{datetime.now().isoformat(timespec='seconds')} (drained {count})"
                _t.sleep(poll_ok)
            else:
                _last_pg_sync_info["last_ok"] = f"{datetime.now().isoformat(timespec='seconds')} (empty)"
                _t.sleep(poll_empty)
        except Exception as e:
            _last_pg_sync_info["last_err"] = f"{datetime.now().isoformat(timespec='seconds')}: {e}"
            _t.sleep(poll_fail)
 
# Load PG config on import
load_pgcfg_from_settings()
if pg_is_available():
    ensure_pg_table()

def get_current_time_ui():
    return datetime.now().strftime('%H:%M')
 
def is_canteen_open_ui():
    db = get_db_connection()
    now = get_current_time_ui()
    row = db.execute("SELECT * FROM time_slots WHERE from_time <= ? AND to_time >= ?",
                     (now, now)).fetchone()
    db.close()
    return bool(row)
 
def get_next_opening_ui():
    db = get_db_connection()
    now = get_current_time_ui()
    row = db.execute("SELECT from_time FROM time_slots WHERE from_time > ? ORDER BY from_time ASC LIMIT 1",
                     (now,)).fetchone()
    db.close()
    return row[0] if row else None
 
def get_current_slot_row():
    db = get_db_connection()
    now = get_current_time_ui()
    row = db.execute("""
        SELECT ts.*, sh.shift_name
        FROM time_slots ts
        JOIN shifts sh ON sh.shift_code = ts.shift_code
        WHERE ts.from_time <= ? AND ts.to_time >= ?
        ORDER BY ts.from_time LIMIT 1
    """, (now, now)).fetchone()
    db.close()
    return row
 
def get_active_slot_code():
    slot = get_current_slot_row()
    return (slot["slot_code"] if slot else None, slot)

def generate_order_id():
    """Generate order ID: canteen_id-device_id-sequential_number (e.g. Hall_A-CAN_001-009)
    
    Collision-safe: verifies the candidate ID doesn't already exist before returning.
    """
    conn = get_db_connection()
    try:
        canteen_id = get_canteen_id()
        device_id = get_device_id()
        prefix = f"{canteen_id}-{device_id}" if canteen_id else device_id

        # Find the highest sequence number used today
        row = conn.execute(
            """SELECT order_id FROM orders
               WHERE DATE(order_time) = DATE('now','localtime')
               AND order_id LIKE ?
               ORDER BY id DESC LIMIT 1""",
            (prefix + "-%",)
        ).fetchone()

        seq = 1
        if row and row["order_id"]:
            oid = str(row["order_id"])
            suffix = oid[len(prefix) + 1:]
            base_num = suffix.split("-")[0]
            try:
                seq = int(base_num) + 1
            except ValueError:
                seq = 1

        # Collision check: keep incrementing until we find a free order_id
        for _ in range(100):
            candidate = f"{prefix}-{seq:03d}"
            existing = conn.execute(
                "SELECT 1 FROM orders WHERE order_id = ? LIMIT 1",
                (candidate,)
            ).fetchone()
            if not existing:
                return candidate
            seq += 1

        # Ultimate fallback: UUID guarantees uniqueness
        return f"{prefix}-{uuid.uuid4().hex[:6].upper()}"
    except Exception:
        canteen_id = get_canteen_id()
        device_id = get_device_id()
        prefix = f"{canteen_id}-{device_id}" if canteen_id else device_id
        return f"{prefix}-{uuid.uuid4().hex[:6].upper()}"
    finally:
        conn.close()
 
def _get_category_limit(db, category: str, item_name: str):
    r = db.execute("SELECT item_limit FROM item_limits WHERE category=? AND item_name=? LIMIT 1",
                   (category, item_name)).fetchone()
    if r: return int(r["item_limit"])
    r = db.execute("SELECT item_limit FROM item_limits WHERE category=? AND item_name='*' LIMIT 1",
                   (category,)).fetchone()
    return int(r["item_limit"]) if r else 999
 
def _count_taken_today(db, emp_id: str, category: str):
    return db.execute("""
        SELECT COUNT(*) AS c FROM orders
        WHERE emp_id=? AND category=? AND DATE(order_time)=DATE('now','localtime')
    """, (emp_id, category)).fetchone()["c"]
 
def _shorten_name(full_name: str) -> str:
    """'Darshika Patil' -> 'Darshika P'"""
    parts = (full_name or "").strip().split()
    if len(parts) <= 1:
        return (full_name or "").strip()
    return parts[0] + " " + parts[-1][0]


def _shorten_item(item: str) -> str:
    """'Vada Pav' -> 'Vada P'"""
    parts = (item or "").strip().split()
    if len(parts) <= 1:
        return (item or "").strip()
    return parts[0] + " " + parts[-1][0]

def print_order_receipt(order_id, emp_id, item_name, category, slot_name, shift_name, item_code=""):
    from fingerprint import print_user_id_and_cut
    uname = ""
    try:
        _conn = get_db_connection()
        _row = _conn.execute("SELECT name FROM users WHERE emp_id=?", (emp_id,)).fetchone()
        if _row and _row["name"]:
            uname = _row["name"]
        _conn.close()
    except Exception:
        pass
    canteen_id = get_canteen_id()
    last_line = f"{canteen_id}, {get_device_id()}" if canteen_id else get_device_id()
    text = (
        f"{emp_id}, {_shorten_name(uname)}\n"
        f"{item_code}-{_shorten_item(item_name)}\n"
        f"{datetime.now().strftime('%d-%m-%Y, %H:%M:%S')}\n"
        f"{last_line}\n"
    )
    try:
        print_user_id_and_cut(text)
    except Exception as e:
        print(f"[Printer] order slip error: {e}")
 
def get_slot_limits(conn, emp_id: str, slot_code: str):
    row = conn.execute("""
        SELECT per_item_max, slot_total_max, daily_total_max
        FROM user_slot_limits WHERE emp_id=? AND slot_code=? LIMIT 1
    """, (emp_id, slot_code)).fetchone()
    if row:
        return dict(row)
    row = conn.execute("""
        SELECT per_item_max, slot_total_max, daily_total_max
        FROM default_slot_limits WHERE slot_code=? LIMIT 1
    """, (slot_code,)).fetchone()
    return dict(row) if row else {"per_item_max": None, "slot_total_max": None, "daily_total_max": None}
 
def get_usage_today(conn, emp_id: str, slot_code: str):
    r = conn.execute("""
        SELECT COALESCE(SUM(qty),0) AS total FROM orders
        WHERE emp_id=? AND slot_code=? AND DATE(order_time)=DATE('now','localtime')
    """, (emp_id, slot_code)).fetchone()
    slot_total = int(r["total"])
    r = conn.execute("""
        SELECT COALESCE(SUM(qty),0) AS total FROM orders
        WHERE emp_id=? AND DATE(order_time)=DATE('now','localtime')
    """, (emp_id,)).fetchone()
    day_total = int(r["total"])
    rows = conn.execute("""
        SELECT item_code, COALESCE(SUM(qty),0) AS qty FROM orders
        WHERE emp_id=? AND slot_code=? AND DATE(order_time)=DATE('now','localtime')
        GROUP BY item_code
    """, (emp_id, slot_code)).fetchall()
    per_item = {str(rr["item_code"]): int(rr["qty"]) for rr in rows} if rows else {}
    return {"slot_total_today": slot_total, "day_total_today": day_total, "per_item_today": per_item}
 
class SlotLimitError(Exception):
    def __init__(self, code: str, message: str, meta: dict | None = None):
        super().__init__(message)
        self.code = code
        self.meta = meta or {}
 
def validate_items_against_limits(conn, *, emp_id, slot_code, grouped_items):
    limits = get_slot_limits(conn, emp_id, slot_code)
    pim = limits.get("per_item_max")
    stm = limits.get("slot_total_max")
    dtm = limits.get("daily_total_max")
    usage = get_usage_today(conn, emp_id, slot_code)
    order_qty = sum(max(0, int(q)) for q in grouped_items.values())
    if pim is not None:
        for code, q in grouped_items.items():
            q = max(0, int(q))
            already = int(usage["per_item_today"].get(str(code), 0))
            if already + q > int(pim):
                raise SlotLimitError("per_item_exceeded", f"Per-item limit exceeded for {code}.",
                    {"item_code": code, "limit": int(pim), "used": already, "requested": q,
                     "remaining": max(0, int(pim) - already)})
    if stm is not None and usage["slot_total_today"] + order_qty > int(stm):
        raise SlotLimitError("slot_total_exceeded", "Slot total limit exceeded.",
            {"limit": int(stm), "used": usage["slot_total_today"], "requested": order_qty,
             "remaining": max(0, int(stm) - usage["slot_total_today"])})
    if dtm is not None and usage["day_total_today"] + order_qty > int(dtm):
        raise SlotLimitError("daily_total_exceeded", "Daily total limit exceeded.",
            {"limit": int(dtm), "used": usage["day_total_today"], "requested": order_qty,
             "remaining": max(0, int(dtm) - usage["day_total_today"])})
    return {"ok": True, "limits": limits, "used": usage, "order_total_qty": order_qty}
 
def _resolve_item_full(conn, item_obj):
    """Resolve flexible item payloads to (item_code, qty)."""
    if isinstance(item_obj, str):
        return item_obj.strip(), 1
    if not isinstance(item_obj, dict):
        return None, None
    def _deep(v):
        if isinstance(v, str): return v
        if isinstance(v, dict):
            for kk in ("item_code","code","id","item_id","value"):
                vv = v.get(kk)
                if isinstance(vv, str) and vv.strip(): return vv
        return None
    code = None
    for k in ("item_code","code","item","id","item_id"):
        if k in item_obj and item_obj[k] is not None:
            code = _deep(item_obj[k])
            if code: break
    if not code:
        look_name = None
        for k in ("item_name","name","label","title"):
            v = item_obj.get(k)
            if isinstance(v, str): look_name = v.strip()
            elif isinstance(v, dict):
                for kk in ("text","value","name","label"):
                    vv = v.get(kk)
                    if isinstance(vv, str) and vv.strip(): look_name = vv.strip(); break
            if look_name: break
        if look_name:
            row = conn.execute("SELECT item_code FROM items WHERE lower(item_name)=lower(?) LIMIT 1",
                               (look_name,)).fetchone()
            if row: code = row["item_code"]
    try: qty = int(item_obj.get("qty", 1))
    except Exception: qty = 1
    if qty <= 0: qty = 1
    return (code.strip() if isinstance(code, str) else None), qty
 
def group_items_for_limits(conn, items_in: list):
    grouped = {}
    for raw in items_in:
        code, qty = _resolve_item_full(conn, raw)
        if not code: continue
        qty = int(qty) if str(qty).isdigit() else 1
        if qty <= 0: qty = 1
        grouped[code] = grouped.get(code, 0) + qty
    return grouped
 
def place_order_core(emp_id: str, item_code: str, qty: int = 1, skip_log: bool = False, skip_print: bool = False, order_id: str = None):
    slot = get_current_slot_row()
    if not slot:
        return False, {"reason": "closed", "message": "Canteen is closed"}
    db = get_db_connection()
    try:
        item = db.execute("""
            SELECT i.item_code, i.item_name, m.menu_name AS category,
                   ts.slot_code, ts.slot_name, sh.shift_code, sh.shift_name
            FROM items i
            JOIN menu_codes m ON m.menu_code = i.menu_code
            JOIN time_slots ts ON ts.slot_code = m.slot_code
            JOIN shifts sh ON sh.shift_code = ts.shift_code
            WHERE i.item_code=? LIMIT 1
        """, (item_code,)).fetchone()
        if not item:
            return False, {"reason": "unknown_item", "message": "Unknown item"}
        if item["slot_code"] != slot["slot_code"]:
            return False, {"reason": "wrong_slot", "message": "Item not available in current slot"}
        limit = _get_category_limit(db, item["category"], item["item_name"])
        taken = _count_taken_today(db, emp_id, item["category"])
        if taken + qty > limit:
            return False, {"reason": "limit_exceeded", "message": "Limit already reached"}
        oid = order_id or generate_order_id()
        did = get_device_id()
        db.execute("""
            INSERT INTO orders (order_id, emp_id, device_id, shift_code, slot_code,
                                category, item_code, item_name, qty, order_time)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (oid, emp_id, did, item["shift_code"], item["slot_code"],
              item["category"], item["item_code"], item["item_name"], qty, _now_iso()))
        db.commit()
        if not skip_print:
            print_order_receipt(oid, emp_id, item["item_name"], item["category"],
                                item["slot_name"], item["shift_name"], item["item_code"])

        # Log canteen order to logs table with item_name
        # Log canteen order to logs table with item_name
        if not skip_log:
            try:
                _user_row = db.execute("SELECT name FROM users WHERE emp_id=?", (emp_id,)).fetchone()
                _user_name = _user_row["name"] if _user_row and _user_row["name"] else ""
                item_label = f"{item['item_name']} x{qty}" if qty > 1 else item["item_name"]
                insert_login_log(emp_id, _user_name, "canteen", success=True, item_name=item_label)
            except Exception as e:
                print(f"[ORDER] Failed to log canteen order: {e}")

        return True, {"order_id": oid, "item": dict(item)}
    finally:
        db.close()
    
import socket
import threading
import json
import uuid
import time
from datetime import datetime

UDP_SEND_TIMEOUT = 0.6

# In-memory structures
KNOWN_DEVICES = {}           # ip -> {"ip": ip, "last_seen": ts, "info": {...}}
KNOWN_DEVICES_LOCK = threading.Lock()
_ACKED_MSGS = set()
_ACKED_MSGS_LOCK = threading.Lock()

# ── Chunk reassembly buffer ──────────────────────────────────────────────────
# Stores partially-received chunked messages.
# Key: chunk_id -> {"chunks": {num: bytes}, "total": int, "ts": float}
_CHUNK_BUFFER = {}
_CHUNK_BUFFER_LOCK = threading.Lock()
_CHUNK_BUFFER_TTL = 120  # seconds – discard incomplete chunk sets after this


def _reassemble_chunked_message(obj):
    """
    Accept a single chunk envelope and try to reassemble the full message.
    Returns the parsed dict if all chunks are present, otherwise None.
    Thread-safe via _CHUNK_BUFFER_LOCK.
    """
    chunk_id = obj.get("chunk_id")
    chunk_num = obj.get("chunk_num")
    total_chunks = obj.get("total_chunks")
    chunk_data_b64 = obj.get("data")

    if chunk_id is None or chunk_num is None or total_chunks is None or not chunk_data_b64:
        return None

    try:
        chunk_bytes = base64.b64decode(chunk_data_b64)
    except Exception as e:
        print(f"[CHUNK] base64 decode error for chunk {chunk_num}/{total_chunks} of {chunk_id}: {e}")
        return None

    now = time.time()

    with _CHUNK_BUFFER_LOCK:
        # Expire old incomplete entries
        expired = [k for k, v in _CHUNK_BUFFER.items() if now - v["ts"] > _CHUNK_BUFFER_TTL]
        for k in expired:
            received = len(_CHUNK_BUFFER[k]["chunks"])
            expected = _CHUNK_BUFFER[k]["total"]
            print(f"[CHUNK] Expiring incomplete chunk set {k} ({received}/{expected} received)")
            del _CHUNK_BUFFER[k]

        # Store this chunk
        if chunk_id not in _CHUNK_BUFFER:
            _CHUNK_BUFFER[chunk_id] = {"chunks": {}, "total": total_chunks, "ts": now}

        entry = _CHUNK_BUFFER[chunk_id]
        entry["chunks"][chunk_num] = chunk_bytes
        entry["ts"] = now  # refresh timestamp on each chunk received

        received_count = len(entry["chunks"])

        if received_count < total_chunks:
            # Not complete yet
            if received_count % 5 == 0 or received_count == 1:
                print(f"[CHUNK] Progress: {received_count}/{total_chunks} for {chunk_id}")
            return None

        # All chunks received – reassemble
        try:
            full_data = b"".join(entry["chunks"][i] for i in range(total_chunks))
            del _CHUNK_BUFFER[chunk_id]
            result = json.loads(full_data.decode('utf-8'))
            print(f"[CHUNK] Reassembled {len(full_data)} bytes ({total_chunks} chunks) "
                  f"-> type={result.get('type')} id={chunk_id}")
            return result
        except KeyError as e:
            print(f"[CHUNK] Missing chunk number {e} in set {chunk_id} "
                  f"(have: {sorted(entry['chunks'].keys())})")
            del _CHUNK_BUFFER[chunk_id]
            return None
        except Exception as e:
            print(f"[CHUNK] Reassembly/parse error for {chunk_id}: {e}")
            del _CHUNK_BUFFER[chunk_id]
            return None

# =============================================================================
# UDP/TCP NETWORKING CODE - DISABLED
# =============================================================================
# All networking functionality is DISABLED by NETWORK_ENABLED = False (line 144)
# The code below is kept for reference but will NOT execute when NETWORK_ENABLED=False
# All UDP/TCP threads are prevented from starting
# All broadcast functions return early without sending data
# =============================================================================

# persistent queue (sqlite) table name

def ensure_udp_queue_table():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # create table with the full/canonical schema (id primary, target_ip, target_port, payload, retries, last_sent)
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {UDP_QUEUE_TABLE} (
            id TEXT PRIMARY KEY,
            target_ip TEXT,
            target_port INTEGER DEFAULT {UDP_PORT},
            payload TEXT,
            retries INTEGER DEFAULT 0,
            last_sent REAL DEFAULT 0
        )
        """)
        conn.commit()
        # defensive migration: add missing columns if older DB lacks them
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({UDP_QUEUE_TABLE})").fetchall()]
        if "target_port" not in cols:
            try:
                c.execute(f"ALTER TABLE {UDP_QUEUE_TABLE} ADD COLUMN target_port INTEGER DEFAULT {UDP_PORT}")
                conn.commit()
            except Exception:
                pass
        if "payload" not in cols:
            try:
                c.execute(f"ALTER TABLE {UDP_QUEUE_TABLE} ADD COLUMN payload TEXT DEFAULT ''")
                conn.commit()
            except Exception:
                pass
        if "retries" not in cols:
            try:
                c.execute(f"ALTER TABLE {UDP_QUEUE_TABLE} ADD COLUMN retries INTEGER DEFAULT 0")
                conn.commit()
            except Exception:
                pass
        if "last_sent" not in cols:
            try:
                c.execute(f"ALTER TABLE {UDP_QUEUE_TABLE} ADD COLUMN last_sent REAL DEFAULT 0")
                conn.commit()
            except Exception:
                pass
        conn.close()
    except Exception as e:
        print("[UDP] ensure table error:", e)

def get_self_ip():
    """Return best-effort LAN IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

def _make_send_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.settimeout(2.0)
    return s

def _make_listen_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(('', UDP_PORT))
    except Exception:
        # fallback bind to localhost
        s.bind(('0.0.0.0', UDP_PORT))
    s.settimeout(1.0)
    return s

def _row_to_dict(cursor, row):
    """Convert sqlite row (tuple) to dict using cursor.description"""
    if row is None:
        return None
    desc = [col[0] for col in (cursor.description or [])]
    return {k: row[i] for i, k in enumerate(desc)}
    
# ----------------------------
# Mesh selection helpers
# ----------------------------
def get_selected_mesh_devices():
    """Return saved mesh IP list from settings (excluding self IP)."""
    try:
        raw = get_setting("mesh_devices", "[]")
    except Exception:
        raw = "[]"
    try:
        ips = json.loads(raw)
    except Exception:
        ips = []
    # remove ourself if present
    self_ip = get_self_ip()
    ips = [ip for ip in ips if ip and ip != self_ip]
    return ips

def set_selected_mesh_devices(ip_list):
    """Persist a list of IPs (strings)."""
    try:
        # ensure we don't save self ip
        self_ip = get_self_ip()
        filtered = [ip for ip in (ip_list or []) if ip and ip != self_ip]
        set_setting("mesh_devices", json.dumps(filtered))
    except Exception as e:
        print("[mesh] save error:", e)


def _insert_queue_item(msg_id, target_ip, payload_json):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(f"INSERT OR REPLACE INTO {UDP_QUEUE_TABLE} (id, target_ip, payload, retries, last_sent) VALUES (?, ?, ?, coalesce((SELECT retries FROM {UDP_QUEUE_TABLE} WHERE id=?), 0), ?)",
                  (msg_id, target_ip or "", payload_json, msg_id, 0.0))
        conn.commit()
    except Exception as e:
        print("[UDP] insert queue error:", e)

def _delete_queue_item(msg_id):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(f"DELETE FROM {UDP_QUEUE_TABLE} WHERE id=?", (msg_id,))
        conn.commit()
    except Exception as e:
        print("[UDP] delete queue error:", e)

def send_reliable(payload_obj, targets=None, port=None):
    """
    Best-effort bulk send:
      - payload_obj: JSON-serializable dict
      - targets: list of IP strings; if None -> broadcast to BROADCAST_ADDR
      - port: if provided use this port (int); otherwise DEFAULT_UDP_PORT
    Behavior:
      - Try to send immediately via send_udp_json; if fails, queue per-target.
    """
    if not NETWORK_ENABLED:
        return False
    port = int(port) if port else DEFAULT_UDP_PORT
    # if targets is falsy, send a broadcast (single queue entry)
    if not targets or len(targets) == 0:
        ok = send_udp_json(BROADCAST_ADDR, port, payload_obj, use_broadcast=True)
        if ok:
            return True
        # fallback: queue a broadcast
        qid = queue_udp_message(BROADCAST_ADDR, port, payload_obj)
        return qid is not None

    # multiple targets: try direct send, queue if fails
    all_ok = True
    for ip in targets:
        try:
            ok = send_udp_json(ip, port, payload_obj, use_broadcast=False)
            if not ok:
                all_ok = False
                # queue for retry specifically to this target
                queue_udp_message(ip, port, payload_obj)
        except TypeError:
            # older / other definition of send_udp_json that lacks use_broadcast
            try:
                ok = send_udp_json(ip, port, payload_obj)
                if not ok:
                    all_ok = False
                    queue_udp_message(ip, port, payload_obj)
            except Exception as e:
                print("[send_reliable] send exception (fallback):", e)
                all_ok = False
                queue_udp_message(ip, port, payload_obj)
        except Exception as e:
            print("[send_reliable] unexpected send error:", e)
            all_ok = False
            queue_udp_message(ip, port, payload_obj)
    return all_ok
    
    
def _udp_queue_worker(stop_event=None):
    """Background worker that reads pending queue and sends, handles ACKs and retries.

    Uses send_udp_json for retries so that large payloads are automatically
    chunked instead of being sent as oversized raw UDP datagrams.
    """
    while True if stop_event is None else (not stop_event.is_set()):
        try:
            conn = get_db_connection()
            c = conn.cursor()
            rows = c.execute(f"SELECT id, target_ip, payload, retries, last_sent FROM {UDP_QUEUE_TABLE} ORDER BY last_sent ASC LIMIT 50").fetchall()
            now = time.time()
            for row in rows:
                msg_id, target_ip, payload_json, retries, last_sent = row
                if now - (last_sent or 0) < (1.0 + (retries or 0) * 0.5):
                    continue
                if (retries or 0) >= 20:
                    print(f"[UDP] dropping msg {msg_id} after {retries} retries")
                    _delete_queue_item(msg_id)
                    continue
                try:
                    addr = target_ip or BROADCAST_ADDR
                    is_broadcast = (addr == BROADCAST_ADDR or addr.endswith('.255'))
                    # Parse stored JSON and re-send via send_udp_json which
                    # handles chunking for large payloads automatically
                    payload_obj = json.loads(payload_json) if payload_json else {}
                    ok = send_udp_json(addr, UDP_PORT, payload_obj, use_broadcast=is_broadcast)
                    if ok:
                        _delete_queue_item(msg_id)
                    else:
                        # Update retry count
                        try:
                            conn2 = get_db_connection()
                            c2 = conn2.cursor()
                            c2.execute(
                                f"UPDATE {UDP_QUEUE_TABLE} SET retries=coalesce(retries,0)+1, last_sent=? WHERE id=?",
                                (time.time(), msg_id)
                            )
                            conn2.commit()
                        except Exception:
                            pass
                except Exception as e:
                    print(f"[UDP queue] send error for {msg_id}: {e}")
                    try:
                        conn2 = get_db_connection()
                        c2 = conn2.cursor()
                        c2.execute(
                            f"UPDATE {UDP_QUEUE_TABLE} SET retries=coalesce(retries,0)+1, last_sent=? WHERE id=?",
                            (time.time(), msg_id)
                        )
                        conn2.commit()
                    except Exception:
                        pass
                time.sleep(0.05)
            # purge acknowledged messages
            with _ACKED_MSGS_LOCK:
                acked = list(_ACKED_MSGS)
            for ack_id in acked:
                _delete_queue_item(ack_id)
                with _ACKED_MSGS_LOCK:
                    if ack_id in _ACKED_MSGS:
                        _ACKED_MSGS.remove(ack_id)
            # wait briefly, but allow stop_event to short-circuit
            if stop_event is None:
                time.sleep(0.8)
            else:
                stop_event.wait(0.8)
        except Exception as e:
            print("[UDP queue worker] error:", e)
            if stop_event is None:
                time.sleep(1.0)
            else:
                stop_event.wait(1.0)


def _udp_listener_worker(stop_event=None):
    """Background listener: accept messages, update KNOWN_DEVICES, send ACKs, and dispatch payloads.

    Chunked messages are reassembled here at the listener level before dispatch,
    so apply_incoming_payload always receives complete payloads.
    """
    s = _make_listen_socket()
    while True if stop_event is None else (not stop_event.is_set()):
        try:
            data, addr = s.recvfrom(131072)
            ip = addr[0]
            now = time.time()
            try:
                obj = json.loads(data.decode('utf-8', 'ignore'))
            except Exception:
                continue
            with KNOWN_DEVICES_LOCK:
                KNOWN_DEVICES[ip] = {"ip": ip, "last_seen": now, "info": obj.get("info")}

            # Update device sync manager if enabled
            if device_sync_manager and obj.get("info"):
                device_sync_manager.update_device(ip, obj.get("info"))
            if obj.get("type") == "udp_ack" and obj.get("ack_id"):
                ack_id = obj.get("ack_id")
                with _ACKED_MSGS_LOCK:
                    _ACKED_MSGS.add(ack_id)
                continue
            try:
                ack = {"type": "udp_ack", "ack_id": obj.get("_msg_id"), "from": get_self_ip()}
                s.sendto(json.dumps(ack, separators=(',', ':')).encode('utf-8'), (ip, UDP_PORT))
            except Exception:
                pass

            # ── Chunked message reassembly at listener level ─────────────
            if obj.get("type") == "chunked_message":
                # Check self-origin on the chunk wrapper
                chunk_from = obj.get("_from") or obj.get("from") or ""
                try:
                    if chunk_from and str(chunk_from) == str(get_self_ip()):
                        continue
                except Exception:
                    pass

                complete_msg = _reassemble_chunked_message(obj)
                if complete_msg is None:
                    # Not all chunks received yet – wait for more
                    continue
                # All chunks received – dispatch the reassembled message
                try:
                    threading.Thread(
                        target=apply_incoming_payload,
                        args=(complete_msg,),
                        daemon=True
                    ).start()
                except Exception:
                    pass
                continue
            # ── End chunked message handling ──────────────────────────────

            if obj.get("_from") == get_self_ip():
                continue
            try:
                threading.Thread(target=apply_incoming_payload, args=(obj,), daemon=True).start()
            except Exception:
                pass
        except socket.timeout:
            continue
        except Exception as e:
            print("[UDP listener] error:", e)
            if stop_event is None:
                time.sleep(0.5)
            else:
                stop_event.wait(0.5)
    try:
        s.close()
    except Exception:
        pass
       
def handle_incoming_mesh_payload(payload: dict, ip: str):
    # Implement your application logic: e.g., if payload['type']=='user_upsert', apply to local DB
    try:
        typ = payload.get("type")
        if typ == "user_upsert":
            # sample: call your existing handler to upsert user
            print(f"[MESH] user_upsert from {ip}")
            # call existing function: app_user_upsert(payload['data'])
        elif typ == "user_delete":
            print(f"[MESH] user_delete from {ip}")
        # add more handlers as needed
    except Exception as e:
        print("[MESH] handle incoming error:", e)

UDP_STOP_EVENT = Event()
UDP_QUEUE_THREAD = None
UDP_LISTENER_THREAD = None

# Only start UDP threads if networking is enabled
if NETWORK_ENABLED:
    # Initialize UDP queue table first
    print("[UDP] Initializing UDP queue table...")
    ensure_udp_queue_table()
    print("[UDP] UDP queue table ready")

    # Initialize Device Sync Manager
    init_device_sync_manager()

    UDP_QUEUE_THREAD = threading.Thread(target=_udp_queue_worker, args=(UDP_STOP_EVENT, ), daemon=True)
    UDP_LISTENER_THREAD = threading.Thread(target=_udp_listener_worker, args=(UDP_STOP_EVENT, ), daemon=True)
    UDP_QUEUE_THREAD.start()
    UDP_LISTENER_THREAD.start()
    print("[UDP] UDP networking threads started")
else:
    print("[UDP] UDP networking DISABLED - threads not started")


def apply_incoming_payload(obj):
    """
    Apply a received mesh payload to the local DB.
    Supports:
      - type == "user_upsert"  -> small metadata upsert (emp_id, name, template_id)
      - type == "face_edit"    -> full face update (base64 encoding + display_image)
    Defensive: parameterized SQL, correct column names (name not emp_name),
    no rebroadcasting, and helpful logs.
    """
    try:
        if not obj or not isinstance(obj, dict):
            return False

        # Basic fields
        msg_type = obj.get("type")
        msg_from = obj.get("from")
        msg_id = obj.get("msg_id") or obj.get("id") or None

        # Filter out UDP ACK messages - they should not reach here
        if msg_type == "udp_ack":
            return False

        # avoid applying our own broadcasts (if get_self_ip is available)
        try:
            self_ip = get_self_ip() if callable(get_self_ip) else None
            if self_ip and msg_from and str(msg_from) == str(self_ip):
                # ignore messages originated by self
                return False
        except Exception:
            # if get_self_ip fails, continue and process normally
            pass

        # Connect DB
        conn = get_db_connection()
        c = conn.cursor()

        if msg_type == "user_upsert":
            u = obj.get("user") or {}
            emp_id = (u.get("emp_id") or u.get("id") or "").strip()
            if not emp_id:
                print("[MESH] apply incoming payload error: user_upsert missing emp_id")
                conn.close()
                return False

            # sanitize / extract fields we actually have in schema
            name = u.get("name") or u.get("emp_name") or None
            template_id = u.get("template_id")
            # Build upsert that matches your schema (emp_id primary key, column name is 'name')
            # Note: excluded.<col> is the right form for SQLite ON CONFLICT DO UPDATE
            # Only update columns provided (avoid overwriting with None)
            if name is not None and template_id is not None:
                c.execute("""
                    INSERT INTO users (emp_id, name, template_id)
                    VALUES (?, ?, ?)
                    ON CONFLICT(emp_id) DO UPDATE SET
                      name = excluded.name,
                      template_id = excluded.template_id
                """, (emp_id, name, template_id))
            elif name is not None:
                c.execute("""
                    INSERT INTO users (emp_id, name) VALUES (?, ?)
                    ON CONFLICT(emp_id) DO UPDATE SET name = excluded.name
                """, (emp_id, name))
            elif template_id is not None:
                c.execute("""
                    INSERT INTO users (emp_id, template_id) VALUES (?, ?)
                    ON CONFLICT(emp_id) DO UPDATE SET template_id = excluded.template_id
                """, (emp_id, template_id))
            else:
                # ensure row exists
                c.execute("INSERT OR IGNORE INTO users (emp_id) VALUES (?)", (emp_id,))

            conn.commit()
            conn.close()
            print(f"[MESH] applied user_upsert for emp_id={emp_id} from {msg_from}")
            return True

        elif msg_type == "face_edit":
            u = obj.get("user") or {}
            emp_id = (u.get("emp_id") or u.get("id") or "").strip()
            if not emp_id:
                print("[MESH] apply incoming payload error: face_edit missing emp_id")
                conn.close()
                return False

            # Get base64 fields (may be empty strings)
            enc_b64 = u.get("encoding_b64") or u.get("encoding") or ""
            disp_b64 = u.get("display_image_b64") or u.get("display_image") or ""

            enc_bytes = None
            disp_bytes = None
            if enc_b64:
                    try:
                        enc_bytes = base64.b64decode(enc_b64)
                    except Exception as e:
                        print("[MESH] apply incoming payload warning: failed to decode encoding_b64:", e)

            if disp_b64:
                    try:
                        disp_bytes = base64.b64decode(disp_b64)
                    except Exception as e:
                        print("[MESH] apply incoming payload warning: failed to decode display_image_b64:", e)


            # Save to file system and get paths
            enc_path = save_encoding(emp_id, enc_bytes) if enc_bytes else None
            img_path = save_image(emp_id, disp_bytes) if disp_bytes else None

            # Upsert row and update file paths 
            # We'll ensure row exists first
            c.execute("INSERT OR IGNORE INTO users (emp_id) VALUES (?)", (emp_id,))

            updates = []
            params = []
            if enc_path is not None:
                updates.append("encoding_path = ?")
                params.append(enc_path)
            if img_path is not None:
                updates.append("image_path = ?")
                params.append(img_path)
            # optionally update name if provided
            name = u.get("name")
            if name is not None:
                updates.append("name = ?")
                params.append(name)

            if updates:
                params.append(emp_id)
                sql = f"UPDATE users SET {', '.join(updates)} WHERE emp_id = ?"
                c.execute(sql, tuple(params))

            conn.commit()
            conn.close()

            # Reload face recognizer so this device can immediately recognize
            # the newly received face encoding
            if enc_bytes:
                try:
                    recognizer.load_all_encodings()
                    print(f"[MESH] face_edit: reloaded face recognizer for emp_id={emp_id}")
                except Exception as e:
                    print(f"[MESH] face_edit: recognizer reload error: {e}")

            print(f"[MESH] applied face_edit for emp_id={emp_id} from {msg_from} "
                  f"(enc={'yes' if enc_bytes else 'no'}, img={'yes' if disp_bytes else 'no'})")
            return True

        elif msg_type == "template_transfer":
            # Handle template transfer from device sync manager
            # device_sync_manager handles face/image/user DB,
            # then we inject fingerprint into sensor via receive_fingerprint_template()
            conn.close()
            if device_sync_manager:
                success = device_sync_manager.handle_template_transfer(obj)
                fp_log.info(f"[MESH] template_transfer DB/files: emp_id={obj.get('emp_id')} "
                            f"success={success}")

                # Now inject fingerprint into sensor (device_sync_manager doesn't do this)
                fp_b64 = obj.get("fingerprint")
                tt_emp_id = (obj.get("emp_id") or "").strip()
                tt_username = (obj.get("user_data") or {}).get("name", "")
                if fp_b64 and tt_emp_id:
                    try:
                        fp_bytes = base64.b64decode(fp_b64)
                        def _bg_tt_inject(eid=tt_emp_id, fb=fp_bytes, un=tt_username):
                            ok, tid, msg = receive_fingerprint_template(eid, fb, un)
                            print(f"[MESH] template_transfer sensor inject: "
                                  f"emp_id={eid} ok={ok} slot={tid} msg={msg}")
                        threading.Thread(target=_bg_tt_inject, daemon=True).start()
                    except Exception as e:
                        fp_log.error(f"[MESH] template_transfer sensor inject error: {e}")

                return success
            else:
                print("[MESH] template_transfer received but device_sync_manager not initialized")
                return False

        elif msg_type == "database_sync":
            # Handle database sync from device sync manager
            conn.close()  # Close the connection, sync manager will handle its own
            if device_sync_manager:
                success = device_sync_manager.handle_database_sync(obj)
                print(f"[MESH] database_sync: {'success' if success else 'failed'}")
                return success
            else:
                print("[MESH] database_sync received but device_sync_manager not initialized")
                return False

        elif msg_type == "request_user_list":
            # Handle request for list of existing users (for incremental sync)
            conn.close()
            response_port = obj.get("response_port")
            sender_ip = msg_from

            if response_port and sender_ip:
                try:
                    # Get list of all user emp_ids from database
                    conn_db = get_db_conn()
                    cursor = conn_db.cursor()
                    cursor.execute("SELECT emp_id FROM users")
                    rows = cursor.fetchall()
                    conn_db.close()

                    emp_ids = [row[0] for row in rows]

                    # Send response back
                    response = {
                        "type": "user_list_response",
                        "emp_ids": emp_ids,
                        "timestamp": time.time()
                    }

                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.sendto(json.dumps(response).encode('utf-8'), (sender_ip, response_port))
                    sock.close()

                    print(f"[MESH] Sent user list to {sender_ip}: {len(emp_ids)} users")
                    return True
                except Exception as e:
                    print(f"[MESH] Error handling request_user_list: {e}")
                    return False
            return False

        elif msg_type == "mesh_sync":
            # Handle mesh network synchronization
            conn.close()  # Close the connection, sync manager will handle its own
            if device_sync_manager:
                success = device_sync_manager.handle_mesh_sync(obj)
                mesh_devices = obj.get('mesh_devices', [])
                print(f"[MESH] mesh_sync: {'success' if success else 'failed'}, devices: {len(mesh_devices)}")
                return success
            else:
                print("[MESH] mesh_sync received but device_sync_manager not initialized")
                return False

        elif msg_type == "chunked_message":
            # Fallback: chunks are normally reassembled at the listener level,
            # but handle here too in case one slips through.
            conn.close()
            complete_message = _reassemble_chunked_message(obj)
            if complete_message:
                print(f"[MESH] Reassembled chunked message (fallback), type: {complete_message.get('type')}")
                return apply_incoming_payload(complete_message)
            return False

        elif msg_type == "ping":
            # Handle ping message - respond with pong
            conn.close()
            if device_sync_manager:
                device_sync_manager.handle_ping(msg_from)
                return True
            return False

        elif msg_type == "pong":
            # Handle pong message - update health tracking
            conn.close()
            if device_sync_manager:
                device_sync_manager.handle_pong(msg_from)
                return True
            return False

        elif msg_type == "connection_request":
            # Handle connection request - acknowledge
            conn.close()
            print(f"[MESH] Connection request from {msg_from}")
            return True

        elif msg_type == "finger_edit":
            # ── Fingerprint template sync from peer device ────────────────
            # Uses receive_fingerprint_template() which handles:
            #   skip-if-exists, lowest-free-slot, save files, DB update,
            #   sensor injection via global sensor + sensor_lock.
            conn.close()
            try:
                fp_emp_id   = (obj.get("emp_id") or "").strip()
                fp_username = obj.get("username", "")
                fp_b64      = obj.get("template", "")

                if not fp_emp_id or not fp_b64:
                    fp_log.error(f"[MESH] finger_edit: missing emp_id or template")
                    return False

                # Decode base64
                try:
                    fp_bytes = base64.b64decode(fp_b64)
                except Exception as e:
                    fp_log.error(f"[MESH] finger_edit: base64 decode error: {e}")
                    return False

                fp_log.info(f"[MESH] finger_edit received: emp_id={fp_emp_id} "
                            f"size={len(fp_bytes)} from={msg_from}")

                # Run sensor injection in background to not block the listener
                def _bg_receive(eid=fp_emp_id, fb=fp_bytes, un=fp_username):
                    ok, tid, msg = receive_fingerprint_template(eid, fb, un)
                    print(f"[MESH] finger_edit result: emp_id={eid} ok={ok} "
                          f"slot={tid} msg={msg}")

                threading.Thread(target=_bg_receive, daemon=True).start()
                return True

            except Exception as e:
                fp_log.error(f"[MESH] finger_edit error: {e}", exc_info=True)
                return False
            
        elif msg_type == "rfid_edit":
            # ── RFID card sync from peer device ───────────────────────────
            # Receives emp_id + rfid_card (10-digit UID string)
            # Inserts into local rfid_card_map — never overwrites existing card
            conn.close()
            try:
                rfid_emp_id  = (obj.get("emp_id")    or "").strip()
                rfid_card    = (obj.get("rfid_card")  or "").strip()
                rfid_name    = (obj.get("name")       or "").strip()

                if not rfid_emp_id or not rfid_card:
                    print("[MESH] rfid_edit: missing emp_id or rfid_card")
                    return False

                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                conn_main = get_db_connection()
                try:
                    # Check if this emp_id already has a card registered locally
                    existing = conn_main.execute(
                        "SELECT rfid_card FROM rfid_card_map WHERE emp_id=?",
                        (rfid_emp_id,)
                    ).fetchone()

                    if existing:
                        # emp_id already has a card — do NOT overwrite
                        # (same policy as import sync: INSERT OR IGNORE)
                        print(f"[MESH] rfid_edit: {rfid_emp_id} already has "
                              f"card {existing['rfid_card']} locally — skipping")
                        return True

                    # Check if the card UID is already assigned to someone else
                    conflict = conn_main.execute(
                        "SELECT emp_id FROM rfid_card_map WHERE rfid_card=?",
                        (rfid_card,)
                    ).fetchone()

                    if conflict and conflict["emp_id"] != rfid_emp_id:
                        print(f"[MESH] rfid_edit: card {rfid_card} already "
                              f"assigned to {conflict['emp_id']} — skipping")
                        return False

                    # Safe to insert
                    conn_main.execute(
                        """INSERT OR IGNORE INTO rfid_card_map
                           (emp_id, rfid_card, name, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        (rfid_emp_id, rfid_card, rfid_name, now, now)
                    )
                    conn_main.commit()
                    print(f"[MESH] rfid_edit: saved card={rfid_card} "
                          f"for emp_id={rfid_emp_id}")
                    return True

                except Exception as e:
                    print(f"[MESH] rfid_edit DB error: {e}")
                    return False
                finally:
                    conn_main.close()

            except Exception as e:
                print(f"[MESH] rfid_edit error: {e}")
                return False

        else:
            # Unknown payload type - ignore but log
            conn.close()
            print("[MESH] apply incoming payload: unknown type", msg_type)
            return False

    except Exception as exc:
        # catch-all logging for debugging
        try:
            conn.close()
        except Exception:
            pass
        print("[MESH] apply incoming payload error:", exc)
        return False

# Public API for legacy callers in app:
def load_mesh_state():
    """Return currently known devices from listener snapshot."""
    with KNOWN_DEVICES_LOCK:
        return {ip: {"ip": info["ip"], "last_seen": info["last_seen"], "info": info.get("info")} for ip, info in KNOWN_DEVICES.items()}

def send_udp_json(target_ip, port, payload, use_broadcast: bool = False):
    """
    Canonical UDP send helper with automatic chunking for large payloads.
    - target_ip: destination IP or broadcast address string. If None/empty, falls back to broadcast.
    - port: destination UDP port (int).
    - payload: JSON-serializable object (dict/list/...).
    - use_broadcast: when True sets SO_BROADCAST on socket.
    Returns True if send succeeded, False on exception.

    Large payloads (>60KB) are automatically split into numbered chunks and
    reassembled by the receiver.  Each chunk is <=60KB of raw bytes after the
    JSON+base64 wrapper, keeping the on-wire UDP datagram well under 65535.
    """
    if not NETWORK_ENABLED:
        return False

    # default fallback: if no target provided, broadcast to BROADCAST_ADDR
    if not target_ip:
        target_ip = BROADCAST_ADDR

    try:
        data = json.dumps(payload, separators=(',', ':')).encode("utf-8")
        data_len = len(data)

        # --- Small payload: send directly (single UDP datagram) ---
        # Use 60000 as threshold to stay safely under 65535 UDP max
        MAX_SINGLE_PACKET = 60000
        if data_len <= MAX_SINGLE_PACKET:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(2.0)
            if use_broadcast:
                try:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                except Exception:
                    pass
            s.sendto(data, (str(target_ip), int(port)))
            s.close()
            return True

        # --- Large payload: chunk and send ---
        # Each chunk's raw data portion must fit inside a wrapper JSON that
        # itself stays under MAX_SINGLE_PACKET.  The wrapper overhead is
        # ~250 bytes of JSON keys + base64 expansion (4/3x).
        # So raw chunk size = (MAX_SINGLE_PACKET - 300) * 3/4  ≈ 44775
        CHUNK_RAW_SIZE = 44000  # conservative; leaves room for wrapper
        chunk_id = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        total_chunks = (data_len + CHUNK_RAW_SIZE - 1) // CHUNK_RAW_SIZE

        print(f"[UDP-CHUNK] Sending {data_len} bytes to {target_ip} in {total_chunks} chunks (id={chunk_id})")

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(2.0)
        if use_broadcast:
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            except Exception:
                pass

        for i in range(total_chunks):
            start = i * CHUNK_RAW_SIZE
            end = min(start + CHUNK_RAW_SIZE, data_len)
            chunk_bytes = data[start:end]

            chunk_envelope = {
                "type": "chunked_message",
                "chunk_id": chunk_id,
                "chunk_num": i,
                "total_chunks": total_chunks,
                "data": base64.b64encode(chunk_bytes).decode('ascii'),
                "_from": payload.get("from") or payload.get("_from") or get_self_ip(),
            }
            chunk_data = json.dumps(chunk_envelope, separators=(',', ':')).encode('utf-8')
            s.sendto(chunk_data, (str(target_ip), int(port)))
            # Small delay between chunks to avoid overwhelming the receiver
            # and to reduce UDP packet loss on busy networks
            time.sleep(0.015)

        s.close()
        print(f"[UDP-CHUNK] Sent all {total_chunks} chunks to {target_ip} (id={chunk_id})")
        return True

    except Exception as e:
        try:
            current_app.logger.debug(f"[send_udp_json] error sending to {target_ip}:{port} -> {e}")
        except Exception:
            print(f"[send_udp_json] error sending to {target_ip}:{port} -> {e}")
        try:
            s.close()
        except Exception:
            pass
        return False

def queue_udp_message(target_ip, target_port, payload_obj, msg_id=None):
    """
    Adds (or updates) an entry into udp_queue.
    - target_ip: str or None (None will be stored as NULL)
    - target_port: int or convertible numeric; will default to DEFAULT_UDP_PORT
    - payload_obj: JSON-serializable object (we will json.dumps it)
    - msg_id: optional string id; if not provided we generate uuid4 hex
    Returns: msg_id on success, None on failure
    """
    try:
        # normalize
        if not msg_id:
            msg_id = str(uuid.uuid4())
        if target_port is None or target_port == "":
            target_port = DEFAULT_UDP_PORT
        try:
            target_port = int(target_port)
        except Exception:
            target_port = DEFAULT_UDP_PORT

        # serialize payload to JSON string (safe)
        payload_json = json.dumps(payload_obj, separators=(',', ':'), ensure_ascii=False)

        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            f"INSERT OR REPLACE INTO {UDP_QUEUE_TABLE} (id, target_ip, target_port, payload, retries, last_sent) VALUES (?, ?, ?, ?, ?, ?)",
            (msg_id, target_ip, target_port, payload_json, 0, 0.0)
        )
        conn.commit()
        # debug log
        print(f"[UDP] queued msg {msg_id} -> {target_ip}:{target_port}")
        return msg_id
    except sqlite3.IntegrityError as e:
        print("[UDP] queue insert integrity error:", e)
        return None
    except sqlite3.OperationalError as e:
        print("[UDP] queue insert operational error:", e)
        return None
    except Exception as e:
        print("[UDP] queue insert error:", e)
        return None

def broadcast_login_udp(emp_id, name, medium):
    if not NETWORK_ENABLED:
        return False
    try:
        p = {"type":"user_login","emp_id":emp_id,"name":name,"medium":medium,"ts":time.time()}
        return send_reliable(p, targets=None)
    except Exception:
        return False

def broadcast_login_tcp(emp_id, name, medium):
    # TCP not used; reuse UDP reliable
    return broadcast_login_udp(emp_id, name, medium)

@app.route("/api/devices", methods=["GET"])
def api_devices():
    if not NETWORK_ENABLED:
        return jsonify({"devices": []})
    with KNOWN_DEVICES_LOCK:
        out = []
        for ip, info in KNOWN_DEVICES.items():
            meta = info.get("info") or {}
            out.append({
                "ip": ip,
                "device_id": meta.get("device_id"),
                "name": meta.get("name"),
                "last_seen": info.get("last_seen"),
            })
    return jsonify({"devices": out})

@app.route("/api/udp_status", methods=["GET"])
def api_udp_status():
    if not NETWORK_ENABLED:
        return jsonify({"devices": [], "udp_queue_count": 0})
    with KNOWN_DEVICES_LOCK:
        devices = [{"ip": ip, "last_seen": info["last_seen"], "info": info.get("info")} for ip, info in KNOWN_DEVICES.items()]
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(f"SELECT count(*) FROM {UDP_QUEUE_TABLE}")
        qcount = c.fetchone()[0]
    except Exception:
        qcount = None
    return jsonify({"devices": devices, "udp_queue_count": qcount})


@app.route('/api/discover_now', methods=['GET','POST'])
def api_discover_now():
    if not NETWORK_ENABLED:
        return jsonify({"success": False, "message": "Networking disabled"})
    # existing behavior: broadcast discovery
    payload = {
        "type": "discover",
        "from": get_self_ip(),
        "info": {
            "name": os.uname().nodename,
            "device_id": get_device_id(),   # <-- CANT_A_001 style ID
            "ip": get_self_ip(),            # <-- also include sender IP in info
        },
    }
    try:
        send_udp_json(BROADCAST_ADDR, UDP_PORT, payload, use_broadcast=True)
    except Exception:
        queue_udp_message(BROADCAST_ADDR, UDP_PORT, payload)
    # return the current known devices (so client gets immediate state)
    with KNOWN_DEVICES_LOCK:
        devices = {k: v for k, v in KNOWN_DEVICES.items()}
    return jsonify({"success": True, "devices": devices})


@app.route("/api/save_mesh_devices", methods=["POST"])
def api_save_mesh_devices():
    if not NETWORK_ENABLED:
        return jsonify({"success": True, "saved": []})
    data = request.get_json(force=True) or {}
    ips = data.get("devices") or []
    try:
        # remove empties and ourself
        self_ip = get_self_ip()
        cleaned = [ip for ip in ips if ip and ip != self_ip]
        # persist to database
        set_selected_mesh_devices(cleaned)

        # Load saved devices into sync manager for auto-reconnection
        if cleaned and device_sync_manager:
            try:
                print(f"[MESH] Loading {len(cleaned)} devices into sync manager for auto-reconnection")
                device_sync_manager.load_saved_devices(cleaned)
            except Exception as e:
                print(f"[MESH] Error loading devices into sync manager: {e}")

            # Sync mesh connections - broadcast full mesh topology to all devices
            try:
                # Include self in the mesh list for broadcasting
                full_mesh = cleaned + [self_ip]
                results = device_sync_manager.sync_mesh_connections(full_mesh)
                success_count = sum(1 for v in results.values() if v)
                print(f"[MESH] Mesh sync: {success_count}/{len(results)} devices synchronized")
            except Exception as e:
                print(f"[MESH] Mesh sync error: {e}")

        # notify peers of mesh update (so they can optionally save/ack or at least mark online)
        if cleaned:
            try:
                payload = {
                    "type": "mesh_update",
                    "from": self_ip,
                    "members": cleaned,
                    "ts": time.time()
                }
                # use send_reliable targets=cleaned so each selected peer receives the update
                send_reliable(payload, targets=cleaned)
            except Exception as e:
                print(f"[MESH] Notify error: {e}")

        # return saved + current discovered device info
        return jsonify({"success": True, "saved": cleaned})
    except Exception as e:
        print(f"[MESH] Save endpoint error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/saved_mesh", methods=["GET"])
def api_saved_mesh():
    if not NETWORK_ENABLED:
        return jsonify({"success": True, "saved": []})
    try:
        ips = get_selected_mesh_devices()
        return jsonify({"success": True, "saved": ips})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# -----------------------------------------------------------------------------
# Device Sync Manager API Endpoints
# -----------------------------------------------------------------------------

@app.route("/api/validate_device_id", methods=["POST"])
def api_validate_device_id():
    """Validate that device ID is unique across the network"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"valid": True})  # If networking disabled, assume valid

    data = request.get_json() or {}
    device_id = data.get("device_id", "").strip()
    current_ip = data.get("current_ip")

    if not device_id:
        return jsonify({"valid": False, "error": "Device ID is required"}), 400

    is_valid, error_msg = device_sync_manager.validate_device_id(device_id, current_ip)
    return jsonify({"valid": is_valid, "error": error_msg})


@app.route("/api/validate_device_name", methods=["POST"])
def api_validate_device_name():
    """Validate that device name is unique across the network"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"valid": True})  # If networking disabled, assume valid

    data = request.get_json() or {}
    device_name = data.get("device_name", "").strip()
    current_ip = data.get("current_ip")

    if not device_name:
        return jsonify({"valid": False, "error": "Device name is required"}), 400

    is_valid, error_msg = device_sync_manager.validate_device_name(device_name, current_ip)
    return jsonify({"valid": is_valid, "error": error_msg})


@app.route("/api/connect_device", methods=["POST"])
def api_connect_device():
    """Connect to a discovered device"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"success": False, "error": "Networking is disabled"}), 400

    data = request.get_json() or {}
    ip = data.get("ip", "").strip()

    if not ip:
        return jsonify({"success": False, "error": "IP address is required"}), 400

    success, error_msg = device_sync_manager.connect_device(ip)
    if success:
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "error": error_msg}), 400


@app.route("/api/disconnect_device", methods=["POST"])
def api_disconnect_device():
    """Disconnect from a device"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"success": False, "error": "Networking is disabled"}), 400

    data = request.get_json() or {}
    ip = data.get("ip", "").strip()

    if not ip:
        return jsonify({"success": False, "error": "IP address is required"}), 400

    device_sync_manager.disconnect_device(ip)
    return jsonify({"success": True})


@app.route("/api/connected_devices", methods=["GET"])
def api_connected_devices():
    """Get list of connected devices"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"devices": []})

    devices = device_sync_manager.get_connected_devices()
    return jsonify({"devices": devices})


@app.route("/api/online_devices", methods=["GET"])
def api_online_devices():
    """Get list of online devices (seen recently)"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"devices": []})

    timeout = request.args.get("timeout", 30, type=int)
    devices = device_sync_manager.get_online_devices(timeout_seconds=timeout)
    return jsonify({"devices": devices})


@app.route("/api/transfer_template", methods=["POST"])
def api_transfer_template():
    """Transfer user template to connected devices"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"success": False, "error": "Networking is disabled"}), 400

    data = request.get_json() or {}
    emp_id = data.get("emp_id", "").strip()
    target_ips = data.get("target_ips")  # Optional, defaults to all connected

    if not emp_id:
        return jsonify({"success": False, "error": "emp_id is required"}), 400

    results = device_sync_manager.transfer_user_template(emp_id, target_ips)

    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)

    return jsonify({
        "success": success_count > 0,
        "results": results,
        "success_count": success_count,
        "total_count": total_count
    })


@app.route("/api/pull_face_template/<emp_id>", methods=["GET"])
def api_pull_face_template(emp_id):
    """
    HTTP endpoint for peer devices to pull face template data.
    Returns face encoding, user image, and user metadata as JSON.
    This serves as a reliable fallback when UDP chunked transfer fails.
    """
    emp_id = (emp_id or "").strip()
    if not emp_id:
        return jsonify({"success": False, "error": "emp_id is required"}), 400

    try:
        # Get user data
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT emp_id, name, role, birthdate,
                   encoding_path, image_path, created_at, updated_at
            FROM users WHERE emp_id = ?
        """, (emp_id,))
        row = c.fetchone()
        conn.close()

        if not row:
            return jsonify({"success": False, "error": f"User {emp_id} not found"}), 404

        user_data = {
            "emp_id": row["emp_id"], "name": row["name"],
            "role": row["role"], "birthdate": row["birthdate"],
            "encoding_path": row["encoding_path"], "image_path": row["image_path"],
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        }

        # Load face encoding
        enc_bytes = load_encoding(emp_id)
        enc_b64 = base64.b64encode(enc_bytes).decode() if enc_bytes else None

        # Load and compress user image
        img_bytes = load_image(emp_id)
        if img_bytes:
            img_bytes = _compress_image_for_transfer(img_bytes, max_kb=120)
        img_b64 = base64.b64encode(img_bytes).decode() if img_bytes else None

        return jsonify({
            "success": True,
            "emp_id": emp_id,
            "user_data": user_data,
            "face_encoding": enc_b64,
            "user_image": img_b64,
        })
    except Exception as e:
        print(f"[API] pull_face_template error for {emp_id}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/sync_database", methods=["POST"])
def api_sync_database():
    """Synchronize database to connected devices"""
    if not NETWORK_ENABLED or not device_sync_manager:
        return jsonify({"success": False, "error": "Networking is disabled"}), 400

    data = request.get_json() or {}
    target_ips = data.get("target_ips")  # Optional, defaults to all connected

    results = device_sync_manager.sync_database(target_ips)

    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)

    return jsonify({
        "success": success_count > 0,
        "results": results,
        "success_count": success_count,
        "total_count": total_count
    })


# -----------------------------------------------------------------------------
# Template-ID API (auto-incremental & reserved per emp_id)
# -----------------------------------------------------------------------------
@app.route("/api/user_template_id")
def api_user_template_id():
    emp_id = (request.args.get("emp_id") or "").strip()
    if not emp_id:
        return jsonify({"success": False, "message": "emp_id missing"}), 400
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS user_template_seq (emp_id TEXT PRIMARY KEY, seq INTEGER)")
        conn.commit()
        c.execute("SELECT seq FROM user_template_seq WHERE emp_id=?", (emp_id,))
        row = c.fetchone()
        if row is None:
            seq = 1
            c.execute("INSERT INTO user_template_seq (emp_id, seq) VALUES (?,?)", (emp_id, seq))
        else:
            seq = row[0] + 1
            c.execute("UPDATE user_template_seq SET seq=? WHERE emp_id=?", (seq, emp_id))
        conn.commit()
        return jsonify({"success": True, "template_id": seq})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/modes")
@require_page_permission("modes") 
def modes_page():
    return render_template("modes.html")

def _bool_from_setting(val, default=True):
    if val is None:
        return bool(default)
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "on")

@app.route("/api/modes_get")
def modes_get():
    data = {
        "conn_wifi":     _bool_from_setting(get_setting("conn_wifi", "1"),     True),
        "conn_mesh":     _bool_from_setting(get_setting("conn_mesh", "1"),     True),
        "auth_face":     _bool_from_setting(get_setting("auth_face", "1"),     True),
        "auth_finger":   _bool_from_setting(get_setting("auth_finger", "1"),   True),
        "auth_rfid":     _bool_from_setting(get_setting("auth_rfid", "1"),     True),
        "conn_postgres": _bool_from_setting(get_setting("conn_postgres", "1"), True),
    }
    return jsonify(data)

@app.route("/api/modes_set", methods=["POST"])
def modes_set():
    payload = request.get_json(force=True) or {}
    def to_store_bool(v, default=True):
        if isinstance(v, bool):
            return "1" if v else "0"
        if v is None:
            return "1" if default else "0"
        s = str(v).strip().lower()
        truthy = s in ("1", "true", "yes", "on")
        falsy  = s in ("0", "false", "no", "off")
        if truthy:
            return "1"
        if falsy:
            return "0"
        return "1" if default else "0"

    set_setting("conn_wifi",     to_store_bool(payload.get("conn_wifi"),     True))
    set_setting("conn_mesh",     to_store_bool(payload.get("conn_mesh"),     True))
    set_setting("auth_face",     to_store_bool(payload.get("auth_face"),     True))
    set_setting("auth_finger",   to_store_bool(payload.get("auth_finger"),   True))
    set_setting("auth_rfid",     to_store_bool(payload.get("auth_rfid"),     True))
    set_setting("conn_postgres", to_store_bool(payload.get("conn_postgres"), True))
    return jsonify({"success": True})

def get_user_role(emp_id: str) -> str:
    try:
        conn = get_db_connection()
        row = conn.execute("SELECT role FROM users WHERE emp_id=?", (str(emp_id),)).fetchone()
        conn.close()
        if row and row["role"]:
            return row["role"]
    except Exception:
        pass
    return "User"


# -----------------------------------------------------------------------------
# Admin password helpers
# -----------------------------------------------------------------------------
def get_admin_password():
    if not os.path.exists(ADMIN_PW_FILE):
        with open(ADMIN_PW_FILE, "w") as f:
            f.write(hashlib.sha256("admin".encode()).hexdigest())
        return hashlib.sha256("admin".encode()).hexdigest()
    with open(ADMIN_PW_FILE, "r") as f:
        return f.read().strip()

def set_admin_password(new_pw):
    with open(ADMIN_PW_FILE, "w") as f:
        f.write(hashlib.sha256(new_pw.encode()).hexdigest())

def check_admin_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest() == get_admin_password()


# -----------------------------------------------------------------------------
# Fingerprint DB (unified in users.db)
# -----------------------------------------------------------------------------
_fingerprint_table_ensured = False
def get_finger_db():
    global _fingerprint_table_ensured
    conn = get_db_connection()
    if not _fingerprint_table_ensured:
        conn.execute('''CREATE TABLE IF NOT EXISTS fingerprints (
            id INTEGER PRIMARY KEY,
            username TEXT,
            template BLOB NOT NULL
        )''')
        conn.commit()
        _fingerprint_table_ensured = True
    return conn

# Database retry wrapper (not needed with single connection, but kept for compatibility)
def db_retry(func, max_retries=5, initial_delay=0.1):
    """Retry database operations on lock errors with exponential backoff"""
    import random
    for attempt in range(max_retries):
        try:
            return func()
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                delay = initial_delay * (2 ** attempt) + random.uniform(0, 0.1)
                print(f"[DB] Database locked, retry {attempt + 1}/{max_retries} after {delay:.2f}s")
                time.sleep(delay)
                continue
            raise
    raise sqlite3.OperationalError(f"Database still locked after {max_retries} retries")



# ---------- Template-ID allocation helpers ----------
def get_or_reserve_template_id(emp_id: str) -> tuple[int, bool]:
    """
    Get or reserve template ID - ONLY uses fingerprint_map.
    Also syncs name from users table.
    """
    emp_id = str(emp_id).strip()
    if not emp_id:
        raise ValueError("emp_id required")

    def _do_reserve():
        conn = get_db_connection()
        try:
            # Check if template_id already exists in fingerprint_map
            r = conn.execute(
                "SELECT template_id FROM fingerprint_map WHERE emp_id=?", 
                (emp_id,)
            ).fetchone()
            
            if r and r["template_id"] is not None:
                return int(r["template_id"]), False

            # Get user info from users table
            user = conn.execute(
                "SELECT name, created_at FROM users WHERE emp_id=?",
                (emp_id,)
            ).fetchone()
            
            name = user['name'] if user and user['name'] else ''
            created_at = user['created_at'] if user and user['created_at'] else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Allocate new template_id
            tid = _first_free_template_id(conn)
            
            # Save to fingerprint_map with user info
            conn.execute(
                "INSERT INTO fingerprint_map(emp_id, template_id, name, created_at) VALUES (?, ?, ?, ?)",
                (emp_id, tid, name, created_at)
            )
            conn.commit()
            return tid, True
        finally:
            conn.close()
    
    return db_retry(_do_reserve)


def get_template_id_if_any(emp_id: str) -> int | None:
    """Get template ID - ONLY checks fingerprint_map"""
    emp_id = str(emp_id).strip()
    if not emp_id:
        return None
    
    def _do_get():
        conn = get_db_connection()
        try:
            r = conn.execute(
                "SELECT template_id FROM fingerprint_map WHERE emp_id=?", 
                (emp_id,)
            ).fetchone()
            return int(r["template_id"]) if r and r["template_id"] is not None else None
        finally:
            conn.close()
    
    return db_retry(_do_get)


def _first_free_template_id(conn) -> int:
    """Find first available template ID - checks ONLY fingerprint_map"""
    used = set()
    
    # Check fingerprint_map (primary source)
    for r in conn.execute("SELECT template_id FROM fingerprint_map WHERE template_id IS NOT NULL"):
        try:
            used.add(int(r["template_id"]))
        except Exception:
            pass
    
    # Check fingerprints backup table
    # for r in conn.execute("SELECT id FROM fingerprints"):
    #     try:
    #         used.add(int(r["id"]))
    #     except Exception:
    #         pass

    tid = 1
    while tid in used:
        tid += 1
    return tid



# Initialize database for concurrent access
def init_database():
    '''Initialize database with WAL mode for concurrent access'''
    try:
        conn = get_db_connection()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.execute("PRAGMA page_size=4096")
        conn.execute("PRAGMA cache_size=10000")
        conn.close()
        print("[DB] Database initialized with WAL mode")
    except Exception as e:
        print(f"[DB] Warning: Could not initialize WAL mode: {e}")

# Start MSSQL sync worker for offline queue processing
if MSSQL_AVAILABLE:
    _mssql_stop_event = Event()
    threading.Thread(target=mssql_sync_worker, args=(_mssql_stop_event,), daemon=True).start()
    print("[MSSQL] Background worker thread started")

    _pg_stop_event = Event()
    threading.Thread(target=pg_sync_worker, args=(_pg_stop_event,), daemon=True).start()
    print("[PG] Background sync worker started")

# -----------------------------------------------------------------------------
# Slot-aware helpers, limits, device ID, and ordering
# -----------------------------------------------------------------------------
def get_device_id():
    """Get current device ID"""
    val = get_setting("device_id", "IN_001")
    return (val or "IN_001").strip().upper()

def get_device_type():
    """Get current device type (IN/OUT/ALTI/O/Canteen)"""
    val = get_setting("device_type", "IN")
    return (val or "IN").strip()

def get_canteen_id():
    """Get canteen ID from canteen_mappings table."""
    try:
        conn = get_db_connection()
        row = conn.execute("SELECT canteen_id FROM canteen_mappings ORDER BY updated_at DESC LIMIT 1").fetchone()
        conn.close()
        return row["canteen_id"] if row and row["canteen_id"] else ""
    except Exception:
        return ""

def get_device_direction():
    """Extract direction from device_id (e.g., IN_001 -> IN)"""
    device_id = get_device_id()
    return device_id.split('_')[0] if '_' in device_id else 'IN'

def set_device_config(device_id: str, device_type: str):
    """Save device ID and type configuration"""
    device_id = device_id.strip().upper()
    device_type = device_type.strip()

    # Validate device_type
    if device_type not in ['IN', 'OUT', 'ALTI/O', 'Canteen']:
        raise ValueError("Device type must be IN, OUT, ALTI/O, or Canteen")

    # Validate device_id format: TYPE_NNN (e.g., IN_001, OUT_002, Canteen_001)
    if not re.fullmatch(r"[A-Za-z\/]+_[0-9]{3}", device_id):
        raise ValueError("Device ID must be in format TYPE_001 (e.g., IN_001, OUT_002)")

    set_setting("device_id", device_id)
    set_setting("device_type", device_type)

def _now_iso():
    return datetime.now().isoformat(timespec="seconds")

 
def _today_datestr():
    return date.today().isoformat()
  

 
 
 
# =============================================================================
# DEVICE CONSOLE SERVICE — Integration with central DeviceConsole Server
# =============================================================================
device_console_service = None
try:
    device_console_service = init_device_console(
        app=app,
        get_setting=get_setting,
        set_setting=set_setting,
        get_self_ip=get_self_ip,
        get_device_id=get_device_id,
        db_path=DB_PATH,
        app_port=app.config.get("APP_PORT", 5000),
    )
    print("[DEVICECONSOLE] Device Console Service initialized")
except Exception as e:
    print(f"[DEVICECONSOLE] Init failed (non-fatal): {e}")
    device_console_service = None


# -----------------------------------------------------------------------------
# UI routes
# -----------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route("/Network")
@require_page_permission("Network")
def Network_page():
    return render_template("Network.html")

@app.route("/api/my_ip")
def api_my_ip():
    return jsonify({"ip": get_self_ip()})

@app.route("/api/device_state")
def api_device_state():
    return jsonify({"sleep_mode": is_sleep_mode()})

@app.route('/menu')
def menu():
    return render_template('menu.html')

@app.route('/settings')
@require_page_permission("settings") 
def settings():
    return render_template('settings.html')

@app.route('/device_config')
@require_page_permission("device_config") 
def device_config():
    return render_template('device_config.html')

@app.route('/user')
def user():
    return render_template('user.html')

@app.route("/register")
@require_page_permission("register")
def register():
    return render_template("register.html")

@app.route("/logout")
def logout():
    session.pop("admin_session_active", None)
    session.pop("admin_emp_id", None)
    return redirect(url_for("menu"))

@app.route('/delete')
@require_page_permission("delete") 
def delete():
    return render_template('delete.html')

@app.route("/finger_user")
def finger_user():
    return render_template("finger_user.html")

@app.route("/finger_register")
@require_page_permission("finger_register") 
def finger_register():
    return render_template("finger_register.html")

@app.route("/finger_edit")
@require_page_permission("finger_edit")
def finger_edit():
    return render_template("finger_edit.html")

@app.route("/finger_delete")
@require_page_permission("finger_delete") 
def finger_delete():
    return render_template("finger_delete.html")

@app.route("/rfid_user")
def rfid_user_page():
    return render_template("rfid_user.html")

@app.route("/rfid_register")
@require_page_permission("rfid_register") 
def rfid_register_page():
    return render_template("rfid_register.html")

@app.route("/rfid_edit")
@require_page_permission("rfid_edit") 
def rfid_edit():
    return render_template("rfid_edit.html")

@app.route("/rfid_delete")
@require_page_permission("rfid_delete")
def rfid_delete():
    return render_template("rfid_delete.html")

@app.route("/userlog_import")
def userlog_import():
    return render_template("userlog_import.html")
    
@app.route("/datalog_import")
def datalog_import():
    return render_template("datalog_import.html")
    
@app.route("/userlog_export")
def userlog_export():
    return render_template("userlog_export.html")

@app.route("/datalog_export")
def datalog_export():
    return render_template("datalog_export.html")
@app.route('/shift_master')
@require_page_permission("shift_master") 
def shift_master(): 
    return render_template('shift_master.html')

@app.route('/menu_master')
@require_page_permission("menu_master") 
def menu_master(): 
    return render_template('menu_master.html')
@app.route('/item_limit_master')
@require_page_permission("item_limits")  
def item_limit_master(): 
    return render_template('item_limit_master.html')
@app.route('/item_master')
@require_page_permission("item_master")
def item_master(): 
    return render_template('item_master.html')
@app.route('/check_menu')
def check_menu(): 
    return render_template('check_menu.html')
@app.route('/time_slot_master')
@require_page_permission("time_slot_master")
def time_slot_master(): 
     return render_template('time_slot_master.html')    
@app.route('/diagnostic')
@require_page_permission("diagnostic") 
def diagnostic_page():
    return render_template('diagnostic.html')
    
@app.route("/about")
def about():
    return render_template("about.html")
    
@app.route("/userconfig")
@require_page_permission("userconfig") 
def userconfig():
    return render_template("userconfig.html")
    
@app.route('/shift_config')
def shift_config():
    return render_template('shift_config.html')
    
@app.route('/config')
def config_page():
    return render_template('config.html')
    
@app.route('/report')
def report_page():
    return render_template('report.html')

@app.route('/user_management')
def user_management():
    return render_template('user_management.html')

@app.route('/user_data')
def user_data():
    return render_template('user_data.html')
    
@app.route("/api/diagnostic")
def api_diagnostic():
    import diagnostic
    try:
        results = diagnostic.run_diagnostic(json_mode=True)
        return jsonify(results=results)
    except Exception as e:
        return jsonify(results=[{"name": "Error", "ok": False, "info": str(e)}])

# Themed Import/Export desktop pages (you supplied these templates)
@app.route("/import")
@require_page_permission("import") 
def import_page():
    return render_template("import.html")

@app.route("/export")
@require_page_permission("export") 
def export_page():
    return render_template("export.html")
    
    




# -----------------------------------------------------------------------------
# LED DIAGNOSTIC (INDEPENDENT) — does NOT depend on face_recognizer.led_blink
# -----------------------------------------------------------------------------
LED_DIAG_PIXELS = int(os.environ.get("LED_PIXELS", "15"))          # you said 5 pixels
LED_DIAG_GPIO   = int(os.environ.get("LED_GPIO", "12"))           # default GPIO12 (PWM-capable pin)
LED_DIAG_PIN    = os.environ.get("LED_PIN", "D12").strip().upper()# for CircuitPython neopixel backend
LED_DIAG_BRIGHT = float(os.environ.get("LED_BRIGHTNESS", "1.0"))
LED_DIAG_BACKEND = os.environ.get("LED_BACKEND", "").strip().lower()
# LED_BACKEND can be: neopixel_spi | neopixel | rpi_ws281x (optional)

_diag_led_lock = Lock()
_diag_led_obj = None
_diag_led_backend = None
_diag_led_init_error = None

def _diag_led_init():
    """Try multiple LED backends. Returns (ok, err_str_or_None)."""
    global _diag_led_obj, _diag_led_backend, _diag_led_init_error

    if _diag_led_obj is not None:
        return True, None

    with _diag_led_lock:
        if _diag_led_obj is not None:
            return True, None

        _diag_led_obj = None
        _diag_led_backend = None
        _diag_led_init_error = None

        candidates = [LED_DIAG_BACKEND] if LED_DIAG_BACKEND else ["neopixel_spi", "neopixel", "rpi_ws281x"]
        errs = []

        for cand in candidates:
            if not cand:
                continue

            try:
                # 1) SPI backend (common + very stable): uses SPI0 MOSI (GPIO10)
                if cand in ("neopixel_spi", "spi", "spi0"):
                    import board
                    import busio
                    import neopixel_spi as neopixel_spi

                    spi = busio.SPI(board.SCK, MOSI=board.MOSI)
                    px = neopixel_spi.NeoPixel_SPI(
                        spi,
                        LED_DIAG_PIXELS,
                        auto_write=True,
                        pixel_order=neopixel_spi.GRB
                    )
                    px.brightness = max(0.0, min(1.0, float(LED_DIAG_BRIGHT)))
                    px.fill((0, 0, 0))

                    _diag_led_obj = px
                    _diag_led_backend = "neopixel_spi"
                    return True, None

                # 2) CircuitPython neopixel on a GPIO pin (uses LED_PIN like D12/D18)
                if cand in ("neopixel", "gpio", "bitbang"):
                    import board
                    import neopixel

                    if not hasattr(board, LED_DIAG_PIN):
                        raise RuntimeError(f"board.{LED_DIAG_PIN} not found. Set LED_PIN like D12, D18, etc.")
                    pin = getattr(board, LED_DIAG_PIN)

                    px = neopixel.NeoPixel(
                        pin,
                        LED_DIAG_PIXELS,
                        brightness=max(0.0, min(1.0, float(LED_DIAG_BRIGHT))),
                        auto_write=True
                    )
                    px.fill((0, 0, 0))

                    _diag_led_obj = px
                    _diag_led_backend = "neopixel"
                    return True, None

                # 3) rpi_ws281x PixelStrip backend (GPIO PWM / sometimes SPI)
                if cand in ("rpi_ws281x", "ws281x", "pixelstrip", "pwm"):
                    from rpi_ws281x import PixelStrip, Color

                    bright = int(max(0.0, min(1.0, float(LED_DIAG_BRIGHT))) * 255)
                    strip = PixelStrip(
                        LED_DIAG_PIXELS,
                        LED_DIAG_GPIO,
                        800000,   # WS2812 freq
                        10,       # DMA
                        False,    # invert
                        bright,
                        0         # channel
                    )
                    strip.begin()
                    for i in range(LED_DIAG_PIXELS):
                        strip.setPixelColor(i, Color(0, 0, 0))
                    strip.show()

                    _diag_led_obj = strip
                    _diag_led_backend = "rpi_ws281x"
                    return True, None

                errs.append(f"{cand}: unknown backend")

            except Exception as e:
                errs.append(f"{cand}: {e}")

        _diag_led_init_error = " | ".join(errs) if errs else "No LED backend available"
        return False, _diag_led_init_error

def _diag_led_fill(rgb):
    """Set all pixels to rgb."""
    ok, err = _diag_led_init()
    if not ok:
        raise RuntimeError(err)

    r, g, b = [int(x) for x in rgb]

    if _diag_led_backend in ("neopixel_spi", "neopixel"):
        _diag_led_obj.fill((r, g, b))
    else:
        from rpi_ws281x import Color
        for i in range(LED_DIAG_PIXELS):
            _diag_led_obj.setPixelColor(i, Color(r, g, b))
        _diag_led_obj.show()

def diag_led_blink_pattern(color=(0, 255, 0), on=0.20, off=0.12, repeats=4):
    """Blink pattern used by /api/diag/led (blocking)."""
    ok, err = _diag_led_init()
    if not ok:
        raise RuntimeError(err)

    with _diag_led_lock:
        try:
            for _ in range(int(repeats)):
                _diag_led_fill(color)
                time.sleep(float(on))
                _diag_led_fill((0, 0, 0))
                time.sleep(float(off))
        finally:
            try:
                _diag_led_fill((0, 0, 0))
            except Exception:
                pass


@app.route("/api/diag/led", methods=["POST"])
def api_diag_led():
    try:
        mark_activity()
    except Exception:
        pass

    try:
        diag_led_blink_pattern(color=(0, 255, 0), on=0.20, off=0.12, repeats=4)

        return jsonify({
            "success": True,
            "needs_user_confirm": True,
            "backend": _diag_led_backend,
            "message": f"LED blink executed (backend: {_diag_led_backend}). Confirm visually if it blinked."
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"LED command failed: {e}"
        }), 200





@app.route("/api/diag/fingerprint", methods=["POST"])
def api_diag_fingerprint():
    try:
        mark_activity()
    except Exception:
        pass

    def _try_fp_glow(sensor, on: bool) -> bool:
        """Best-effort LED glow (safe no-op if unsupported)."""
        noarg_on  = ("led_on", "cmos_led_on", "light_on", "turn_on_led")
        noarg_off = ("led_off", "cmos_led_off", "light_off", "turn_off_led")
        arg_names = ("set_led", "setLED", "set_led_state", "cmosled")

        for name in (noarg_on if on else noarg_off):
            fn = getattr(sensor, name, None)
            if callable(fn):
                try:
                    fn()
                    return True
                except Exception:
                    pass

        for name in arg_names:
            fn = getattr(sensor, name, None)
            if callable(fn):
                for val in (on, (1 if on else 0), ("ON" if on else "OFF")):
                    try:
                        fn(val)
                        return True
                    except Exception:
                        pass
        return False

    try:
        with sensor_lock:
            # IMPORTANT: drop cached instance so we force a REAL handshake every click
            global fingerprint_sensor
            fingerprint_sensor = None

            # This calls Fingerprint(port,9600).init() inside get_fingerprint_sensor()
            # If sensor is removed / not responding -> init should fail/raise -> we return FAIL.
            s = get_fingerprint_sensor()

            s.open()
            try:
                # Glow once (also proves the command path works)
                _try_fp_glow(s, True)
                time.sleep(0.35)
                _try_fp_glow(s, False)

                return jsonify({
                    "success": True,
                    "message": "Fingerprint sensor communication OK"
                }), 200
            finally:
                try:
                    s.close()
                except Exception:
                    pass

    except Exception as e:
        return jsonify({"success": False, "message": f"Fingerprint driver missing / not responding: {e}"}), 200


@app.route("/api/diag/rfid", methods=["POST"])
def api_diag_rfid():
    try:
        mark_activity()
    except Exception:
        pass

    try:
        # Try to read for a few seconds
        ok, msg = rfid.rfid_read(timeout=8)
        if ok:
            # msg may be UID or "UID: xxxx"
            m = re.search(r"([0-9A-Fa-f]{8,})", str(msg))
            uid = m.group(1) if m else None
            return jsonify({"success": True, "uid": uid, "message": "Card detected"}), 200

        # Some implementations put UID inside msg even when ok=False
        m = re.search(r"([0-9A-Fa-f]{8,})", msg)
        if m:
            return jsonify({"success": True, "uid": m.group(1), "message": "Card detected"}), 200

        return jsonify({"success": False, "message": msg}), 200

    except Exception as e:
        return jsonify({"success": False, "message": f"RFID test failed: {e}"}), 200


import os, base64, sqlite3, threading, subprocess
import cv2
from flask import jsonify, request

@app.route("/api/diag/db", methods=["POST"])
def api_diag_db():
    try:
        mark_activity()
    except Exception:
        pass

    sqlite_ok = False
    sqlite_msg = ""
    tables = []
    user_version = None

    # ---- SQLite check ----
    try:
        conn = get_db_connection()
        conn.execute("SELECT 1").fetchone()
        sqlite_ok = True

        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        tables = [(r["name"] if hasattr(r, "keys") else r[0]) for r in rows]

        uv = conn.execute("PRAGMA user_version").fetchone()
        if uv:
            user_version = int(uv[0])

        sqlite_msg = "SQLite OK"

    except Exception as e:
        sqlite_ok = False
        sqlite_msg = f"SQLite failed: {e}"

    # ---- MSSQL check (only if you already have these helpers in your project) ----
    mssql_cfg = None
    mssql_ok = None
    mssql_msg = ""
    try:
        if "get_mssql_connection_params" in globals() and "test_mssql_connection" in globals():
            mssql_cfg = get_mssql_connection_params()
            # if configured, test it
            if mssql_cfg and mssql_cfg.get("server") and mssql_cfg.get("database") and mssql_cfg.get("user"):
                mssql_ok, mssql_msg = test_mssql_connection(
                    server=mssql_cfg["server"],
                    database=mssql_cfg["database"],
                    user=mssql_cfg["user"],
                    password=mssql_cfg.get("password", ""),
                    port=str(mssql_cfg.get("port", "1433") or "1433"),
                )
    except Exception as e:
        mssql_ok = False
        mssql_msg = f"MSSQL failed: {e}"

    # Decide overall result:
    # - If MSSQL is configured -> its result decides success
    # - Else -> SQLite decides success
    if mssql_ok is None:
        overall_ok = sqlite_ok
        overall_msg = sqlite_msg + " (no MSSQL configured)"
    else:
        overall_ok = bool(mssql_ok)
        overall_msg = ("MSSQL OK" if mssql_ok else "MSSQL failed") + (f": {mssql_msg}" if mssql_msg else "")
        # keep sqlite info too
        overall_msg = f"{sqlite_msg}; {overall_msg}"

    return jsonify({
        "success": overall_ok,
        "message": overall_msg,
        "tables": tables,
        "user_version": user_version
    }), 200


@app.route("/api/diag/camera", methods=["POST"])
def api_diag_camera():
    try:
        mark_activity()
    except Exception:
        pass

    payload = request.get_json(silent=True) or {}
    idx = 0
    try:
        idx = int(payload.get("device_index", 0) or 0)
    except Exception:
        idx = 0

    # Fast snapshot for diagnostics
    try:
        cap = cv2.VideoCapture(idx, cv2.CAP_V4L2)
        if not cap or not cap.isOpened():
            return jsonify({"success": False, "message": f"Camera index {idx} not accessible"}), 200

        ok, frame = cap.read()
        cap.release()

        if not ok or frame is None:
            return jsonify({"success": False, "message": "Camera read failed"}), 200

        ok, buf = cv2.imencode(".jpg", frame)
        if not ok:
            return jsonify({"success": False, "message": "JPEG encode failed"}), 200

        b64 = base64.b64encode(buf.tobytes()).decode("ascii")
        return jsonify({
            "success": True,
            "index": idx,
            "snapshot": "data:image/jpeg;base64," + b64,
            "message": "Camera snapshot OK"
        }), 200

    except Exception as e:
        return jsonify({"success": False, "message": f"Camera error: {e}"}), 200


@app.route("/api/diag/speaker", methods=["POST"])
def api_diag_speaker():
    try:
        mark_activity()
    except Exception:
        pass

    # best-effort: trigger server-side playback (doesn't block)
    try:
        wav_path = os.path.join(app.root_path, "static", "audio", "thank_you.wav")

        def _play():
            try:
                if "play_wav" in globals():
                    play_wav(wav_path)
                else:
                    subprocess.Popen(
                        ["pw-play", wav_path],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
            except Exception:
                pass

        threading.Thread(target=_play, daemon=True).start()

        return jsonify({
            "success": True,
            "message": "Speaker test triggered (did you hear sound?)"
        }), 200

    except Exception as e:
        return jsonify({"success": False, "message": f"Speaker test failed: {e}"}), 20


# -----------------------------------------------------------------------------
# Camera (live stream) — single authoritative implementation (no duplicates)
# -----------------------------------------------------------------------------
picam2_instance = None
vcap_instance = None
_cam_lock = Lock()
_CAM_SIZE = (640, 480)
_STREAM_FPS = 8.0          # throttle MJPEG to reduce CPU/FPS issues
_JPEG_QUALITY = 70         # lighter JPEG for snappier streaming
_last_frame_bgr = None     # fallback frame if capture hiccups
_cam_error_count = 0       # Track camera errors for self-repair
_cam_last_reset = time.time()

def _init_camera_locked():
    global picam2_instance, vcap_instance
    if picam2_instance or vcap_instance:
        return
    if Picamera2 is not None:
        try:
            _p = Picamera2()
            _cfg = _p.create_preview_configuration(
                main={"format": "YUV420", "size": _CAM_SIZE}
            )
            _p.configure(_cfg)
            _p.start()
            time.sleep(0.4)
            # Disable continuous AF on streaming camera — face quality checker
            # will trigger AF on the face bounding box via set_controls
            try:
                import libcamera as _lc
                _p.set_controls({
                    "AfMode": _lc.controls.AfModeEnum.Manual,
                })
                print("[CAM] Streaming camera: continuous AF disabled (face-targeted AF active)")
            except Exception as _af_err:
                print(f"[CAM] Could not set AF mode: {_af_err}")
            picam2_instance = _p
            return
        except Exception as e:
            print(f"[CAM] PiCamera2 unavailable: {e}. Falling back to OpenCV webcam...")
    cap = cv2.VideoCapture(0, cv2.CAP_V4L2)
    if not cap or not cap.isOpened():
        raise RuntimeError("No camera found (PiCamera2 and /dev/video0 both unavailable)")
    cap.set(cv2.CAP_PROP_FRAME_WIDTH,  _CAM_SIZE[0])
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, _CAM_SIZE[1])
    cap.set(cv2.CAP_PROP_AUTOFOCUS, 0)  # Disable continuous AF
    vcap_instance = cap

_last_stream_af_trigger = 0.0

def _trigger_face_af_on_stream(face_box):
    """Trigger one-shot AF on the streaming picam2 instance, targeting the face box."""
    global _last_stream_af_trigger
    now = time.time()
    if now - _last_stream_af_trigger < 1.5:
        return  # throttle to avoid AF hunting
    _last_stream_af_trigger = now

    if picam2_instance is None:
        return
    try:
        import libcamera as _lc
        top, right, bottom, left = face_box
        w, h = _CAM_SIZE
        # Pad the AF window slightly (20%) for better AF context
        fw = right - left
        fh = bottom - top
        pad_x = int(fw * 0.1)
        pad_y = int(fh * 0.1)
        x0 = max(0, left - pad_x)
        y0 = max(0, top - pad_y)
        x1 = min(w, right + pad_x)
        y1 = min(h, bottom + pad_y)
        picam2_instance.set_controls({
            "AfMode": _lc.controls.AfModeEnum.Auto,
            "AfWindows": [(x0, y0, x1 - x0, y1 - y0)],
            "AfTrigger": _lc.controls.AfTriggerEnum.Start,
        })
    except Exception as e:
        print(f"[CAM] Stream face AF trigger failed: {e}")


def _yuv420_to_bgr(yuv, size):
    w, h = size
    if yuv.ndim == 2 and yuv.shape[0] == h * 3 // 2 and yuv.shape[1] == w:
        return cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_I420)
    if yuv.ndim == 3 and yuv.shape[0] == h and yuv.shape[1] == w and yuv.shape[2] in (2, 3):
        try:
            return cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_NV12)
        except Exception:
            pass
    return yuv

def get_live_frame_bgr():
    """
    Capture a single BGR frame with auto-recovery (self-repair):
    - Re-init camera on failure once.
    - Return last good frame if capture keeps failing.
    - Auto-reset camera if too many errors accumulate.
    """
    global picam2_instance, vcap_instance, _last_frame_bgr, _cam_error_count, _cam_last_reset

    with _cam_lock:
        # Self-repair: Force camera reset if too many errors
        if _cam_error_count > 20 and (time.time() - _cam_last_reset) > 60:
            print("[SELF-REPAIR] Forcing camera reset due to excessive errors")
            try:
                if picam2_instance:
                    picam2_instance.stop()
                    picam2_instance.close()
                if vcap_instance:
                    vcap_instance.release()
            except Exception as cleanup_err:
                print(f"[SELF-REPAIR] Camera cleanup error: {cleanup_err}")
            picam2_instance = None
            vcap_instance = None
            _cam_error_count = 0
            _cam_last_reset = time.time()

        if not (picam2_instance or vcap_instance):
            _init_camera_locked()

        try:
            if picam2_instance:
                yuv = picam2_instance.capture_array("main")
                if yuv is None:
                    raise RuntimeError("PiCamera2 capture returned None")
                frame_bgr = _yuv420_to_bgr(yuv, _CAM_SIZE)
                _last_frame_bgr = frame_bgr
                _cam_error_count = max(0, _cam_error_count - 1)  # Decrease error count on success
                return frame_bgr
            ok, frame_bgr = vcap_instance.read()
            if not ok or frame_bgr is None:
                raise RuntimeError("Webcam frame read failed")
            _last_frame_bgr = frame_bgr
            _cam_error_count = max(0, _cam_error_count - 1)  # Decrease error count on success
            return frame_bgr
        except Exception as e:
            _cam_error_count += 1
            print(f"[CAM] capture error (error count: {_cam_error_count}), retrying: {e}")
            # try one soft re-init
            picam2_instance = None
            vcap_instance = None
            try:
                _init_camera_locked()
                if picam2_instance:
                    yuv = picam2_instance.capture_array("main")
                    if yuv is not None:
                        frame_bgr = _yuv420_to_bgr(yuv, _CAM_SIZE)
                        _last_frame_bgr = frame_bgr
                        return frame_bgr
                elif vcap_instance:
                    ok, frame_bgr = vcap_instance.read()
                    if ok and frame_bgr is not None:
                        _last_frame_bgr = frame_bgr
                        return frame_bgr
            except Exception as e2:
                _cam_error_count += 1
                print(f"[CAM] re-init failed (error count: {_cam_error_count}): {e2}")
            if _last_frame_bgr is not None:
                print("[CAM] Using cached frame as fallback")
                return _last_frame_bgr
            raise

def gen_frames():
    frame_interval = 1.0 / max(_STREAM_FPS, 1.0)
    encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), _JPEG_QUALITY]
    try:
        while True:
            loop_start = time.time()
            try:
                bgr = get_live_frame_bgr()
            except Exception as e:
                print(f"[CAM] gen_frames error: {e}")
                bgr = _last_frame_bgr
            if bgr is None:
                bgr = np.zeros((_CAM_SIZE[1], _CAM_SIZE[0], 3), dtype=np.uint8)

            ok, buffer = cv2.imencode('.jpg', bgr, encode_params)
            if ok:
                frame_bytes = buffer.tobytes()
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
            else:
                print("[CAM] imencode failed")

            elapsed = time.time() - loop_start
            if elapsed < frame_interval:
                time.sleep(frame_interval - elapsed)
    except GeneratorExit:
        # Client disconnected - cleanup happens here
        print("[CAM] Video feed client disconnected, generator cleaned up")
    except Exception as e:
        print(f"[CAM] gen_frames fatal error: {e}")

@app.route('/video_feed')
def video_feed():
    return Response(gen_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/api/show_keyboard')
def show_keyboard():
    return ('', 204)


# -----------------------------------------------------------------------------
# Serve stored user images (for register.html HEAD check)
# -----------------------------------------------------------------------------
USERS_IMG_DIR = os.path.join(os.getcwd(), "users")
os.makedirs(USERS_IMG_DIR, exist_ok=True)

@app.route("/users/<path:filename>", methods=["GET", "HEAD"])
def serve_user_image(filename):
    if not re.fullmatch(r"[A-Za-z0-9_\-\.]+", filename):
        return abort(404)
    path = os.path.join(USERS_IMG_DIR, filename)
    if not os.path.isfile(path):
        return abort(404)
    return send_from_directory(USERS_IMG_DIR, filename)


# -----------------------------------------------------------------------------
# Lightweight NO-MESH STUBS (mesh/TCP/UDP removed)
# -----------------------------------------------------------------------------
def load_mesh_state():
    """Mesh removed — return empty state so callers behave normally."""
    return {}
    
    
def broadcast_tcp(payload):
    """Mesh removed — log and ignore."""
    try:
        print(f"[NET-TCP-STUB] Would broadcast TCP payload keys: {list(payload.keys())}")
    except Exception:
        pass
    return False

def broadcast_login_udp(emp_id, name, medium):
    """Called when login happens — stubbed out."""
    try:
        print(f"[LOGIN-UDP-STUB] login {emp_id} ({name}) via {medium}")
    except Exception:
        pass

def broadcast_login_tcp(emp_id, name, medium):
    """Called when login happens — stubbed out."""
    try:
        print(f"[LOGIN-TCP-STUB] login {emp_id} ({name}) via {medium}")
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Template-ID API (auto-incremental & reserved per emp_id)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# User upsert (emp_id, name, role)
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# User upsert (emp_id, name, role, birthdate)
# -----------------------------------------------------------------------------
@app.route("/api/user_upsert", methods=["POST"])
def api_user_upsert():
    data      = request.get_json(force=True) or {}
    emp_id    = (data.get("emp_id") or "").strip()
    name      = (data.get("name") or "").strip()
    role      = (data.get("role") or "User").strip()
    shift     = (data.get("shift") or "General").strip()

    if not emp_id:
        return jsonify({"success": False, "message": "Employee ID required."}), 400

    conn = get_db_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Get actual columns present in users table right now
        col_rows = conn.execute("PRAGMA table_info(users)").fetchall()
        existing_cols = {r[1] for r in col_rows}

        # Ensure row exists
        conn.execute("INSERT OR IGNORE INTO users (emp_id) VALUES (?)", (emp_id,))

        # Build UPDATE using only columns that actually exist
        updates = []
        params  = []

        if "role" in existing_cols:
            updates.append("role = ?")
            params.append(role)

        if "shift" in existing_cols:
            updates.append("shift = ?")
            params.append(shift)

        if "updated_at" in existing_cols:
            updates.append("updated_at = ?")
            params.append(now)

        if "created_at" in existing_cols:
            # COALESCE keeps original value, only sets if NULL
            updates.append("created_at = COALESCE(created_at, ?)")
            params.append(now)

        if name and "name" in existing_cols:
            updates.append("name = ?")
            params.append(name)

        if updates:
            params.append(emp_id)
            conn.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE emp_id = ?",
                params
            )

        conn.commit()

        # Mesh broadcast
        mesh_ok = False
        try:
            mesh_ok = bool(broadcast_user_upsert_by_emp(emp_id))
        except Exception as e:
            print("[MESH] broadcast error:", e)

        # Auto template transfer to connected devices
        if NETWORK_ENABLED and device_sync_manager:
            try:
                results = device_sync_manager.transfer_user_template(emp_id)
                if results:
                    ok_count = sum(1 for v in results.values() if v)
                    print(f"[SYNC] transfer {emp_id}: {ok_count}/{len(results)}")
            except Exception as e:
                print(f"[SYNC] transfer error {emp_id}: {e}")

        return jsonify({
            "success": True,
            "message": "User registered successfully",
            
            "mesh_sent": mesh_ok
        })

    except Exception as e:
        print(f"[USER_UPSERT] Error: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        conn.close()

# -----------------------------------------------------------------------------
# Face login & helpers
# -----------------------------------------------------------------------------
recognizer = FaceRecognizer(DB_PATH)
recognizer.load_all_encodings()
@app.route("/api/face_login", methods=["POST"])
def face_login():
    data = request.json or {}
    img_data = (data.get("image") or "").split(",")[-1]
    enable_module_effects = data.get("trigger_effects", False)

    if not img_data:
        return jsonify({"success": False, "reason": "bad_payload"}), 400

    mark_activity()

    # Decode image
    img_bytes = base64.b64decode(img_data)
    nparr = np.frombuffer(img_bytes, np.uint8)
    bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if bgr is None:
        return jsonify({"success": False, "reason": "decode_failed"}), 400

    # Convert to RGB for face_recognition library (same as old version)
    frame_rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

    # Recognize expects RGB frame
    user_id = recognizer.recognize(frame_rgb, trigger_effects=enable_module_effects)
    if user_id is None:
        return jsonify({"success": False, "reason": "no_face"}), 200
    if not user_id:
        return jsonify({"success": False, "reason": "unknown_face"}), 200

    # Fetch name
    name = ""
    conn = get_db_connection()
    row = conn.execute("SELECT name FROM users WHERE emp_id=?", (str(user_id),)).fetchone()
    if row and row["name"]:
        name = row["name"]

    

    # Send to MSSQL in background
    run_parallel(
        lambda: send_to_mssql_with_queue(str(user_id), name, "face"),
    )

    mark_activity()

    # Delegate all effects + banner events to unified handler
    with app.test_request_context():
        pass  # keep context clean
    _dispatch_auth_result(success=True, mode="face", emp_id=str(user_id), name=name)

    return jsonify({"success": True, "user_id": str(user_id), "name": name})

# ============================================================================
# CENTRALIZED EFFECTS SYSTEM - Single Source of Truth for Audio/LED/Printer
# ============================================================================

def trigger_effects(effect_type, emp_id=None, extra_params=None):
    """
    Centralized effects handler - Single source of truth for all audio/LED/printer.

    This function is called by ALL authentication and admin endpoints to ensure
    consistent behavior and eliminate duplicate audio/LED/printer calls.

    Args:
        effect_type (str): Type of effect to trigger
            - 'success': Thank you audio + green LED + printer receipt
            - 'denied': Access denied audio + red LED
            - 'registered': Successfully registered audio + green LED + printer
            - 'deleted': Success deleted audio + green LED + printer
            - 'birthday': Happy birthday audio

        emp_id (str, optional): Employee ID for printer receipt and logging

        extra_params (dict, optional): Additional parameters for customization
            - duration: LED blink duration override
            - color: LED color override (R, G, B tuple)
            - audio_file: Custom audio file path
            - skip_printer: Set True to skip printer

    Returns:
        None - Effects are triggered asynchronously in background threads

    Example:
        trigger_effects('success', emp_id='220503')
        trigger_effects('denied')
        trigger_effects('birthday', emp_id='220503')
    """
    try:
        from face_recognizer import play_wav, led_blink, print_user_id_and_cut

        extra = extra_params or {}
        skip_printer = extra.get('skip_printer', False)

        if effect_type == 'success':
            # Thank you + green LED (no printer on login)
            run_parallel(
                lambda: play_wav(AUDIO_PATH + "thank_you.wav"),
                lambda: led_blink((0, 255, 0), extra.get('duration', 1.2)),
            )

        elif effect_type == 'denied':
            # Access denied + red LED
            run_parallel(
                lambda: play_wav(AUDIO_PATH + "access_denied.wav"),
                lambda: led_blink((255, 0, 0), extra.get('duration', 0.8))
            )

        elif effect_type == 'registered':
            # Registration success + green LED + printer
            run_parallel(
                lambda: play_wav(AUDIO_PATH + "Successfully_Registered.wav"),
                lambda: led_blink((0, 255, 0), extra.get('duration', 1.2)),
                lambda: print_user_id_and_cut(emp_id) if (emp_id and not skip_printer) else None
            )

        elif effect_type == 'deleted':
            # Deletion success + green LED + printer
            run_parallel(
                lambda: play_wav(AUDIO_PATH + "success_deleted.wav"),
                lambda: led_blink((0, 255, 0), extra.get('duration', 1.2)),
                lambda: print_user_id_and_cut(emp_id) if (emp_id and not skip_printer) else None
            )

        elif effect_type == 'birthday':
            # Birthday audio only
            run_parallel(
                lambda: play_wav(AUDIO_PATH + "happy_birthday.wav")
            )

        else:
            print(f"[EFFECTS] Unknown effect type: {effect_type}")

    except Exception as e:
        print(f"[EFFECTS ERROR] {effect_type}: {e}")

def _dispatch_auth_result(success: bool, mode: str, emp_id: str = None,
                           name: str = None, reason: str = None):
    """
    Internal dispatcher — same logic as /api/auth_result but called in-process.
    Used by face_login, finger_identify, rfid_login so effects always go
    through one code path.
    """
    try:
        if success:
            trigger_effects('success', emp_id=emp_id)
            is_birthday = check_and_mark_birthday_greet(emp_id or "")
            if is_birthday:
                trigger_effects('birthday', emp_id=emp_id)
            _emit_thankyou(emp_id, name, mode, extra={
                "birthday": bool(is_birthday),
                "result":   "success"
            })
            run_parallel(lambda: send_to_mssql_with_queue(emp_id, name or "", mode))
        else:
            trigger_effects('denied')
            _emit_thankyou(None, None, mode, extra={
                "result": "denied",
                "reason": reason or "not_recognized"
            })
    except Exception as e:
        print(f"[_dispatch_auth_result] Error: {e}")

# =============================================================================
# UNIFIED AUTH RESULT ENDPOINT
# =============================================================================

@app.route("/api/auth_result", methods=["POST"])
def api_auth_result():
    """
    Unified authentication result handler for Face, Fingerprint, and RFID.

    POST body:
        {
            "success": true/false,
            "mode": "face" | "fingerprint" | "rfid",
            "emp_id": "220503",       // required on success
            "name": "John Doe",       // required on success
            "reason": "not_found"     // optional, for failure logging
        }

    On success:  plays thank_you.wav + green LED blink + emits thankyou event
    On failure:  plays access_denied.wav + red LED blink + emits denied event
    """
    try:
        data = request.get_json(force=True) or {}
        success  = bool(data.get("success", False))
        mode     = (data.get("mode") or "face").strip().lower()
        emp_id   = (data.get("emp_id") or "").strip()
        name     = (data.get("name") or "").strip()
        reason   = (data.get("reason") or "").strip()

        if success:
            if not emp_id:
                return jsonify({"success": False, "message": "emp_id required on success"}), 400

            # 1. Trigger audio + LED + printer
            trigger_effects('success', emp_id=emp_id)

            # 2. Birthday check
            is_birthday = check_and_mark_birthday_greet(emp_id)
            if is_birthday:
                trigger_effects('birthday', emp_id=emp_id)

            # 3. Emit thank-you event so user.html banner fires
            _emit_thankyou(emp_id, name, mode, extra={
                "birthday": bool(is_birthday),
                "result":   "success"
            })

            # 4. MSSQL queue (non-blocking)
            run_parallel(lambda: send_to_mssql_with_queue(emp_id, name, mode))

            return jsonify({
                "success":      True,
                "action":       "success",
                "emp_id":       emp_id,
                "name":         name,
                "mode":         mode,
                "is_birthday":  bool(is_birthday)
            })

        else:
            # 1. Trigger denied audio + red LED
            trigger_effects('denied')

            # 2. Emit denied event so user.html banner fires
            _emit_thankyou(None, None, mode, extra={
                "result": "denied",
                "reason": reason or "not_recognized"
            })

            return jsonify({
                "success": True,        # HTTP call itself succeeded
                "action":  "denied",
                "mode":    mode,
                "reason":  reason or "not_recognized"
            })

    except Exception as e:
        print(f"[AUTH_RESULT] Error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


# New unified API endpoint (replaces /api/trigger_thank_you and /api/trigger_access_denied)
@app.route("/api/trigger_effects", methods=["POST"])
def api_trigger_effects():
    """
    Unified API endpoint for triggering effects.

    POST body:
        {
            "type": "success" | "denied" | "registered" | "deleted" | "birthday",
            "emp_id": "220503",  // optional
            "extra": {...}        // optional extra parameters
        }

    Returns:
        {"success": true}
    """
    try:
        data = request.json or {}
        effect_type = data.get("type", "success")
        emp_id = data.get("emp_id")
        extra = data.get("extra")

        trigger_effects(effect_type, emp_id, extra)

        return jsonify({"success": True})
    except Exception as e:
        print(f"[ERROR] api_trigger_effects: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# DEPRECATED: Old endpoints kept for backward compatibility (will be removed)
@app.route("/api/trigger_access_denied", methods=["POST"])
def trigger_access_denied():
    """DEPRECATED: Use /api/trigger_effects with type='denied' instead"""
    trigger_effects('denied')
    return jsonify({"success": True})


@app.route("/api/trigger_thank_you", methods=["POST"])
def trigger_thank_you():
    """Trigger thank you effects: audio + green LED + printer receipt.
    No debounce - always triggers (user requested: every login should work)
    """
    data = request.json or {}
    emp_id = data.get("emp_id") or ""

    try:
        trigger_effects('success', emp_id)
    except Exception as e:
        print(f"[trigger_thank_you] Error (ignored): {e}")

    return jsonify({"success": True})


@app.route("/api/trigger_registered", methods=["POST"])
def trigger_registered():
    """Trigger registration success effects: audio + green LED"""
    data = request.json or {}
    emp_id = data.get("emp_id")
    trigger_effects('registered', emp_id)
    return jsonify({"success": True})


@app.route("/api/trigger_deleted", methods=["POST"])
def trigger_deleted():
    """Trigger deletion success effects: audio + green LED"""
    data = request.json or {}
    emp_id = data.get("emp_id")
    trigger_effects('deleted', emp_id)
    return jsonify({"success": True})


@app.route("/api/trigger_error", methods=["POST"])
def trigger_error():
    """Trigger error effects: access_denied audio + red LED"""
    trigger_effects('denied')
    return jsonify({"success": True})


@app.route("/api/face_detect", methods=["POST"])
def api_face_detect():
    try:
        data = request.get_json(force=True) or {}
        img_b64 = (data.get("image") or "").split(",")[-1]
        img_bytes = base64.b64decode(img_b64)
        nparr = np.frombuffer(img_bytes, np.uint8)
        bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if bgr is None:
            return jsonify({"success": False, "message": "decode_failed"})
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

        import face_recognition
        locs = face_recognition.face_locations(rgb)
        if not locs:
            return jsonify({"success": False})
        top, right, bottom, left = locs[0]
        box = {"x": int(left), "y": int(top), "w": int(right - left), "h": int(bottom - top)}
        return jsonify({"success": True, "box": box, "score": 0.96})
    except Exception:
        return jsonify({"success": False})

@app.route("/api/detect_face_presence", methods=["POST"])
def api_detect_face_presence():
    """
    Simple endpoint to detect if ANY face is present (for index.html sleep mode wake-up).
    Returns {"face_detected": true/false}
    """
    data = request.get_json(force=True) or {}
    img_b64 = (data.get("image") or "").split(",")[-1]
    if not img_b64:
        return jsonify({"face_detected": False})

    img_bytes = base64.b64decode(img_b64)
    nparr = np.frombuffer(img_bytes, np.uint8)
    bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if bgr is None:
        return jsonify({"face_detected": False})

    # Use Haar Cascade for presence detection
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    faces = SLEEP_FACE_CASCADE.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=4, minSize=(50, 50))

    return jsonify({"face_detected": len(faces) > 0})

@app.route("/api/rfid_detect_presence", methods=["POST"])
def api_rfid_detect_presence():
    """
    Simple endpoint to detect if an RFID card is present (for index.html sleep mode wake-up).
    Returns {"card_detected": true/false}
    """
    try:
        # Use rfid module's rfid_read function with short timeout to avoid creating new instances
        # This properly handles RFID reader creation and cleanup
        ok, result = rfid.rfid_read(timeout=1)  # Quick 1 second check

        if ok:
            return jsonify({"card_detected": True})
        return jsonify({"card_detected": False})
    except Exception as e:
        print(f"[ERROR] RFID presence detection failed: {e}")
        return jsonify({"card_detected": False})

@app.route("/api/check_face_duplicate", methods=["POST"])
def check_face_duplicate():
    data = request.json or {}
    img_data = (data.get("image") or "").split(",")[-1]
    if not img_data:
        return jsonify({"duplicate": False})
    img_bytes = base64.b64decode(img_data)
    nparr = np.frombuffer(img_bytes, np.uint8)
    bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if bgr is None:
        return jsonify({"duplicate": False})
    frame = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
    match = recognizer.find_duplicate(frame)
    if match:
        return jsonify({
            "duplicate": True, "emp_id": match["emp_id"], "name": match["name"], "image": match["image"]
        })
    else:
        return jsonify({"duplicate": False})

def _json_safe_payload(payload: dict):
    """
    Convert numpy types to native Python so Flask's JSON encoder can handle them.
    Leaves unknown types untouched for fallback error handling.
    """
    def _convert(val):
        try:
            import numpy as _np
            if isinstance(val, _np.generic):
                return val.item()
            if isinstance(val, _np.ndarray):
                return val.tolist()
        except Exception:
            pass
        if isinstance(val, dict):
            return {k: _convert(v) for k, v in val.items()}
        if isinstance(val, (list, tuple)):
            return type(val)(_convert(v) for v in val)
        return val
    return _convert(payload)

@app.route("/api/face_quality_check", methods=["POST"])
def face_quality_check():
    """
    Real-time face quality checking with progress feedback
    Returns quality score, coverage, sharpness, brightness and guidance message
    Similar to fingerprint scanner experience - keeps checking until 100% quality
    """
    try:
        data = request.json or {}
        img_data = (data.get("image") or "").split(",")[-1]

        if not img_data:
            return jsonify({
                "success": False,
                "quality_score": 0,
                "message": "No image provided"
            })

        # Decode image
        img_bytes = base64.b64decode(img_data)
        nparr = np.frombuffer(img_bytes, np.uint8)
        bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if bgr is None:
            return jsonify({
                "success": False,
                "quality_score": 0,
                "message": "Image decode failed"
            })

        sleeping = is_sleep_mode()
        if sleeping:
            gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
            faces = SLEEP_FACE_CASCADE.detectMultiScale(gray, 1.1, 4)
            detected = len(faces) > 0
            if detected:
                mark_activity()
                sleeping = is_sleep_mode()
            if sleeping:
                return jsonify({
                    "success": False,
                    "quality_score": 0,
                    "face_detected": detected,
                    "message": "sleep_mode",
                    "sleep_mode": True,
                    "ready_to_capture": False
                })

        # Convert to RGB for face_recognition
        frame = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

        # Check quality (with face-targeted AF on the streaming camera)
        quality_checker = get_quality_checker()
        result = quality_checker.check_face_quality(frame)

        # Trigger face-targeted autofocus on the streaming camera
        if result.get('face_box') and picam2_instance is not None:
            try:
                _trigger_face_af_on_stream(result['face_box'])
            except Exception:
                pass

        if result.get("success"):
            mark_activity()

        # Convert encoding to serializable format if present
        # Convert encoding to serializable format if present
        if result.get('encoding') is not None:
            result['encoding'] = result['encoding'].tolist()
 
        # ── PERFORMANCE FIX: mark activity so idle watchdog doesn't fire ──
        if result.get('success') or result.get('ready_to_capture'):
            mark_activity()
 
        safe_result = _json_safe_payload(result)
        return jsonify(safe_result)


    except Exception as e:
        print(f"[FACE_QUALITY] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "quality_score": 0,
            "message": f"Error: {str(e)}"
        })


@app.route("/api/face_register", methods=["POST"])
@require_permission("register") 
def face_register():
    mark_activity()  
    data     = request.json or {}
    raw_img  = data.get("image") or ""
    img_data = raw_img.split(",", 1)[-1]  # safe split on first comma only
    emp_id   = (data.get("employee_id") or data.get("emp_id") or "").strip()
    name     = (data.get("name") or "").strip()
    role     = (data.get("role") or "User").strip()

    print(f"[FACE_REG] Registration attempt: emp_id={emp_id}, name={name}, role={role}")

    # ── Basic validation ───────────────────────────────────────────────────
    if not img_data:
        return jsonify({"success": False, "message": "Image is required"}), 400
    if not emp_id:
        return jsonify({"success": False, "message": "Employee ID is required"}), 400

    # ── Ensure storage directories exist ──────────────────────────────────
    try:
        os.makedirs(ENCODING_DIR, exist_ok=True)
        os.makedirs(IMAGE_DIR, exist_ok=True)
    except Exception as e:
        return jsonify({"success": False, "message": f"Server storage error: {e}"}), 500

    # ── Already registered check (file-based) ─────────────────────────────
    # NOTE: We do NOT block here for same emp_id — the duplicate face check
    # below will handle same-emp_id re-registration by deleting old files first.
    # We only hard-block here if called without going through quality check
    # (enc_file existence will be cleared by duplicate check when emp_id matches).
    enc_file = os.path.join(ENCODING_DIR, f"{emp_id}.dat")
    _enc_file_existed_before = os.path.isfile(enc_file)
    # (file existence is checked again after duplicate logic cleans it up)

    # ── Decode base64 image ────────────────────────────────────────────────
    try:
        img_bytes = base64.b64decode(img_data)
        if len(img_bytes) == 0:
            return jsonify({"success": False, "message": "Decoded image is empty"}), 400

        nparr = np.frombuffer(img_bytes, np.uint8)
        bgr   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if bgr is None:
            return jsonify({"success": False, "message": "Image decode failed — invalid or corrupted image"}), 400

        print(f"[FACE_REG] Raw image shape: {bgr.shape}")

        # ── AUTO-FIX 1: Resize if image is too small for face detection ───
       # ── CAP resolution before processing ──────────────────────────────────
        h, w = bgr.shape[:2]
        if w > 640 or h > 480:
            scale = min(640 / w, 480 / h)
            bgr = cv2.resize(bgr, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
            print(f"[FACE_REG] Image downscaled to {bgr.shape[1]}x{bgr.shape[0]}")

        # ── AUTO-FIX 2: Enhance brightness/contrast if image is too dark ──
        gray     = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        mean_val = np.mean(gray)
        print(f"[FACE_REG] Image mean brightness: {mean_val:.1f}")
        if mean_val < 80:
            alpha = 1.5   # contrast boost
            beta  = 40    # brightness boost
            bgr   = cv2.convertScaleAbs(bgr, alpha=alpha, beta=beta)
            print(f"[FACE_REG] Image brightness enhanced (was too dark)")

        # ── AUTO-FIX 3: Convert to RGB for face_recognition library ───────
        frame = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

        # ── AUTO-FIX 4: Sharpen face region to recover blur ──────────────
        # Detect face first, then sharpen only the face ROI
        try:
            from face_quality_checker import FaceQualityChecker
            temp_locs = face_recognition.face_locations(frame, model='hog')
            if temp_locs:
                largest = max(temp_locs, key=lambda b: (b[2]-b[0])*(b[3]-b[1]))
                frame = FaceQualityChecker.sharpen_full_frame(frame, largest)
                print(f"[FACE_REG] Applied face sharpening to registration image")
        except Exception as sharp_err:
            print(f"[FACE_REG] Sharpening skipped: {sharp_err}")

    except Exception as e:
        print(f"[FACE_REG] Image decode exception: {e}")
        return jsonify({"success": False, "message": f"Image decode failed: {e}"}), 400

    # ── Try face detection with multiple models ────────────────────────────
    # First try fast HOG model, fallback to accurate CNN model
    # ── Face detection — HOG only (fast, same as login) ───────────────────
    encoding_arr = None
    precomputed = data.get("precomputed_encoding")
 
    if precomputed and isinstance(precomputed, list) and len(precomputed) == 128:
        # Use the encoding computed during quality check — no re-detection needed
        encoding_arr = np.array(precomputed, dtype=np.float32)
        print(f"[FACE_REG] Using precomputed encoding from quality check (fast path)")
    else:
        # Fallback: detect + encode now (slower path, used if encoding not passed)
        print(f"[FACE_REG] No precomputed encoding — detecting face with HOG model")
        try:
            ok, result = recognizer.save_face(frame)
            if ok:
                encoding_arr = result
            else:
                return jsonify({
                    "success": False,
                    "message": (
                        "No face detected. Ensure: (1) face is clearly visible and centered, "
                        "(2) good lighting with no shadows, "
                        "(3) no sunglasses or mask, "
                        "(4) camera is not too far away."
                    )
                }), 400
        except Exception as e:
            print(f"[FACE_REG] save_face exception: {e}")
            return jsonify({"success": False, "message": f"Face detection error: {e}"}), 500


    if encoding_arr is None:
        return jsonify({
            "success": False,
            "message": (
                "No face detected in the image. "
                "Please ensure: (1) face is clearly visible and centered, "
                "(2) good lighting with no shadows, "
                "(3) no sunglasses or mask, "
                "(4) camera is not too far away."
            )
        }), 400

    # ── Convert encoding to bytes ──────────────────────────────────────────
    try:
        encoding_bytes = encoding_arr.astype(np.float32).tobytes()
        print(f"[FACE_REG] Encoding size: {len(encoding_bytes)} bytes")
    except Exception as e:
        return jsonify({"success": False, "message": f"Encoding conversion failed: {e}"}), 500

    # # ── Reload encodings ONCE (for duplicate check) ────────────────────────
    # try:
    #     recognizer.load_all_encodings()
    # except Exception as e:
    #     print(f"[FACE_REG] load_all_encodings warning: {e}")

   # ── Duplicate face check ───────────────────────────────────────────────
    try:
        if encoding_arr is not None and len(recognizer.encodings) > 0:
            # Fast duplicate check: compare encoding directly without re-detecting
            query = encoding_arr.astype(np.float32)
            encs = np.vstack([e.astype(np.float32) for e in recognizer.encodings])
            diffs = encs - query
            dists = np.sqrt((diffs * diffs).sum(axis=1))
            idx = int(np.argmin(dists))
            if dists[idx] < 0.25:  # Strict duplicate threshold
                dup_emp_id = recognizer.ids[idx]

                # ── SAME emp_id: allow re-registration (face update) ──────
                if str(dup_emp_id).strip() == str(emp_id).strip():
                    print(f"[FACE_REG] Same emp_id face update allowed: {emp_id}")
                    # Remove old encoding/image so save_encoding won't find existing file
                    try:
                        delete_encoding(emp_id)
                        delete_image(emp_id)
                    except Exception as _del_err:
                        print(f"[FACE_REG] Cleanup before re-register warning: {_del_err}")
                    # Remove the "already registered" enc_file guard so code continues
                    # (enc_file check is BEFORE this point, so we need to reset the file)
                    # Fall through — code below will save new files and update DB
                else:
                    # ── DIFFERENT emp_id: deny + play access denied ───────
                    conn_dup = get_db_connection()
                    dup_row = conn_dup.execute(
                        "SELECT name FROM users WHERE emp_id=?", (dup_emp_id,)
                    ).fetchone()
                    conn_dup.close()
                    dup_name = dup_row["name"] if dup_row else dup_emp_id
                    # Trigger access denied audio + red LED in background
                    threading.Thread(
                        target=trigger_effects,
                        args=('denied',),
                        daemon=True
                    ).start()
                    return jsonify({
                        "success": False,
                        "duplicate": True,
                        "duplicate_emp_id": str(dup_emp_id),
                        "message": f"This face is already registered with Employee ID: {dup_emp_id} ({dup_name}). Registration denied."
                    }), 400
        else:
            # Fallback to standard find_duplicate if no precomputed encoding
            match = recognizer.find_duplicate(frame)
            if match:
                dup_emp_id = match.get('emp_id', '')
                dup_name = match.get('name', '')

                if str(dup_emp_id).strip() == str(emp_id).strip():
                    print(f"[FACE_REG] Same emp_id face update allowed (fallback): {emp_id}")
                    try:
                        delete_encoding(emp_id)
                        delete_image(emp_id)
                    except Exception as _del_err:
                        print(f"[FACE_REG] Cleanup before re-register warning: {_del_err}")
                else:
                    threading.Thread(
                        target=trigger_effects,
                        args=('denied',),
                        daemon=True
                    ).start()
                    return jsonify({
                        "success": False,
                        "duplicate": True,
                        "duplicate_emp_id": str(dup_emp_id),
                        "message": f"This face is already registered with Employee ID: {dup_emp_id} ({dup_name}). Registration denied."
                    }), 400
    except Exception as e:
        print(f"[FACE_REG] find_duplicate warning (non-fatal): {e}")

    # ── Re-check file guard after duplicate logic (handles same-emp_id update) ─
    enc_file = os.path.join(ENCODING_DIR, f"{emp_id}.dat")
    if os.path.isfile(enc_file):
        # Duplicate check didn't clear it — means it's a fresh non-duplicate block
        # (should not normally reach here, but safety net)
        pass  # allow overwrite — save_encoding will overwrite existing file

    # ── Save encoding and image files to disk ──────────────────────────────
    enc_path = None
    img_path = None
    try:
        enc_path = save_encoding(emp_id, encoding_bytes)
        if not enc_path:
            return jsonify({"success": False, "message": "Failed to save face encoding file"}), 500

        img_path = save_image(emp_id, img_bytes)
        if not img_path:
            # Cleanup orphaned encoding file
            try:
                if os.path.isfile(enc_path):
                    os.remove(enc_path)
            except Exception:
                pass
            return jsonify({"success": False, "message": "Failed to save face image file"}), 500

        print(f"[FACE_REG] Files saved — encoding={enc_path}, image={img_path}")

    except Exception as e:
        for path in [enc_path]:
            try:
                if path and os.path.isfile(path):
                    os.remove(path)
            except Exception:
                pass
        return jsonify({"success": False, "message": f"File save error: {e}"}), 500

    # ── Update database ────────────────────────────────────────────────────
    conn = get_db_connection()
    try:
        now           = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        col_rows      = conn.execute("PRAGMA table_info(users)").fetchall()
        existing_cols = {r[1] for r in col_rows}

        conn.execute("INSERT OR IGNORE INTO users (emp_id) VALUES (?)", (emp_id,))

        updates = []
        params  = []

        if "encoding_path" in existing_cols:
            updates.append("encoding_path = ?");           params.append(enc_path)
        if "image_path" in existing_cols:
            updates.append("image_path = ?");              params.append(img_path)
        if "name" in existing_cols and name:
            updates.append("name = ?");                    params.append(name)
        if "role" in existing_cols:
            updates.append("role = ?");                    params.append(role)
        if "updated_at" in existing_cols:
            updates.append("updated_at = ?");              params.append(now)
        if "created_at" in existing_cols:
            updates.append("created_at = COALESCE(created_at, ?)"); params.append(now)

        if updates:
            params.append(emp_id)
            conn.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE emp_id = ?",
                params
            )

        conn.commit()
        print(f"[FACE_REG] DB updated for emp_id={emp_id}")

    except Exception as e:
        print(f"[FACE_REG] DB update error (non-fatal): {e}")
    finally:
        conn.close()

    # ── Reload recognizer with new encoding ───────────────────────────────
    try:
        recognizer.load_all_encodings()
    except Exception as e:
        print(f"[FACE_REG] Recognizer reload warning: {e}")

    # ── Post-registration tasks in background ─────────────────────────────
    def _post_register_bg():
        if NETWORK_ENABLED:
            try:
                broadcast_user_upsert_by_emp(emp_id)
            except Exception as e:
                print(f"[MESH] Broadcast error: {e}")
            if device_sync_manager:
                try:
                    devs = device_sync_manager.get_connected_devices()
                    if devs:
                        device_sync_manager.transfer_user_template(emp_id)
                except Exception as e:
                    print(f"[SYNC] Transfer error: {e}")

    threading.Thread(target=_post_register_bg, daemon=True).start()
 
    # ── PERFORMANCE FIX: Reload encodings in background ─────────────────
    def _reload_encodings_bg():
        try:
            recognizer.load_all_encodings()
            print(f"[FACE_REG] Background encoding reload complete")
        except Exception as e:
            print(f"[FACE_REG] Background reload warning: {e}")
    threading.Thread(target=_reload_encodings_bg, daemon=True).start()
 
    is_update = _enc_file_existed_before
    return jsonify({
            "success": True,
            "message": "Face updated successfully" if is_update else "Face registered successfully",
            "emp_id": emp_id,
            "name": name,
            "is_update": is_update,
        })



# helper to broadcast face_edit specifically (sends encoding+image)
def _compress_image_for_transfer(image_bytes, max_kb=80):
    """
    Compress a JPEG image to fit within max_kb kilobytes.
    Reduces quality and/or resolution to minimize UDP payload size.
    Returns compressed JPEG bytes.
    """
    if not image_bytes:
        return image_bytes
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return image_bytes

        max_bytes = max_kb * 1024

        # If already small enough, return as-is
        if len(image_bytes) <= max_bytes:
            return image_bytes

        h, w = img.shape[:2]

        # Step 1: Resize if image is large (keep aspect ratio)
        max_dim = 320
        if max(h, w) > max_dim:
            scale = max_dim / max(h, w)
            img = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)

        # Step 2: Reduce JPEG quality until it fits
        for quality in (70, 55, 40, 25, 15):
            ok, buf = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, quality])
            if ok and len(buf) <= max_bytes:
                print(f"[FACE-COMPRESS] {len(image_bytes)}B -> {len(buf)}B (q={quality})")
                return buf.tobytes()

        # Fallback: return lowest quality attempt
        ok, buf = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 10])
        if ok:
            print(f"[FACE-COMPRESS] {len(image_bytes)}B -> {len(buf)}B (q=10, fallback)")
            return buf.tobytes()

        return image_bytes
    except Exception as e:
        print(f"[FACE-COMPRESS] error: {e}")
        return image_bytes


def broadcast_face_edit(emp_id):
    """
    Broadcast face encoding and display image to all mesh peers.
    Uses chunked UDP transport for reliable delivery of large payloads.
    Image is compressed before sending to minimize network usage.
    """
    try:
        if not emp_id:
            print("[MESH] broadcast_face_edit called without emp_id")
            return False

        row = _fetch_user_row(emp_id=emp_id)
        if not row:
            print(f"[MESH] broadcast_face_edit: no user row for emp_id={emp_id}")
            return False

        # Load encoding and image from file system
        enc_bytes = load_encoding(emp_id)
        disp_bytes = load_image(emp_id)
        name = row.get("name") if row else ""

        if not enc_bytes:
            print(f"[MESH] broadcast_face_edit: no encoding found for emp_id={emp_id}")
            return False

        # Compress image for network transfer (target ~80KB max)
        disp_bytes_compressed = _compress_image_for_transfer(disp_bytes, max_kb=80)

        enc_b64 = base64.b64encode(enc_bytes).decode() if enc_bytes else ""
        disp_b64 = base64.b64encode(disp_bytes_compressed).decode() if disp_bytes_compressed else ""

        self_ip = get_self_ip()

        payload = {
            "type": "face_edit",
            "msg_id": str(uuid.uuid4()),
            "ts": time.time(),
            "from": self_ip,
            "_from": self_ip,
            "user": {
                "emp_id": emp_id,
                "name": name or "",
                "encoding_b64": enc_b64,
                "display_image_b64": disp_b64
            }
        }

        payload_size = len(json.dumps(payload, separators=(',', ':')).encode('utf-8'))
        print(f"[MESH] broadcast_face_edit emp_id={emp_id} payload_size={payload_size}B "
              f"enc={len(enc_b64)}B img={len(disp_b64)}B")

        targets = get_saved_mesh_devices()
        if targets:
            ok = send_reliable(payload, targets=targets, port=UDP_PORT)
            print(f"[MESH] broadcast_face_edit emp_id={emp_id} ok={ok} targets={targets}")
            return bool(ok)

        ok = send_reliable(payload, targets=None, port=UDP_PORT)
        print(f"[MESH] broadcast_face_edit emp_id={emp_id} broadcast_ok={ok}")
        return bool(ok)
    except Exception as e:
        import traceback
        print(f"[MESH] broadcast_face_edit error: {e}")
        traceback.print_exc()
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MESH: Broadcast fingerprint .bin template to peer devices
# Mirrors broadcast_face_edit() but sends the .bin file instead of face data
# ─────────────────────────────────────────────────────────────────────────────
def broadcast_finger_edit(emp_id: str):
    """
    Read the employee's fingerprint template and broadcast it to all
    mesh peers so their sensors get the template too.

    Checks fingerprint_bins/{emp_id}.bin first, then falls back to
    fingerprint_encodings/{emp_id}.dat.

    Payload type: "finger_edit"
    Fields: emp_id, template_id, username, template (base64 498-byte raw)
    """
    from fingerprint import TEMPLATE_SIZE
    try:
        if not emp_id:
            fp_log.error("[BROADCAST] broadcast_finger_edit: emp_id missing")
            return False

        fp_log.info(f"[BROADCAST] === broadcast_finger_edit START emp_id={emp_id} ===")

        # ── Get template_id and name from fingerprint_map ─────────────────
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT template_id, name FROM fingerprint_map WHERE emp_id=?",
                (emp_id,)
            ).fetchone()
        finally:
            conn.close()

        if not row or row["template_id"] is None:
            fp_log.error(f"[BROADCAST] No fingerprint_map entry for {emp_id}")
            return False

        template_id = int(row["template_id"])
        username    = row["name"] or ""

        # ── Read template file (try .bin first, then .dat) ────────────────
        fp_bytes = None

        bins_dir = os.path.join(os.getcwd(), "fingerprint_bins")
        bin_path = os.path.join(bins_dir, f"{emp_id}.bin")
        if os.path.isfile(bin_path):
            with open(bin_path, "rb") as f:
                fp_bytes = f.read()
            fp_log.info(f"[BROADCAST] Read {bin_path} ({len(fp_bytes)} bytes)")

        if not fp_bytes or len(fp_bytes) != TEMPLATE_SIZE:
            # Fallback to fingerprint_encodings/{emp_id}.dat
            dat_data = load_fingerprint_template(emp_id)
            if dat_data and len(dat_data) == TEMPLATE_SIZE:
                fp_bytes = dat_data
                fp_log.info(f"[BROADCAST] Fallback: read fingerprint_encodings/{emp_id}.dat "
                            f"({len(fp_bytes)} bytes)")

        if not fp_bytes or len(fp_bytes) != TEMPLATE_SIZE:
            fp_log.error(f"[BROADCAST] No valid template found for {emp_id} "
                         f"(checked .bin and .dat, got {len(fp_bytes) if fp_bytes else 0} bytes)")
            return False

        fp_b64 = base64.b64encode(fp_bytes).decode()

        # ── Build payload (same structure as apply_incoming_payload expects) ─
        payload = {
            "type":        "finger_edit",
            "msg_id":      str(uuid.uuid4()),
            "ts":          time.time(),
            "from":        get_self_ip(),
            "emp_id":      emp_id,
            "user_id":     template_id,
            "username":    username,
            "template":    fp_b64,           # base64 .bin bytes (498 bytes raw)
        }

        # ── Send to saved mesh targets or broadcast ───────────────────────
        targets = get_saved_mesh_devices()
        if targets:
            ok = send_reliable(payload, targets=targets, port=UDP_PORT)
            fp_log.info(f"[BROADCAST] Sent finger_edit emp_id={emp_id} "
                        f"slot={template_id} ok={ok} targets={targets}")
        else:
            ok = send_reliable(payload, targets=None, port=UDP_PORT)
            fp_log.info(f"[BROADCAST] Broadcast finger_edit emp_id={emp_id} "
                        f"slot={template_id} broadcast_ok={ok}")

        return bool(ok)

    except Exception as e:
        fp_log.error(f"[BROADCAST] broadcast_finger_edit error: {e}", exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MESH: Broadcast RFID card number to peer devices
# Mirrors broadcast_face_edit() but sends a single rfid_card string
# ─────────────────────────────────────────────────────────────────────────────
def broadcast_rfid_edit(emp_id: str):
    """
    Read the employee's RFID card from rfid_card_map and broadcast it
    to all mesh peers so their local DB gets updated too.

    Payload type: "rfid_edit"
    Fields: emp_id, name, rfid_card
    """
    try:
        if not emp_id:
            print("[MESH] broadcast_rfid_edit: emp_id missing")
            return False

        # ── Get rfid_card from rfid_card_map ──────────────────────────────
        conn = get_db_connection()
        try:
            row = conn.execute(
                "SELECT rfid_card, name FROM rfid_card_map WHERE emp_id=?",
                (emp_id,)
            ).fetchone()
        finally:
            conn.close()

        if not row or not row["rfid_card"]:
            print(f"[MESH] broadcast_rfid_edit: no rfid_card_map entry for {emp_id}")
            return False

        rfid_card = str(row["rfid_card"]).strip()
        name      = row["name"] or ""

        # ── Build payload ─────────────────────────────────────────────────
        payload = {
            "type":      "rfid_edit",
            "msg_id":    str(uuid.uuid4()),
            "ts":        time.time(),
            "from":      get_self_ip(),
            "emp_id":    emp_id,
            "name":      name,
            "rfid_card": rfid_card,          # the 10-digit UID string
        }

        # ── Send to saved mesh targets or broadcast ───────────────────────
        targets = get_saved_mesh_devices()
        if targets:
            ok = send_reliable(payload, targets=targets, port=UDP_PORT)
            print(f"[MESH] broadcast_rfid_edit emp_id={emp_id} "
                  f"card={rfid_card} ok={ok} targets={targets}")
        else:
            ok = send_reliable(payload, targets=None, port=UDP_PORT)
            print(f"[MESH] broadcast_rfid_edit emp_id={emp_id} "
                  f"card={rfid_card} broadcast_ok={ok}")

        return bool(ok)

    except Exception as e:
        print(f"[MESH] broadcast_rfid_edit error: {e}")
        return False
    
# Face Update
@app.route("/api/face_edit", methods=["POST"])
@require_permission("edit")
def face_edit():
    data     = request.json or {}
    img_data = (data.get("image") or "").split(",")[-1]
    emp_id   = (data.get("emp_id") or "").strip()

    # ADD THESE DEBUG LINES
    print(f"[FACE_EDIT] emp_id='{emp_id}', img_data_len={len(img_data)}")
    print(f"[FACE_EDIT] received keys: {list(data.keys())}")

    if not img_data or not emp_id:
        return jsonify({"success": False, "message": "image and emp_id required"}), 400

    # Decode image
    try:
        img_bytes = base64.b64decode(img_data)
        nparr = np.frombuffer(img_bytes, np.uint8)
        bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if bgr is None:
            return jsonify({"success": False, "message": "Image decode failed"}), 400
        frame = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
    except Exception as e:
        return jsonify({"success": False, "message": f"Image decode failed: {e}"}), 400

    # Extract encoding
    ok, result = recognizer.save_face(frame)
    if not ok:
        return jsonify({"success": False, "message": result}), 400

    encoding_arr = result
    enc_bytes = encoding_arr.astype(np.float32).tobytes()

    # ── Save files to disk ONLY — no BLOBs written to DB ──────────────────
    enc_path = None
    img_path = None
    try:
        enc_path = save_encoding(emp_id, enc_bytes)
        img_path = save_image(emp_id, img_bytes)
        print(f"[FACE_EDIT] Files saved: encoding={enc_path}, image={img_path}")
    except Exception as e:
        return jsonify({"success": False, "message": f"File save error: {e}"}), 500

    # ── Update DB: file paths only, no BLOBs, no template_id ──────────────
    conn = get_db_connection()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        col_rows = conn.execute("PRAGMA table_info(users)").fetchall()
        existing_cols = {r[1] for r in col_rows}

        updates = []
        params  = []
        if "encoding_path" in existing_cols and enc_path:
            updates.append("encoding_path = ?"); params.append(enc_path)
        if "image_path" in existing_cols and img_path:
            updates.append("image_path = ?"); params.append(img_path)
        if "updated_at" in existing_cols:
            updates.append("updated_at = ?"); params.append(now)

        if updates:
            params.append(emp_id)
            conn.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE emp_id = ?",
                params
            )
            conn.commit()
            print(f"[FACE_EDIT] DB updated for emp_id={emp_id}")
    except Exception as e:
        print(f"[FACE_EDIT] DB update error: {e}")
    finally:
        conn.close()

    # Reload recognizer
    try:
        recognizer.load_all_encodings()
    except Exception as e:
        print(f"[FACE_EDIT] reload error: {e}")

    # Broadcast to mesh (non-blocking)
    def _broadcast():
        try:
            broadcast_face_edit(emp_id)
        except Exception as e:
            print(f"[MESH] face_edit broadcast error: {e}")
    threading.Thread(target=_broadcast, daemon=True).start()

    return jsonify({
        "success": True,
        "message": "Face updated successfully",
        "emp_id": emp_id,
        "encoding_path": enc_path,
        "image_path": img_path
    })

# -----------------------------------------------------------------------------
# Avatar/image fetch for popup (success-only avatar hydration)
# -----------------------------------------------------------------------------
def _generate_initials_avatar_b64(text: str, size=120):
    t = "".join([ch for ch in text if ch.isalnum()]) or "?"
    initials = (t[:1] + (t[1:2] if len(t) > 1 else "")).upper()
    bg  = (230, 240, 255)  # BGR light background
    fg  = (40, 60, 90)     # BGR dark text
    img = np.full((size, size, 3), bg, dtype=np.uint8)
    cv2.circle(img, (size//2, size//2), int(size*0.49), bg, thickness=-1)
    font = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = 1.8 if len(initials) == 1 else 1.4
    thickness  = 3
    (tw, th), _ = cv2.getTextSize(initials, font, font_scale, thickness)
    x = (size - tw) // 2
    y = (size + th) // 2 - 6
    cv2.putText(img, initials, (x, y), font, font_scale, fg, thickness, cv2.LINE_AA)
    ok, enc = cv2.imencode(".png", img)
    return base64.b64encode(enc if ok else b"").decode("utf-8")

@app.route("/api/get_user_image", methods=["POST"])
def get_user_image():
    """
    Get user image for display in popups/banners.
    Returns base64 PNG image (120x120).

    FIX: Loads from database paths first:
    1. users.image_path (primary source from DB)
    2. users_img/{emp_id}.jpg (fallback)
    3. Initials avatar (last resort)
    """
    try:
        data = request.get_json(force=True) or {}
        emp_id = (data.get("emp_id") or "").strip()
        if not emp_id:
            img_b64 = _generate_initials_avatar_b64("?", size=120)
            return jsonify({"success": False, "image": f"data:image/png;base64,{img_b64}"})

        conn = get_db_connection()
        row = conn.execute("SELECT name, role, birthdate, image_path, encoding_path FROM users WHERE emp_id=?", (emp_id,)).fetchone()
        conn.close()

        # Prepare user info
        user_name = row["name"] if row else "N/A"
        user_role = row["role"] if row else "User"
        user_birthdate = row["birthdate"] if row else "N/A"

        # Try to load image from file system
        img_bytes = None

        # First try: Load from image_path in DB (sqlite3.Row uses [] not .get())
        if row and row["image_path"]:
            image_path = row["image_path"]
            try:
                p = Path(image_path)
                if not p.is_absolute():
                    p = Path.cwd() / image_path
                if p.exists():
                    img_bytes = p.read_bytes()
                    print(f"[get_user_image] Loaded from image_path: {image_path}")
            except Exception as e:
                print(f"[get_user_image] Failed to load from image_path {image_path}: {e}")

        # Second try: Load from standard location
        if not img_bytes:
            img_bytes = load_image(emp_id)
            if img_bytes:
                print(f"[get_user_image] Loaded from users_img/{emp_id}.jpg")

        # Process image if found
        if img_bytes:
            try:
                nparr = np.frombuffer(img_bytes, np.uint8)
                bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if bgr is not None:
                    bgr = cv2.resize(bgr, (120, 120), interpolation=cv2.INTER_AREA)
                    ok, enc = cv2.imencode(".png", bgr)
                    if ok:
                        return jsonify({
                            "success": True,
                            "image": "data:image/png;base64," + base64.b64encode(enc).decode("utf-8"),
                            "name": user_name,
                            "role": user_role,
                            "birthdate": user_birthdate
                        })
                    else:
                        print(f"[get_user_image] cv2.imencode failed for {emp_id}")
                else:
                    print(f"[get_user_image] cv2.imdecode returned None for {emp_id}")
            except Exception as e:
                print(f"[get_user_image] Image processing error for {emp_id}: {e}")

        # Fall back to initials avatar
        initials = (row["name"] or emp_id) if row else emp_id
        img_b64 = _generate_initials_avatar_b64(initials, size=120)
        print(f"[get_user_image] Using initials avatar for {emp_id}")
        return jsonify({
            "success": False,
            "image": f"data:image/png;base64,{img_b64}",
            "name": user_name,
            "role": user_role,
            "birthdate": user_birthdate
        })
    except Exception as e:
        print(f"[get_user_image] Fatal error: {e}")
        img_b64 = _generate_initials_avatar_b64("?", size=120)
        return jsonify({"success": False, "image": f"data:image/png;base64,{img_b64}"})


# -----------------------------------------------------------------------------
# Fingerprint APIs
# -----------------------------------------------------------------------------
fingerprint_sensor = None
sensor_lock = Lock()
sensor_error_count = 0
sensor_last_reset = time.time()
_sensor_permanently_open = False

def _auto_serial_port(candidates=("/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyACM0", "/dev/ttyACM1")):
    for p in candidates:
        if os.path.exists(p):
            return p
    for p in glob.glob("/dev/ttyUSB*") + glob.glob("/dev/ttyACM*"):
        return p
    return "/dev/ttyUSB0"


def get_fingerprint_sensor():
    global fingerprint_sensor, sensor_error_count, sensor_last_reset
    global _sensor_permanently_open

    # ── Auto-reset after too many consecutive errors ──────────────────────
    if sensor_error_count > 10 and (time.time() - sensor_last_reset) > 30:
        print("[SELF-REPAIR] Resetting fingerprint sensor due to excessive errors")
        try:
            if fingerprint_sensor:
                fingerprint_sensor.force_reset()
        except Exception:
            pass
        fingerprint_sensor = None
        _sensor_permanently_open = False
        sensor_error_count = 0
        sensor_last_reset = time.time()

    # ── Create instance if needed ─────────────────────────────────────────
    if fingerprint_sensor is None:
        try:
            port = _auto_serial_port()
            fingerprint_sensor = Fingerprint(port, 9600)
            _sensor_permanently_open = False
            print(f"[FP] New Fingerprint instance on {port}")
        except Exception as e:
            sensor_error_count += 1
            raise Exception(f"Failed to create Fingerprint instance: {e}")

    # ── Ensure Open command sent once (persistent connection) ─────────────
    if not _sensor_permanently_open:
        try:
            ok = fingerprint_sensor.init()   # opens serial + basic handshake
            if not ok:
                sensor_error_count += 1
                raise RuntimeError("Sensor did not respond to init()")

            fingerprint_sensor.open()        # activate sensor
            fingerprint_sensor._flush()      # clear buffers

            _sensor_permanently_open = True
            sensor_error_count = 0
            print("[FP] Sensor Open — persistent connection active")

        except Exception as e:
            sensor_error_count += 1
            fingerprint_sensor = None
            _sensor_permanently_open = False
            print(f"[FP] get_fingerprint_sensor failed (err={sensor_error_count}): {e}")
            raise

    return fingerprint_sensor

def call_with_timeout(func, timeout=30):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            return False, "Operation timed out. Please try again."


# ─────────────────────────────────────────────────────────────────────────────
# SHARED: receive_fingerprint_template()
# Called by BOTH "finger_edit" and "template_transfer" handlers to inject
# a fingerprint template into the local sensor + DB + files.
# ─────────────────────────────────────────────────────────────────────────────
def receive_fingerprint_template(emp_id, fp_bytes, username=""):
    """
    Receive a fingerprint template from a remote device and inject into local sensor.

    Logic:
    1. If emp_id already has a fingerprint registered locally → SKIP (return success)
    2. Find the lowest available template_id on THIS device's sensor
    3. Save template to fingerprint_encodings/{emp_id}.dat AND fingerprint_bins/{emp_id}.bin
    4. Update fingerprint_map, fingerprints in DB (user_finger_map is optional)
    5. Inject into sensor using set_template() with correct GT-521F52 protocol

    IMPORTANT: Sensor injection (Step 5) ALWAYS runs even if a non-critical DB table
    is missing. Only critical failures (fingerprint_map) abort the process.

    Args:
        emp_id: Employee ID
        fp_bytes: Raw 498-byte fingerprint template data
        username: Employee name (optional)

    Returns:
        (success: bool, template_id: int or None, message: str)
    """
    from fingerprint import TEMPLATE_SIZE

    fp_log.info(f"[RECEIVE] === receive_fingerprint_template START emp_id={emp_id} "
                f"fp_bytes_len={len(fp_bytes) if fp_bytes else 0} ===")

    target_template_id = None

    try:
        if not emp_id or not fp_bytes:
            msg = f"Missing emp_id or fp_bytes (emp_id={emp_id}, bytes={len(fp_bytes) if fp_bytes else 0})"
            fp_log.error(f"[RECEIVE] {msg}")
            return False, None, msg

        if len(fp_bytes) != TEMPLATE_SIZE:
            msg = f"Bad template size {len(fp_bytes)} (expected {TEMPLATE_SIZE}) for {emp_id}"
            fp_log.error(f"[RECEIVE] {msg}")
            return False, None, msg

        # ── Step 1: Check if employee already has fingerprint registered ────
        conn = get_db_connection()
        try:
            # Ensure required tables exist on this device
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fingerprint_map(
                    emp_id TEXT PRIMARY KEY,
                    template_id INTEGER UNIQUE NOT NULL,
                    name TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fingerprints(
                    id INTEGER PRIMARY KEY,
                    username TEXT,
                    template BLOB
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_finger_map(
                    emp_id TEXT PRIMARY KEY,
                    template_id INTEGER
                )
            """)
            conn.commit()

            existing = conn.execute(
                "SELECT template_id FROM fingerprint_map WHERE emp_id=?",
                (emp_id,)
            ).fetchone()

            if existing and existing["template_id"] is not None:
                existing_tid = int(existing["template_id"])
                msg = f"Employee {emp_id} already has fingerprint at slot {existing_tid} — SKIPPED"
                fp_log.info(f"[RECEIVE] {msg}")
                return True, existing_tid, msg

            # ── Step 2: Find lowest available template_id ───────────────────
            used_ids = set()
            for r in conn.execute(
                "SELECT template_id FROM fingerprint_map WHERE template_id IS NOT NULL"
            ):
                try:
                    used_ids.add(int(r[0]))
                except Exception:
                    pass

            target_template_id = None
            for tid in range(1, 3001):
                if tid not in used_ids:
                    target_template_id = tid
                    break

            if target_template_id is None:
                msg = f"No free template slots for {emp_id} (all 3000 used)"
                fp_log.error(f"[RECEIVE] {msg}")
                return False, None, msg

            fp_log.info(f"[RECEIVE] Assigned slot {target_template_id} for {emp_id}")

            # ── Step 3: Save template files ─────────────────────────────────
            # Save to fingerprint_encodings/{emp_id}.dat (for fingerprint_helper)
            try:
                save_fingerprint_template(emp_id, fp_bytes, target_template_id, username)
                fp_log.info(f"[RECEIVE] Saved fingerprint_encodings/{emp_id}.dat")
            except Exception as e:
                fp_log.error(f"[RECEIVE] Error saving .dat file: {e}")

            # Save to fingerprint_bins/{emp_id}.bin (for startup loading)
            bins_dir = os.path.join(os.getcwd(), "fingerprint_bins")
            os.makedirs(bins_dir, exist_ok=True)
            bin_path = os.path.join(bins_dir, f"{emp_id}.bin")
            try:
                with open(bin_path, "wb") as f:
                    f.write(fp_bytes)
                fp_log.info(f"[RECEIVE] Saved fingerprint_bins/{emp_id}.bin")
            except Exception as e:
                fp_log.error(f"[RECEIVE] Error saving .bin file: {e}")

            # ── Step 4: Update database tables ──────────────────────────────
            # Each table update is separate so one failure doesn't block the rest
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            db_ok = True

            # 4a: fingerprint_map (CRITICAL — must succeed)
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO fingerprint_map
                       (emp_id, template_id, name, created_at, updated_at)
                       VALUES (?, ?, ?, COALESCE(
                           (SELECT created_at FROM fingerprint_map WHERE emp_id=?),
                           ?), ?)""",
                    (emp_id, target_template_id, username or "",
                     emp_id, now, now)
                )
                conn.commit()
                fp_log.info(f"[RECEIVE] DB: fingerprint_map OK "
                            f"emp_id={emp_id} slot={target_template_id}")
            except Exception as e:
                fp_log.error(f"[RECEIVE] DB: fingerprint_map FAILED: {e}")
                db_ok = False

            # 4b: fingerprints table (stores raw blob)
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO fingerprints (id, username, template) VALUES (?, ?, ?)",
                    (target_template_id, username or "", fp_bytes)
                )
                conn.commit()
                fp_log.info(f"[RECEIVE] DB: fingerprints OK slot={target_template_id}")
            except Exception as e:
                fp_log.error(f"[RECEIVE] DB: fingerprints FAILED: {e}")

            # 4c: user_finger_map (optional cross-reference)
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO user_finger_map (emp_id, template_id) VALUES (?, ?)",
                    (emp_id, target_template_id)
                )
                conn.commit()
                fp_log.info(f"[RECEIVE] DB: user_finger_map OK")
            except Exception as e:
                fp_log.warning(f"[RECEIVE] DB: user_finger_map skipped: {e}")

            if not db_ok:
                fp_log.error(f"[RECEIVE] Critical DB update (fingerprint_map) failed — "
                             f"continuing to sensor injection anyway")

        finally:
            conn.close()

        # ── Step 5: Inject template into local sensor ───────────────────
        # This ALWAYS runs regardless of DB errors above
        fp_log.info(f"[RECEIVE] Injecting into sensor slot {target_template_id} for {emp_id}...")
        inject_ok = False
        try:
            s = get_fingerprint_sensor()
            with sensor_lock:
                s.open()
                try:
                    s._flush()
                    inject_ok = s.set_template(target_template_id, fp_bytes)
                    if inject_ok:
                        fp_log.info(f"[RECEIVE] SENSOR INJECT OK: emp_id={emp_id} "
                                    f"slot={target_template_id}")
                    else:
                        fp_log.error(f"[RECEIVE] SENSOR INJECT FAILED: emp_id={emp_id} "
                                     f"slot={target_template_id} — set_template returned False")
                finally:
                    s._flush()
        except Exception as e:
            fp_log.error(f"[RECEIVE] SENSOR INJECT EXCEPTION: emp_id={emp_id} "
                         f"slot={target_template_id} error={e}")

        if inject_ok:
            msg = (f"Fingerprint received and injected: emp_id={emp_id} "
                   f"slot={target_template_id}")
            fp_log.info(f"[RECEIVE] === SUCCESS {msg} ===")
            return True, target_template_id, msg
        else:
            msg = (f"Template saved to files/DB but sensor injection FAILED: "
                   f"emp_id={emp_id} slot={target_template_id}")
            fp_log.warning(f"[RECEIVE] === PARTIAL {msg} ===")
            return False, target_template_id, msg

    except Exception as e:
        msg = f"receive_fingerprint_template error: {e}"
        fp_log.error(f"[RECEIVE] === EXCEPTION {msg} ===", exc_info=True)
        return False, None, msg


# ── Device Console: wire up post-import reload and fingerprint inject ────────
def reload_biometrics_after_device_console_import():
    """Called by DeviceConsoleService after users/biometrics are imported."""
    try:
        recognizer.load_all_encodings()
        print("[DEVICECONSOLE] Face encodings reloaded")
    except Exception as e:
        print(f"[DEVICECONSOLE] Face encoding reload failed: {e}")
    try:
        load_fingerprint_templates_on_startup()
        print("[DEVICECONSOLE] Fingerprint template reload started")
    except Exception as e:
        print(f"[DEVICECONSOLE] Fingerprint template reload failed: {e}")

if device_console_service is not None:
    # Set the post-import reload callback
    device_console_service.post_import_reload = reload_biometrics_after_device_console_import
    # Wire up sensor inject callback so Device Console can inject fingerprints into sensor
    device_console_service.set_fingerprint_inject_callback(receive_fingerprint_template)
    print("[DEVICECONSOLE] Callbacks wired: post_import_reload + fingerprint_inject")


# --- replace the whole /api/finger_register in app.py with this ---



def template_to_empname(template_id: int):
    """
    Map sensor template slot -> (emp_id, name).
    ONLY checks fingerprint_map (single source of truth).
    Always returns strings; falls back to showing the slot if no mapping yet.
    """
    template_id = int(template_id)
    emp_id, name = None, None

    conn = get_db_connection()
    try:
        r = conn.execute(
            "SELECT emp_id, COALESCE(name,'') as name FROM fingerprint_map WHERE template_id=?",
            (template_id,)
        ).fetchone()
        if r:
            emp_id, name = r['emp_id'], r['name']
    finally:
        conn.close()

    if not emp_id:
        emp_id = str(template_id)  # last resort label
    return str(emp_id), (name or "")

MAX_FP_TEMPLATES = 3000


def _call_first(obj, names, *args, **kwargs):
    for name in names:
        fn = getattr(obj, name, None)
        if callable(fn):
            try:
                r = fn(*args, **kwargs)
                if isinstance(r, tuple) and len(r) >= 1 and isinstance(r[0], bool):
                    return r[0], (r[1] if len(r) > 1 else "")
                return (bool(r), str(r))
            except Exception:
                continue
    return False, " / ".join(names) + " not available"

def _pick_first_free_template_id(conn, sensor):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fingerprint_map(
            emp_id TEXT PRIMARY KEY,
            template_id INTEGER UNIQUE NOT NULL,
            name TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    rows = conn.execute("SELECT template_id FROM fingerprint_map").fetchall()
    used = {int(r[0]) for r in rows if r and r[0] is not None}

    def dev_used(tid: int) -> bool:
        ok, _ = _call_first(sensor, ["is_enrolled","IsEnrolled","check_enrolled","CheckEnrolled"], tid)
        if ok:
            try:
                probe = getattr(sensor, "is_enrolled", None) or getattr(sensor, "IsEnrolled", None) \
                        or getattr(sensor, "check_enrolled", None) or getattr(sensor, "CheckEnrolled", None)
                if callable(probe):
                    return bool(probe(tid))
            except Exception:
                pass
        return False

    # Find first free template ID in ascending order (lowest available first)
    # This ensures consistent incremental assignment
    for tid in range(1, MAX_FP_TEMPLATES + 1):
        if tid in used:
            continue
        try:
            if dev_used(tid):
                continue
        except Exception:
            pass
        return tid
    return None
    
@app.route("/api/finger_register", methods=["POST"])
@require_permission("finger_edit")
def api_finger_register():
    """
    Fingerprint enrollment endpoint with register + update logic.
    
    Behavior:
    - If emp_id has no fingerprint → fresh enrollment
    - If emp_id has a fingerprint AND new scan matches it → "already registered" message
    - If emp_id has a fingerprint AND new scan is different → delete old, re-enroll with same template_id
    - If new scan matches a DIFFERENT emp_id → block with duplicate message
    """
    try:
        data = request.get_json(silent=True) or {}
        emp_id = (data.get("emp_id") or data.get("employee_id") or "").strip()
        username = (data.get("username") or data.get("name") or "").strip()

        raw_user_id = data.get("user_id")
        if raw_user_id is not None and str(raw_user_id).strip():
            emp_id = str(raw_user_id).strip()

        if not emp_id:
            return jsonify(success=False, message="emp_id or user_id required"), 400

        conn = get_db_connection()
        try:
            # Ensure fingerprint_map table exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fingerprint_map(
                    emp_id TEXT PRIMARY KEY,
                    template_id INTEGER UNIQUE NOT NULL,
                    name TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """)

            # ------------------------------------------------------------------
            # Get user info from users table
            # ------------------------------------------------------------------
            user_info = conn.execute(
                "SELECT name, created_at FROM users WHERE emp_id=?",
                (emp_id,)
            ).fetchone()

            if user_info:
                if not username:
                    username = user_info['name'] or ""
                user_created_at = user_info['created_at'] or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                user_created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # ------------------------------------------------------------------
            # Check if this emp_id already has a fingerprint registered
            # ------------------------------------------------------------------
            existing_fp_row = conn.execute(
                "SELECT template_id, name FROM fingerprint_map WHERE emp_id=?",
                (emp_id,)
            ).fetchone()

            existing_template_id = None
            is_update_mode = False

            if existing_fp_row and fingerprint_template_exists(emp_id):
                existing_template_id = int(existing_fp_row["template_id"])
                is_update_mode = True
                print(f"[FINGERPRINT] Update mode for emp_id={emp_id}, existing template_id={existing_template_id}")
            elif existing_fp_row and not fingerprint_template_exists(emp_id):
                # Stale DB record with no file — clean up and treat as fresh enrollment
                conn.execute("DELETE FROM fingerprint_map WHERE emp_id=?", (emp_id,))
                conn.commit()
                print(f"[FINGERPRINT] Cleaned stale fingerprint_map record for emp_id={emp_id}")

            # ------------------------------------------------------------------
            # Open sensor and perform enrollment
            # ------------------------------------------------------------------
            s = get_fingerprint_sensor()
            with sensor_lock:
                s.open()
                try:
                    _call_first(s, ["_flush", "flush"])

                    # ----------------------------------------------------------
                    # Step 1: Capture a preliminary scan for duplicate checking
                    # ----------------------------------------------------------
                    print(f"[FINGERPRINT] Capturing preliminary scan for duplicate check...")
                    ok_cap, _ = _call_first(
                        s, ["capture_finger", "CaptureFinger", "capture", "scan", "getImage"]
                    )

                    if ok_cap:
                        # Run 1:N search against all enrolled templates
                        ok_ident, ident_val = _call_first(
                            s, ["identify", "Identify", "search", "Search"]
                        )

                        if ok_ident and isinstance(ident_val, int) and ident_val >= 0:
                            matched_template_id = int(ident_val)

                            # Find which emp_id owns this matched template
                            matched_row = conn.execute(
                                "SELECT emp_id FROM fingerprint_map WHERE template_id=?",
                                (matched_template_id,)
                            ).fetchone()

                            matched_emp_id = matched_row["emp_id"] if matched_row else None

                            if matched_emp_id:
                                if str(matched_emp_id).strip() == str(emp_id).strip():
                                    # ------------------------------------------
                                    # Same emp_id: fingerprint already registered
                                    # ------------------------------------------
                                    print(f"[FINGERPRINT] Same emp_id fingerprint detected for emp_id={emp_id}")
                                    return jsonify(
                                        success=False,
                                        duplicate=True,
                                        same_user=True,
                                        emp_id=emp_id,
                                        message=f"This fingerprint is already registered with Employee ID: {emp_id}. "
                                                f"To update, place a different finger."
                                    ), 200
                                else:
                                    # ------------------------------------------
                                    # Different emp_id: block duplicate
                                    # ------------------------------------------
                                    # Look up name for a friendlier message
                                    dup_user = conn.execute(
                                        "SELECT name FROM users WHERE emp_id=?",
                                        (matched_emp_id,)
                                    ).fetchone()
                                    dup_name = dup_user["name"] if dup_user else matched_emp_id

                                    print(f"[FINGERPRINT] Duplicate fingerprint: belongs to emp_id={matched_emp_id}")
                                    return jsonify(
                                        success=False,
                                        duplicate=True,
                                        same_user=False,
                                        duplicate_emp_id=str(matched_emp_id),
                                        message=f"This fingerprint is already registered with "
                                                f"Employee ID: {matched_emp_id} ({dup_name}). "
                                                f"Registration denied."
                                    ), 200
                            else:
                                # Template exists on sensor but not in DB
                                # Could be a stale sensor slot — log and continue
                                print(f"[FINGERPRINT] Sensor matched template_id={matched_template_id} "
                                      f"but no DB record found — treating as no match")

                    # ----------------------------------------------------------
                    # Step 2: Determine target template_id
                    # ----------------------------------------------------------
                    if is_update_mode:
                        # Re-use the existing template_id slot
                        target_template_id = existing_template_id
                        print(f"[FINGERPRINT] Reusing template_id={target_template_id} for update")

                        # Delete old template from sensor before re-enrolling
                        print(f"[FINGERPRINT] Deleting old template_id={target_template_id} from sensor...")
                        try:
                            s.delete(target_template_id)
                            time.sleep(0.3)
                            _call_first(s, ["_flush", "flush"])
                            print(f"[FINGERPRINT] Old template deleted from sensor")
                        except Exception as e_del:
                            print(f"[FINGERPRINT] Warning: could not delete old sensor template: {e_del}")

                        # Delete old template file
                        try:
                            delete_fingerprint_template(emp_id)
                            print(f"[FINGERPRINT] Old template file deleted for emp_id={emp_id}")
                        except Exception as e_file:
                            print(f"[FINGERPRINT] Warning: could not delete old template file: {e_file}")

                    else:
                        # Fresh enrollment — pick first free slot
                        target_template_id = _pick_first_free_template_id(conn, s)
                        if not target_template_id:
                            return jsonify(
                                success=False,
                                message="Sensor storage full (no free template IDs)."
                            ), 409
                        print(f"[FINGERPRINT] Fresh enrollment at template_id={target_template_id}")

                    # ----------------------------------------------------------
                    # Step 3: Start enrollment
                    # ----------------------------------------------------------
                    ok_start, msg = _call_first(
                        s,
                        ["start_enroll", "EnrollStart", "enroll_start", "startEnroll"],
                        int(target_template_id)
                    )
                    if not ok_start:
                        error_msg = "Enrollment start failed. Ensure finger is not on sensor."
                        if msg and msg not in ("False", "True", "None"):
                            error_msg = f"Enroll start failed: {msg}"
                        return jsonify(success=False, message=error_msg), 200

                    # ----------------------------------------------------------
                    # Step 4: 3-step enrollment
                    # ----------------------------------------------------------
                    for step, names in enumerate(
                        (
                            ["enroll1", "Enroll1", "enroll_step1"],
                            ["enroll2", "Enroll2", "enroll_step2"],
                            ["enroll3", "Enroll3", "enroll_step3"],
                        ),
                        start=1
                    ):
                        time.sleep(2.5)
                        ok_cap, _ = _call_first(
                            s, ["capture_finger", "CaptureFinger", "capture", "scan", "getImage"]
                        )
                        if not ok_cap:
                            return jsonify(
                                success=False,
                                message=f"Capture failed at step {step}. Place finger firmly."
                            ), 200

                        ok_step, msg_step = _call_first(s, names)
                        if not ok_step:
                            step_error = f"Step {step}/3 failed. Use same finger for all steps."
                            if msg_step and msg_step not in ("False", "True", "None"):
                                step_error = f"Step {step} failed: {msg_step}"
                            return jsonify(success=False, message=step_error), 200

                    # ----------------------------------------------------------
                    # Step 5: Download enrolled template from sensor
                    # Call get_template() directly (NOT via _call_first which
                    # corrupts bytes→str). Uses corrected GT-521F52 protocol.
                    # ----------------------------------------------------------
                    tpl = None
                    try:
                        tpl = s.get_template(int(target_template_id))
                    except Exception as e_tpl:
                        fp_log.error(f"[ENROLL] get_template exception: {e_tpl}")

                    if not tpl or not isinstance(tpl, bytes):
                        fp_log.error(f"[ENROLL] get_template FAILED for slot {target_template_id} "
                                     f"(got {type(tpl).__name__}, "
                                     f"len={len(tpl) if tpl else 0})")
                        print(f"[FINGERPRINT] Template download failed for template_id={target_template_id}")
                        return jsonify(
                            success=False,
                            message="Template download failed. Please try again."
                        ), 200

                    fp_log.info(f"[ENROLL] get_template OK: slot={target_template_id} "
                                f"size={len(tpl)} bytes")

                    # ----------------------------------------------------------
                    # Step 6: Persist to file system and database
                    # ----------------------------------------------------------
                    # Save template file to fingerprint_encodings/{emp_id}.dat
                    try:
                        save_fingerprint_template(emp_id, tpl, int(target_template_id), username or None)
                        fp_log.info(f"[ENROLL] Saved fingerprint_encodings/{emp_id}.dat "
                                    f"({len(tpl)} bytes, slot={target_template_id})")
                        print(f"[FINGERPRINT] Template file saved: {emp_id}.dat "
                              f"(template_id={target_template_id})")
                    except Exception as e:
                        fp_log.error(f"[ENROLL] Error saving .dat: {e}")
                        print(f"[FINGERPRINT] Template file save error: {e}")

                    # Also save to fingerprint_bins/{emp_id}.bin (for startup loading + broadcast)
                    try:
                        _bins_dir = os.path.join(os.getcwd(), "fingerprint_bins")
                        os.makedirs(_bins_dir, exist_ok=True)
                        _bin_path = os.path.join(_bins_dir, f"{emp_id}.bin")
                        with open(_bin_path, "wb") as _bf:
                            _bf.write(tpl)
                        fp_log.info(f"[ENROLL] Saved fingerprint_bins/{emp_id}.bin ({len(tpl)} bytes)")
                    except Exception as e:
                        fp_log.error(f"[ENROLL] Error saving .bin: {e}")
                        print(f"[FINGERPRINT] .bin file save error: {e}")

                    # Upsert fingerprint_map
                    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    try:
                        conn.execute(
                            """INSERT OR REPLACE INTO fingerprint_map
                               (emp_id, template_id, name, created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?)""",
                            (emp_id, int(target_template_id), username or "", user_created_at, now)
                        )
                        conn.commit()
                        print(f"[FINGERPRINT] fingerprint_map updated: emp_id={emp_id}, "
                              f"template_id={target_template_id}")
                    except Exception as e:
                        print(f"[FINGERPRINT] fingerprint_map update error: {e}")

                    # ----------------------------------------------------------
                    # Step 7: Network broadcast via broadcast_finger_edit()
                    # (Removed dead "finger_register" broadcast — no handler exists for it)
                    # ----------------------------------------------------------
                    # Success response
                    # ----------------------------------------------------------
                    action_msg = (
                        f"Fingerprint updated successfully (Template ID {target_template_id})."
                        if is_update_mode
                        else f"Fingerprint enrolled successfully (Template ID {target_template_id})."
                    )

                    fp_log.info(f"[ENROLL] === SUCCESS emp_id={emp_id} "
                                f"slot={target_template_id} update={is_update_mode} "
                                f"template_size={len(tpl)} ===")

                    # ── Mesh broadcast (non-blocking) ─────────────────────
                    threading.Thread(
                        target=broadcast_finger_edit,
                        args=(emp_id,),
                        daemon=True
                    ).start()

                    return jsonify(
                        success=True,
                        template_id=int(target_template_id),
                        is_update=is_update_mode,
                        message=action_msg
                    ), 200

                finally:
                    try:
                        _call_first(s, ["_flush", "flush"])
                    except Exception:
                        pass
                    try:
                        s.close()
                    except Exception:
                        pass

        finally:
            conn.close()

    except Exception as e:
        print(f"[FINGERPRINT] Unexpected error: {type(e).__name__}: {e}")
        return jsonify(success=False, message=f"{type(e).__name__}: {e}"), 500

@app.route("/api/finger_identify", methods=["POST"])
def api_finger_identify():
    """
    Fingerprint login endpoint — optimised for minimum latency.

    Flow:
      1. Acquire sensor_lock
      2. Call sensor.identify()  (capture + 1:N match — ~300-600 ms on hardware)
      3. Release sensor_lock     ← JSON can now be built
      4. Fire effects in background thread (audio / LED / printer / MSSQL)
      5. Return JSON immediately

    No open()/close() per call — sensor stays persistently open.
    """
    mark_activity()

    # Result container shared between timeout thread and main thread
    identify_result = {}

    def do_identify():
        global fingerprint_sensor, _sensor_permanently_open, sensor_error_count

        with sensor_lock:
            try:
                s = get_fingerprint_sensor()

                # Flush any leftover UART bytes from previous operation
                try:
                    s._flush()
                except Exception:
                    pass

                # ── THE ONLY BLOCKING CALL ─────────────────────────────────
                rid = s.identify()
                # ──────────────────────────────────────────────────────────

                sensor_error_count = max(0, sensor_error_count - 1)

            except Exception as e:
                # Serial/sensor failure — mark for re-init on next call
                _sensor_permanently_open = False
                fingerprint_sensor = None
                sensor_error_count += 1
                print(f"[FP] identify exception: {e}")
                identify_result['error'] = str(e)
                return

        # ── Interpret result (lock already released above) ─────────────────
        if rid is None:
            # No finger placed — silent, no effects
            identify_result['outcome'] = 'no_finger'
            return

        if int(rid) < 0:
            # Finger present but not in DB
            identify_result['outcome'] = 'not_identified'
            return

        # ── Map sensor slot → emp_id + name ───────────────────────────────
        emp_id_str, name = template_to_empname(int(rid))

        # ── Days-allowed policy ────────────────────────────────────────────
        ok_allowed, msg_denied = is_login_allowed_by_days(emp_id_str)
        if not ok_allowed:
            identify_result['outcome'] = 'days_limit'
            identify_result['message'] = msg_denied
            return
# ── Log to local DB (fast — single INSERT) ─────────────────────────
     
        
        mark_activity()

        identify_result['outcome'] = 'success'
        identify_result['emp_id'] = emp_id_str
        identify_result['name'] = name or "Unknown"

    # ── Run identify with timeout ──────────────────────────────────────────
    # Use a plain thread so the timeout does not leave a zombie executor
    t = threading.Thread(target=do_identify, daemon=True)
    t.start()
    t.join(timeout=30)

    if t.is_alive():
        # Sensor hung — force reset so next call recovers
        _sensor_permanently_open = False
        fingerprint_sensor = None
        return jsonify({
            "success": False,
            "reason": "timeout",
            "message": "Sensor timeout. Please try again."
        })

    # ── Handle error during identify ──────────────────────────────────────
    if 'error' in identify_result:
        return jsonify({
            "success": False,
            "reason": "sensor_error",
            "message": f"Sensor error: {identify_result['error']}"
        })

    outcome = identify_result.get('outcome')

    # ── No finger ─────────────────────────────────────────────────────────
    if outcome == 'no_finger':
        return jsonify({"success": False, "reason": "no_finger", "message": "No finger detected"})

    # ── Not recognised — fire denied effects async, return immediately ─────
    if outcome == 'not_identified':
        threading.Thread(
            target=trigger_effects,
            args=('denied',),
            daemon=True
        ).start()
        return jsonify({
            "success": False,
            "reason": "not_identified",
            "message": "Fingerprint not recognized"
        })

    # ── Days limit ────────────────────────────────────────────────────────
    if outcome == 'days_limit':
        threading.Thread(
            target=trigger_effects,
            args=('denied',),
            daemon=True
        ).start()
        return jsonify({
            "success": False,
            "reason": "days_limit",
            "message": identify_result.get('message', 'Login not allowed today')
        })

    # ── SUCCESS — fire ALL effects asynchronously, return JSON NOW ─────────
    if outcome == 'success':
        emp_id = identify_result['emp_id']
        name   = identify_result['name']

        # This thread handles: thank_you.wav + LED green + printer + MSSQL + birthday
        threading.Thread(
            target=_dispatch_auth_result,
            kwargs=dict(success=True, mode="fingerprint", emp_id=emp_id, name=name),
            daemon=True
        ).start()

        # Return JSON to the browser RIGHT NOW — don't wait for audio/LED/printer
        return jsonify({
            "success": True,
            "user_id": emp_id,
            "name":    name
        })

    # Fallback (should never reach here)
    return jsonify({"success": False, "reason": "unknown", "message": "Unexpected state"})
    

@app.route("/api/finger_edit", methods=["POST"])
def api_finger_edit():
    """
    Update (re-enroll) fingerprint for a user.
    Logic: Delete existing template from sensor, then re-enroll with the SAME template_id.
    """
    data = request.json or {}
    emp_id = (data.get("emp_id") or "").strip()

    if not emp_id:
        return jsonify({"success": False, "message": "emp_id required"}), 400

    # Get the user's existing template_id (or reserve a new one)
    # template_id, is_new = get_or_reserve_template_id(emp_id)
    # print(f"[FINGERPRINT] finger_edit: emp_id={emp_id}, template_id={template_id}, is_new={is_new}")
    conn_chk = get_db_connection()
    r_chk = conn_chk.execute(
        "SELECT template_id FROM fingerprint_map WHERE emp_id=?", (emp_id,)
    ).fetchone()
    conn_chk.close()

    if r_chk and r_chk["template_id"]:
        template_id = int(r_chk["template_id"])
        is_new = False
    else:
        return jsonify({"success": False, "message": "No fingerprint registered for this employee. Please register first."}), 400

    # Get username from request or database
    username = (data.get("username") or data.get("name") or "").strip()
    if not username:
        try:
            conn = get_db_connection()
            row = conn.execute("SELECT name FROM users WHERE emp_id=?", (emp_id,)).fetchone()
            if row and row["name"]:
                username = row["name"]
            conn.close()
        except Exception:
            pass

    def do_update():
        db = get_finger_db()
        try:
            with sensor_lock:
                s = get_fingerprint_sensor()
                s.open()
                try:
                    # Step 1: Delete existing fingerprint from sensor (ignore if not exists)
                    print(f"[FINGERPRINT] Deleting existing template_id={template_id} from sensor...")
                    delete_ok = s.delete(template_id)
                    print(f"[FINGERPRINT] Delete result: {delete_ok}")

                    time.sleep(0.5)
                    s._flush()

                    # Step 2: Start enrollment with the SAME template_id
                    print(f"[FINGERPRINT] Starting enrollment at template_id={template_id}...")
                    ok = s.start_enroll(template_id)
                    if not ok:
                        return False, f"Enroll start failed at template_id {template_id}"

                    # Step 3: 3-step enrollment process (same approach as finger_register)
                    for step in (1, 2, 3):
                        print(f"[FINGERPRINT] Step {step}/3 - place finger on sensor...")

                        # Wait for user to place finger, then capture
                        time.sleep(2.5)

                        if not s.capture_finger():
                            return False, f"Capture failed at step {step}. Place finger firmly."

                        if not getattr(s, f'enroll{step}')():
                            return False, f"Enroll step {step} failed. Use same finger for all steps."

                        print(f"[FINGERPRINT] Step {step} complete")

                        if step < 3:
                            # Wait for finger to be lifted before next step
                            time.sleep(2.5)

                    # Step 4: Get template from sensor
                    print(f"[FINGERPRINT] Getting template from sensor...")
                    tpl = s.get_template(template_id)
                    if not tpl:
                        return False, "Template not captured from sensor"

                    print(f"[FINGERPRINT] Template captured: {len(tpl)} bytes")

                    # Step 5: Save to fingerprints table
                    db.execute("INSERT OR REPLACE INTO fingerprints (id, username, template) VALUES (?, ?, ?)",
                               (template_id, username, sqlite3.Binary(tpl)))
                    db.commit()
                    print(f"[FINGERPRINT] Saved to fingerprints table")

                    # Step 6: Update fingerprint_encodings + fingerprint_bins
                    try:
                        save_fingerprint_template(emp_id, tpl, template_id, username or None)
                        fp_log.info(f"[EDIT] Saved fingerprint_encodings/{emp_id}.dat ({len(tpl)} bytes)")
                    except Exception as e:
                        fp_log.error(f"[EDIT] Failed to update .dat file: {e}")

                    try:
                        _bins_dir = os.path.join(os.getcwd(), "fingerprint_bins")
                        os.makedirs(_bins_dir, exist_ok=True)
                        with open(os.path.join(_bins_dir, f"{emp_id}.bin"), "wb") as _bf:
                            _bf.write(tpl)
                        fp_log.info(f"[EDIT] Saved fingerprint_bins/{emp_id}.bin ({len(tpl)} bytes)")
                    except Exception as e:
                        fp_log.error(f"[EDIT] Failed to update .bin file: {e}")

                    # Step 7: Update fingerprint_map
                    conn_main = get_db_connection()
                    try:
                        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        conn_main.execute(
                            "INSERT OR REPLACE INTO fingerprint_map(emp_id, template_id, name,updated_at) VALUES (?,?,?,?)",
                            (emp_id, template_id, username or None,now)
                        )
                        conn_main.commit()
                        print(f"[FINGERPRINT] Updated fingerprint_map")
                    finally:
                        conn_main.close()

                    # Step 8: Broadcast to mesh network
                    try:
                        state = load_mesh_state()
                        self_ip = get_self_ip()
                        if state.get("devices"):
                            payload = {
                                "type": "finger_edit",
                                "emp_id": emp_id,
                                "user_id": template_id,
                                "username": username,
                                "template": base64.b64encode(tpl).decode()
                            }
                            sent_count = 0
                            for dev in state["devices"]:
                                ip = dev.get("ip")
                                if ip and ip != self_ip:
                                    try:
                                        send_udp_json(ip, 5006, payload)
                                        sent_count += 1
                                    except Exception as e:
                                        print(f"[FINGERPRINT] Failed to send to {ip}: {e}")
                            if sent_count > 0:
                                print(f"[FINGERPRINT] Broadcast to {sent_count} mesh devices")
                    except Exception as e:
                        print(f"[FINGERPRINT] Mesh broadcast error: {e}")

                    return True, f"Fingerprint re-enrolled successfully (template_id={template_id})"

                finally:
                    try:
                        s._flush()
                    except Exception:
                        pass
                    s.close()
        finally:
            db.close()

    ok, umsg = call_with_timeout(do_update, timeout=60)  # Increased timeout for enrollment
    return jsonify({"success": ok, "message": umsg, "template_id": template_id})

@app.route("/api/finger_delete", methods=["POST"])
@require_permission("finger_delete") 
def api_finger_delete():
    data = request.json or {}
    emp_id     = (data.get("emp_id") or "").strip()
    admin_pw   = data.get("admin_password")

    if not check_admin_password(admin_pw or ""):
        return jsonify({"success": False, "message": "Invalid admin password"})

    if not emp_id:
        return jsonify({"success": False, "message": "emp_id is required"}), 400

    def do_delete():
        conn = get_db_connection()
        try:
            # Step 1: Get the sensor slot (template_id) for this employee
            row = conn.execute(
                "SELECT template_id FROM fingerprint_map WHERE emp_id = ?", (emp_id,)
            ).fetchone()
        finally:
            conn.close()

        if not row:
            return False, f"No fingerprint found for employee {emp_id}"

        template_id = int(row["template_id"])

        with sensor_lock:
            s = get_fingerprint_sensor()
            s.open()
            try:
                s._flush()
                # Step 2: Delete from physical sensor slot only
                ok = s.delete(template_id)
            finally:
                try: s._flush()
                except Exception: pass
                s.close()

        # Step 3: Remove from fingerprint_map DB only
        # (users table is NOT touched — emp_id record stays intact)
        conn2 = get_db_connection()
        try:
            conn2.execute(
                "DELETE FROM fingerprint_map WHERE emp_id = ?", (emp_id,)
            )
            conn2.commit()
        finally:
            conn2.close()

        # Step 4: Delete .dat template file only
        # (users_img and face_encodings are NOT touched)
        try:
            delete_fingerprint_template(emp_id)
        except Exception as e:
            print(f"[FINGER_DELETE] Template file delete error: {e}")

        if ok:
            return True, (
                f"Fingerprint deleted for employee {emp_id} (slot {template_id}). "
                f"User profile kept intact."
            )
        else:
            # Sensor slot was already empty but DB + file cleaned up successfully
            return True, (
                f"Sensor slot was empty or already deleted, "
                f"but DB and template file cleaned for {emp_id}. "
                f"User profile kept intact."
            )

    try:
        ok, msg = call_with_timeout(do_delete, timeout=30)
        if ok is None:
            return jsonify({"success": False, "message": "Operation timed out"}), 500
        return jsonify({"success": bool(ok), "message": msg})
    except Exception as e:
        return jsonify({"success": False, "message": f"Delete failed: {e}"}), 500

@app.route("/api/finger_delete_all", methods=["POST"])
def api_finger_delete_all():
    admin_pw = (request.json or {}).get("admin_password")
    if not check_admin_password(admin_pw or ""):
        return jsonify({"success": False, "message": "Invalid admin password"})

    def do_delete_all():
        db = get_finger_db()
        with sensor_lock:
            s = get_fingerprint_sensor()
            s.open()
            try:
                ok = s.delete()  # delete all on the module
            finally:
                try:
                    s._flush()
                except Exception:
                    pass
                s.close()
            # there is not table in db to store fingerprints
            # if ok:
            #     db.execute("DELETE FROM fingerprints")
            #     db.commit()
            #     db.close()
            #     return True, "All fingerprints deleted from sensor and DB."
            # else:
            #     db.close()
            #     return False, "Delete all failed on sensor."
            if ok:
                conn_main = get_db_connection()
                try:
                    conn_main.execute("DELETE FROM fingerprint_map")
                    conn_main.commit()
                finally:
                    conn_main.close()
                return True, "All fingerprints deleted."
            else:
                return False, "Delete all failed on sensor."

    ok, msg = call_with_timeout(do_delete_all, timeout=30)
    return jsonify({"success": ok, "message": msg})
@app.route("/api/finger_reset", methods=["POST"])
def finger_reset():
    with sensor_lock:
        s = get_fingerprint_sensor()
        try:
            s.close()
            time.sleep(1)
            s.open()
            s._flush()
            return jsonify({"success": True, "message": "Sensor reset."})
        except Exception as e:
            return jsonify({"success": False, "message": f"Reset failed: {e}"})
        
@app.route("/api/user_stats", methods=["GET"])
def api_user_stats():
    """
    Returns biometric registration statistics using direct SQL COUNT queries.
    Optimized: no full table scan on Python side, all counting done in SQLite.
    
    Response:
    {
        "success": true,
        "total": 120,
        "face": 95,
        "fingerprint": 80,
        "rfid": 60,
        "no_face": 25,
        "no_fingerprint": 40,
        "no_rfid": 60,
        "no_any": 10
    }
    """
    try:
        conn = get_db_connection()

        # Total users in DB
        total = conn.execute(
            "SELECT COUNT(*) FROM users WHERE emp_id IS NOT NULL AND emp_id != ''"
        ).fetchone()[0]

        # Face registered: has a non-empty encoding_path file reference
        face = conn.execute(
            """SELECT COUNT(*) FROM users
               WHERE emp_id IS NOT NULL AND emp_id != ''
               AND encoding_path IS NOT NULL AND encoding_path != ''"""
        ).fetchone()[0]

        # Fingerprint registered: exists in fingerprint_map
        fingerprint = conn.execute(
            """SELECT COUNT(*) FROM users u
               WHERE u.emp_id IS NOT NULL AND u.emp_id != ''
               AND EXISTS (
                   SELECT 1 FROM fingerprint_map fm WHERE fm.emp_id = u.emp_id
               )"""
        ).fetchone()[0]

        # RFID registered: exists in rfid_card_map
        rfid = conn.execute(
            """SELECT COUNT(*) FROM users u
               WHERE u.emp_id IS NOT NULL AND u.emp_id != ''
               AND EXISTS (
                   SELECT 1 FROM rfid_card_map rc WHERE rc.emp_id = u.emp_id
               )"""
        ).fetchone()[0]

        # Users with at least one biometric (Face OR Fingerprint OR RFID)
        registered = conn.execute(
            """SELECT COUNT(*) FROM users u
               WHERE u.emp_id IS NOT NULL AND u.emp_id != ''
               AND (
                   (u.encoding_path IS NOT NULL AND u.encoding_path != '')
                   OR EXISTS (SELECT 1 FROM fingerprint_map fm WHERE fm.emp_id = u.emp_id)
                   OR EXISTS (SELECT 1 FROM rfid_card_map rc WHERE rc.emp_id = u.emp_id)
               )"""
        ).fetchone()[0]

        # No-biometric counts
        no_face = total - face
        no_fingerprint = total - fingerprint
        no_rfid = total - rfid

        # Users with NO biometric at all
        no_any = conn.execute(
            """SELECT COUNT(*) FROM users u
               WHERE u.emp_id IS NOT NULL AND u.emp_id != ''
               AND (u.encoding_path IS NULL OR u.encoding_path = '')
               AND NOT EXISTS (SELECT 1 FROM fingerprint_map fm WHERE fm.emp_id = u.emp_id)
               AND NOT EXISTS (SELECT 1 FROM rfid_card_map rc WHERE rc.emp_id = u.emp_id)"""
        ).fetchone()[0]

        conn.close()

        return jsonify({
            "success":      True,
            "total":        total,
            "registered":   registered,   # has at least one biometric
            "face":         face,
            "fingerprint":  fingerprint,
            "rfid":         rfid,
            "no_face":      no_face,
            "no_fingerprint": no_fingerprint,
            "no_rfid":      no_rfid,
            "no_any":       no_any
        })

    except Exception as e:
        print(f"[USER_STATS] Error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/users_list", methods=["GET"])
def api_users_list():
    """
    Return list of users for the register.html dropdown.
    Fields used by frontend: emp_id, name, role, birthdate

    Query params:
      - limit: max results (default 100, max 500)
      - offset: pagination offset (default 0)
      - q: search query (searches emp_id and name)
    """
    try:
        limit = min(int(request.args.get('limit', 100)), 500)
        offset = int(request.args.get('offset', 0))
        query = (request.args.get('q') or '').strip()

        conn = get_db_connection()

        if query:
            # Search mode - filter by emp_id or name
            search_param = f'%{query}%'
            rows = conn.execute(
                """
                SELECT emp_id,
                       COALESCE(name, '')      AS name,
                       COALESCE(role, 'User')  AS role,
                       COALESCE(shift, 'General') AS shift,
                FROM users
                WHERE emp_id IS NOT NULL AND emp_id <> ''
                  AND (emp_id LIKE ? OR LOWER(name) LIKE LOWER(?))
                ORDER BY
                    CASE WHEN emp_id LIKE ? THEN 0 ELSE 1 END,
                    emp_id
                LIMIT ? OFFSET ?
                """,
                (search_param, search_param, query + '%', limit, offset)
            ).fetchall()
        else:
            # Full list mode (limited)
            rows = conn.execute(
                """
                SELECT emp_id,
                       COALESCE(name, '')      AS name,
                       COALESCE(role, 'User')  AS role,
                       COALESCE(birthdate, '') AS birthdate
                FROM users
                WHERE emp_id IS NOT NULL AND emp_id <> ''
                ORDER BY emp_id
                LIMIT ? OFFSET ?
                """,
                (limit, offset)
            ).fetchall()

        # Get total count
        total = conn.execute("SELECT COUNT(*) FROM users WHERE emp_id IS NOT NULL AND emp_id <> ''").fetchone()[0]
        conn.close()

        users = []
        for r in rows:
            users.append({
                "emp_id":    r["emp_id"],
                "name":      r["name"],
                "role":      r["role"],
                "birthdate": r["birthdate"],
            })

        return jsonify({"success": True, "users": users, "total": total, "limit": limit, "offset": offset})

    except Exception as e:
        print("[USERS_LIST] error:", e)
        return jsonify({"success": False, "message": "users_list_failed"}), 500


@app.route("/api/users_search", methods=["GET"])
def api_users_search():
    """
    Fast search API for user dropdown - optimized for 15000+ users.
    Returns max 50 results matching the query.

    Query params:
      - q: search query (required, min 1 char)
    """
    try:
        query = (request.args.get('q') or '').strip()

        if not query:
            return jsonify({"success": True, "users": [], "total": 0})

        conn = get_db_connection()
        search_param = f'%{query}%'

        # Search with prioritization: exact match > starts with > contains
        rows = conn.execute(
            """
            SELECT emp_id,
                   COALESCE(name, '')      AS name,
                   COALESCE(role, 'User')  AS role,
                   COALESCE(birthdate, '') AS birthdate
            FROM users
            WHERE emp_id IS NOT NULL AND emp_id <> ''
              AND (emp_id LIKE ? OR LOWER(name) LIKE LOWER(?))
            ORDER BY
                CASE
                    WHEN emp_id = ? THEN 0
                    WHEN emp_id LIKE ? THEN 1
                    WHEN LOWER(name) LIKE LOWER(?) THEN 2
                    ELSE 3
                END,
                emp_id
            LIMIT 50
            """,
            (search_param, search_param, query, query + '%', query + '%')
        ).fetchall()
        conn.close()

        users = [
            {
                "emp_id":    r["emp_id"],
                "name":      r["name"],
                "role":      r["role"],
                "birthdate": r["birthdate"],
            }
            for r in rows
        ]

        return jsonify({"success": True, "users": users})

    except Exception as e:
        print("[USERS_SEARCH] error:", e)
        return jsonify({"success": False, "message": "search_failed"}), 500

@app.route("/api/user_get", methods=["GET"])
def api_user_get():
    emp_id = (request.args.get("emp_id") or "").strip()
    if not emp_id:
        return jsonify({"success": False, "message": "emp_id required"}), 400

    conn = get_db_connection()
    try:
        row = conn.execute(
            """SELECT emp_id, name, role, birthdate, 
                      image_path, encoding_path, rfid_cards,
                      created_at, updated_at
               FROM users WHERE emp_id = ?""",
            (emp_id,)
        ).fetchone()

        if not row:
            return jsonify({"success": False, "message": "User not found"}), 404

        return jsonify({
            "success": True,
            "user": {
                "emp_id":        row["emp_id"],
                "name":          row["name"],
                "role":          row["role"],
                "birthdate":     row["birthdate"],
                "image_path":    row["image_path"],
                "encoding_path": row["encoding_path"],
                "rfid_cards":    row["rfid_cards"],
                "created_at":    row["created_at"],
                "updated_at":    row["updated_at"],
            }
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# RFID APIs
# -----------------------------------------------------------------------------
@app.route("/api/rfid_register", methods=["POST"])
@require_permission("rfid_register")
def api_rfid_register():
    data = request.json or {}
    employee_id = (data.get("employee_id") or "").strip()
    name = (data.get("name") or "").strip()
    if not employee_id or not name:
        return jsonify({"success": False, "message": "Employee ID and name required."})

    # Read RFID card UID from hardware
    ok, result = rfid.rfid_read(timeout=10)
    if not ok:
        return jsonify({"success": False, "message": f"No card detected: {result}"})

    # Extract UID from result
    if isinstance(result, dict):
        uid = (result.get("uid") or result.get("card_number") or "").strip()
    else:
        m = re.search(r"([0-9A-Fa-f]{8,})", str(result))
        uid = m.group(1) if m else str(result).strip()

    if not uid:
        return jsonify({"success": False, "message": "Could not read card UID."})

    conn = get_db_connection()
    try:
        # Check if card already assigned to another employee
        existing = conn.execute(
            "SELECT emp_id FROM rfid_card_map WHERE rfid_card = ?", (uid,)
        ).fetchone()
        if existing and existing["emp_id"] != employee_id:
            return jsonify({
                "success": False,
                "message": f"Card already assigned to employee {existing['emp_id']}."
            })

        # Check if this employee already has this card
        already = conn.execute(
            "SELECT emp_id FROM rfid_card_map WHERE emp_id = ? AND rfid_card = ?",
            (employee_id, uid)
        ).fetchone()
        if already:
            return jsonify({"success": False, "message": "This card is already registered to this employee."})

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert into rfid_card_map (the ONLY table for RFID cards)
        conn.execute(
            """INSERT OR REPLACE INTO rfid_card_map (emp_id, name, rfid_card, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (employee_id, name, uid, now, now)
        )
        conn.commit()

         # ── Mesh broadcast (non-blocking) ─────────────────────────────────
        threading.Thread(
            target=broadcast_rfid_edit,
            args=(employee_id,),
            daemon=True
        ).start()
        
        return jsonify({
            "success": True,
            "message": "RFID card registered successfully.",
            "rfid_uid": uid,
            "card_number": uid,
            "emp_id": employee_id,
            "name": name
        })
    except Exception as e:
        return jsonify({"success": False, "message": f"Database error: {e}"})
    finally:
        conn.close()

@app.route("/api/rfid_login", methods=["POST"])
def api_rfid_login():
    mark_activity()

    # Read card from hardware
    ok, result = rfid.rfid_read(timeout=10)
    if not ok:
        return jsonify({"success": False, "message": "No card detected"})

    # Extract UID
    if isinstance(result, dict):
        uid = (result.get("uid") or result.get("card_number") or "").strip()
    else:
        m = re.search(r"([0-9A-Fa-f]{8,})", str(result))
        uid = m.group(1) if m else str(result).strip()

    if not uid:
        return jsonify({"success": False, "message": "Could not read card UID."})

    # Look up employee from rfid_card_map
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT emp_id, name FROM rfid_card_map WHERE rfid_card = ?", (uid,)
        ).fetchone()
    finally:
        conn.close()

    if not row:
        trigger_effects('denied')
        return jsonify({"success": False, "message": "Card not registered."})

    emp = str(row["emp_id"])
    nm = row["name"] or ""

    # Days-allowed check
    ok_allowed, msg_denied = is_login_allowed_by_days(emp)
    if not ok_allowed:
        trigger_effects('denied')
        return jsonify({"success": False, "reason": "days_limit", "message": msg_denied}), 403

   
    mark_activity()
    _dispatch_auth_result(success=True, mode="rfid", emp_id=emp, name=nm)
    return jsonify({"success": True, "user_id": emp, "name": nm})


@app.route("/api/rfid_edit", methods=["POST"])
@require_permission("rfid_edit")
def api_rfid_edit():
    data = request.json or {}
    employee_id = (data.get("employee_id") or "").strip()
    new_name = (data.get("name") or "").strip()
    admin_pw = (data.get("admin_password") or "").strip()
    if not check_admin_password(admin_pw):
        return jsonify({"success": False, "message": "Incorrect admin password."})
    ok, msg = rfid.rfid_edit(employee_id, new_name)
    return jsonify({"success": ok, "message": msg})

@app.route("/api/rfid_delete", methods=["POST"])
@require_permission("rfid_delete")
def api_rfid_delete():
    data = request.json or {}
    employee_id = (data.get("employee_id") or "").strip()
    admin_pw = (data.get("admin_password") or "").strip()
    if not check_admin_password(admin_pw):
        return jsonify({"success": False, "message": "Incorrect admin password."})
    ok, msg = rfid.rfid_delete(employee_id)
    return jsonify({"success": ok, "message": msg})


@app.route("/api/face_delete", methods=["POST"])
def api_face_delete():
    """Delete face encoding and image for a user while keeping their record intact."""
    data = request.json or {}
    emp_id = (data.get("emp_id") or "").strip()
    admin_pw = (data.get("admin_password") or "").strip()

    if not check_admin_password(admin_pw):
        return jsonify({"success": False, "message": "Incorrect admin password."})

    if not emp_id:
        return jsonify({"success": False, "message": "Employee ID required."}), 400

    try:
        # Remove face encoding and image files from disk
        delete_image(emp_id)
        delete_encoding(emp_id)

        # Clear encoding_path and image_path columns in users table
        # (keep user record intact — only remove face-related columns)
        conn = get_db_connection()
        try:
            conn.execute(
                "UPDATE users SET encoding_path = '', image_path = '' WHERE emp_id = ?",
                (emp_id,)
            )
            conn.commit()
        finally:
            conn.close()

        # Reload face recognizer to remove this user from memory
        recognizer.load_all_encodings()

        return jsonify({
            "success": True,
            "message": f"Face data deleted for employee {emp_id}. User record preserved."
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"Error deleting face data: {str(e)}"
        }), 500
    
# -----------------------------------------------------------------------------
# Network Information API
# -----------------------------------------------------------------------------
@app.route('/api/network_info', methods=["GET"])
def api_network_info():
    """Get LAN and WiFi connection information"""
    try:
        network_info = {
            "lan": None,
            "wifi": None
        }

        # Get LAN (eth0) information
        try:
            eth_info = subprocess.check_output("ip addr show eth0", shell=True, timeout=5).decode()
            if "state UP" in eth_info:
                # Extract IP address
                ip_match = re.search(r'inet (\d+\.\d+\.\d+\.\d+)', eth_info)
                # Get MAC address
                mac_match = re.search(r'link/ether ([0-9a-f:]+)', eth_info)
                # Get gateway
                try:
                    gateway_output = subprocess.check_output("ip route | grep default | grep eth0", shell=True, timeout=5).decode()
                    gateway_match = re.search(r'default via (\d+\.\d+\.\d+\.\d+)', gateway_output)
                    gateway = gateway_match.group(1) if gateway_match else None
                except:
                    gateway = None

                network_info["lan"] = {
                    "connected": True,
                    "interface": "eth0",
                    "ip": ip_match.group(1) if ip_match else None,
                    "mac": mac_match.group(1) if mac_match else None,
                    "gateway": gateway
                }
        except:
            network_info["lan"] = {"connected": False}

        # Get WiFi (wlan0) information
        try:
            wlan_info = subprocess.check_output("ip addr show wlan0", shell=True, timeout=5).decode()
            if "state UP" in wlan_info:
                # Extract IP address
                ip_match = re.search(r'inet (\d+\.\d+\.\d+\.\d+)', wlan_info)
                # Get MAC address
                mac_match = re.search(r'link/ether ([0-9a-f:]+)', wlan_info)
                # Get SSID
                try:
                    ssid_output = subprocess.check_output("iwgetid -r", shell=True, timeout=5).decode().strip()
                    ssid = ssid_output if ssid_output else None
                except:
                    ssid = None
                # Get gateway
                try:
                    gateway_output = subprocess.check_output("ip route | grep default | grep wlan0", shell=True, timeout=5).decode()
                    gateway_match = re.search(r'default via (\d+\.\d+\.\d+\.\d+)', gateway_output)
                    gateway = gateway_match.group(1) if gateway_match else None
                except:
                    gateway = None

                network_info["wifi"] = {
                    "connected": True,
                    "interface": "wlan0",
                    "ssid": ssid,
                    "ip": ip_match.group(1) if ip_match else None,
                    "mac": mac_match.group(1) if mac_match else None,
                    "gateway": gateway
                }
        except:
            network_info["wifi"] = {"connected": False}

        return jsonify({
            "success": True,
            "lan": network_info["lan"],
            "wifi": network_info["wifi"]
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route('/api/network_disconnect', methods=["POST"])
def api_network_disconnect():
    """Disconnect from LAN or WiFi"""
    try:
        data = request.json or {}
        network_type = data.get("type")  # "lan" or "wifi"

        # Map type to interface
        if network_type == "lan":
            interface = "eth0"
        elif network_type == "wifi":
            interface = "wlan0"
        else:
            return jsonify({"success": False, "message": "Invalid type. Use 'lan' or 'wifi'."})

        # Bring down the interface
        subprocess.check_output(f"sudo ifconfig {interface} down", shell=True, timeout=5)

        return jsonify({"success": True, "message": f"{network_type.upper()} disconnected"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route('/api/network_lan_toggle', methods=['POST'])
def network_lan_toggle():
    """Enable or disable LAN (eth0) interface"""
    try:
        data = request.get_json()
        enable = data.get('enable', True)
        interface = 'eth0'

        if enable:
            subprocess.run(['sudo', 'ifconfig', interface, 'up'], check=True, timeout=10)
            message = 'LAN enabled successfully'
        else:
            subprocess.run(['sudo', 'ifconfig', interface, 'down'], check=True, timeout=10)
            message = 'LAN disabled successfully'

        return jsonify({'success': True, 'message': message})
    except subprocess.TimeoutExpired:
        return jsonify({'success': False, 'message': 'Network command timed out'}), 500
    except subprocess.CalledProcessError as e:
        return jsonify({'success': False, 'message': f'Command failed: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# -----------------------------------------------------------------------------
# Delete user
# -----------------------------------------------------------------------------
@app.route("/api/delete_user", methods=["POST"])
@require_permission("delete")
def delete_user():
    data = request.json or {}
    emp_id = (data.get("emp_id") or "").strip()
    admin_pw = (data.get("admin_password") or "").strip()

    print("Delete request for:", emp_id)

    if not emp_id:
        return jsonify({"success": False, "message": "emp_id missing"})

    if not check_admin_password(admin_pw):
        return jsonify({"success": False, "message": "Incorrect admin password."})

    # ── Step 1: Delete fingerprint from physical sensor ──
    try:
        conn_fp = get_db_connection()
        fp_row = conn_fp.execute(
            "SELECT template_id FROM fingerprint_map WHERE emp_id = ?", (emp_id,)
        ).fetchone()
        conn_fp.close()

        if fp_row:
            template_id = int(fp_row["template_id"])
            try:
                with sensor_lock:
                    s = get_fingerprint_sensor()
                    s.open()
                    try:
                        s._flush()
                        s.delete(template_id)
                    finally:
                        try: s._flush()
                        except Exception: pass
                        s.close()
                print(f"Fingerprint slot {template_id} deleted from sensor")
            except Exception as e:
                print(f"[DELETE_USER] Sensor delete error (non-fatal): {e}")
    except Exception as e:
        print(f"[DELETE_USER] Fingerprint lookup error (non-fatal): {e}")

    # ── Step 2: Delete fingerprint template files from disk ──
    try:
        delete_fingerprint_template(emp_id)
        print(f"Fingerprint template files deleted for {emp_id}")
    except Exception as e:
        print(f"[DELETE_USER] Fingerprint file delete error (non-fatal): {e}")

    # ── Step 3: Delete face image and encoding files from disk ──
    try:
        delete_image(emp_id)
        delete_encoding(emp_id)
        print(f"Face image and encoding deleted for {emp_id}")
    except Exception as e:
        print(f"[DELETE_USER] Face file delete error (non-fatal): {e}")

    # ── Step 4: Delete all database records ──
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("DELETE FROM users WHERE emp_id=?", (emp_id,))
    print("Users deleted:", c.rowcount)

    c.execute("DELETE FROM fingerprint_map WHERE emp_id=?", (emp_id,))
    print("Fingerprint rows deleted:", c.rowcount)

    c.execute("DELETE FROM rfid_card_map WHERE emp_id=?", (emp_id,))
    print("RFID rows deleted:", c.rowcount)

    conn.commit()
    conn.close()

    # ── Step 5: Reload face recognizer to remove from memory ──
    try:
        recognizer.load_all_encodings()
        print(f"Face recognizer reloaded after deleting {emp_id}")
    except Exception as e:
        print(f"[DELETE_USER] Recognizer reload error (non-fatal): {e}")

    return jsonify({
        "success": True,
        "message": f"User {emp_id} completely deleted — database, face, fingerprint (sensor+files), and RFID."
    })


# -----------------------------------------------------------------------------
# Admin gates (face/finger/rfid/password)
# -----------------------------------------------------------------------------
def _is_operator_role(role: str | None) -> bool:
    if not role:
        return False
    r = role.strip().lower()
    return r in ("admin", "super admin", "superadmin")


def _is_super_admin_role(role: str | None) -> bool:
    """Restrict admin login flows to Super Admin only."""
    if not role:
        return False
    r = role.strip().lower().replace(" ", "")
    return r == "superadmin"

def _is_admin_or_super_admin_role(role: str | None) -> bool:
    """
    Returns True for both Admin and Super Admin roles.
    Used by all operator-verify endpoints so both roles can access /menu.
    Super Admin gets full access; Admin gets RBAC-limited access.
    """
    if not role:
        return False
    r = role.strip().lower().replace(" ", "").replace("_", "")
    return r in ("admin", "superadmin")

def _fetch_user_row(emp_id=None):
    """
    Fetch user row from users table and return a dict.
    If emp_id is None, returns the most recently inserted user.

    Uses column names from your schema:

        emp_id, name, display_image, face_encoding,
        rfid_cards, role, template_id, birthdate
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()

        # IMPORTANT: include role so operator checks work
        cols = [
            "emp_id",
            "name",
            "role",
            "birthdate",
            "created_at",
            "updated_at",
            "encoding_path", 
            "image_path",
        ]

        if emp_id:
            q = f"SELECT {', '.join(cols)} FROM users WHERE emp_id = ? LIMIT 1"
            c.execute(q, (emp_id,))
        else:
            q = f"SELECT {', '.join(cols)} FROM users ORDER BY rowid DESC LIMIT 1"
            c.execute(q)

        row = c.fetchone()
        row_dict = _row_to_dict(c, row) if row else None
        return row_dict

    except Exception as e:
        print("[MESH] _fetch_user_row error:", e)
        return None

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

        
def broadcast_user_upsert_by_emp(emp_id):
    """
    Broadcast the upsert for the user identified by emp_id.
    Reads canonical DB row, constructs a minimal payload, and calls send_reliable().
    Returns True if send_reliable returned truthy, False otherwise.
    """
    if not NETWORK_ENABLED:
        return False
    try:
        if not emp_id:
            print("[MESH] broadcast_user_upsert_by_emp called without emp_id")
            return False

        user = _fetch_user_row(emp_id=emp_id)
        if not user:
            print(f"[MESH] no user row found for emp_id={emp_id}")
            return False

        # Build minimal payload to avoid huge UDP packets
        payload = {
            "type": "user_upsert",
            "msg_id": str(uuid.uuid4()),
            "ts": time.time(),
            "from": (get_self_ip() if callable(get_self_ip) else None),
            "user": {
                "emp_id": user.get("emp_id"),
                "name": user.get("name"),
                "template_id": user.get("template_id"),
                # do NOT include large blob fields here (face_encoding/display_image)
                # receivers can request full data via HTTP endpoint if needed.
            }
        }

        targets = get_saved_mesh_devices()
        if not targets:
            # broadcast
            ok = send_reliable(payload, targets=None, port=UDP_PORT)
            print(f"[MESH] broadcast_user_upsert_by_emp emp_id={emp_id} broadcast_ok={ok}")
            return bool(ok)

        ok = send_reliable(payload, targets=targets, port=UDP_PORT)
        print(f"[MESH] broadcast_user_upsert_by_emp emp_id={emp_id} ok={ok} targets={targets}")
        return bool(ok)
    except Exception as e:
        print("[MESH] error broadcasting user_upsert:", e)
        return False
def broadcast_user_upsert_last():
    """
    Convenience: broadcast most recent user (useful if handler didn't keep emp_id).
    """
    if not NETWORK_ENABLED:
        return False
    try:
        user = _fetch_user_row(emp_id=None)
        if not user:
            print("[MESH] no recent user to broadcast")
            return False
        return broadcast_user_upsert_by_emp(user.get("emp_id"))
    except Exception as e:
        print("[MESH] broadcast_user_upsert_last error:", e)
        return False

@app.route("/api/operator_face_verify", methods=["POST"])
def api_operator_face_verify():

    data = request.get_json(silent=True) or {}
    img_b64 = (data.get("image") or "").split(",")[-1]

    if not img_b64:
        return jsonify({"success": False, "message": "no_image"}), 400

    try:
        img_bytes = base64.b64decode(img_b64)
        nparr = np.frombuffer(img_bytes, np.uint8)
        bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if bgr is None:
            return jsonify({"success": False, "message": "decode_failed"}), 400
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

        import face_recognition
        if not face_recognition.face_locations(rgb):
            return jsonify({"success": False, "message": "no_face"})

        user_id = recognizer.recognize(rgb)
        if not user_id:
            return jsonify({"success": False, "message": "unknown_face"})

        u = _fetch_user_row(str(user_id)) or {"emp_id": str(user_id), "name": "", "role": ""}

        # CHANGED: accept Admin OR Super Admin (was: _is_super_admin_role only)
        if not _is_admin_or_super_admin_role(u.get("role")):
            return jsonify({"success": False, "message": "not_admin"}), 403

        session["admin_session_active"] = True
        session["admin_emp_id"] = str(u["emp_id"])
        return jsonify({
            "success": True,
            "emp_id":  str(u["emp_id"]),
            "name":    u.get("name", ""),
            "role":    u.get("role", "")
        })
    except Exception as e:
        return jsonify({"success": False, "message": f"error: {e}"}), 500

@app.route("/api/operator_finger_verify", methods=["POST"])
def api_operator_finger_verify():
    try:
        s = get_fingerprint_sensor()
        with sensor_lock:
            s.open()
            try:
                if hasattr(s, "capture_finger"):
                    if not s.capture_finger():
                        return jsonify(success=False, message="Place finger on sensor"), 200

                rid = s.identify() if "side_effects" not in inspect.signature(s.identify).parameters else s.identify(side_effects=True)
                if rid is None:
                    return jsonify(success=False, message="No finger"), 200
                if int(rid) < 0:
                    return jsonify(success=False, message="Unknown finger"), 200

                emp_id, name = template_to_empname(int(rid))
                u = _fetch_user_row(emp_id) or {"emp_id": emp_id, "name": name, "role": ""}

                # CHANGED: accept Admin OR Super Admin (was: _is_super_admin_role only)
                if not _is_admin_or_super_admin_role(u.get("role")):
                    return jsonify(success=False, message="not_admin"), 403

                
                try:  broadcast_login_udp(str(emp_id), name, "finger")
                except Exception: pass
                try:  broadcast_login_tcp(str(emp_id), name, "finger")
                except Exception: pass

                session["admin_session_active"] = True
                session["admin_emp_id"] = str(emp_id)

                return jsonify(
                    success=True,
                    emp_id=str(emp_id),
                    name=name,
                    role=u.get("role", ""),
                    template_id=int(rid)
                ), 200
            finally:
                try: s.close()
                except Exception: pass
    except Exception as e:
        return jsonify(success=False, message=f"{type(e).__name__}: {e}"), 500


@app.route("/api/operator_rfid_verify", methods=["POST"])
def api_operator_rfid_verify():
    try:
        ok, res = rfid.rfid_login()
        if not ok:
            return jsonify({"success": False, "message": "no_card"})

        emp = str(res.get("employee_id") or res.get("emp_id") or "")
        if not emp:
            return jsonify({"success": False, "message": "no_emp_id"}), 400

        u = _fetch_user_row(emp) or {"emp_id": emp, "name": "", "role": ""}

        # CHANGED: accept Admin OR Super Admin (was: _is_super_admin_role only)
        if not _is_admin_or_super_admin_role(u.get("role")):
            return jsonify({"success": False, "message": "not_admin"}), 403

        session["admin_session_active"] = True
        session["admin_emp_id"] = str(u["emp_id"])
        return jsonify({
            "success": True,
            "emp_id":  str(u["emp_id"]),
            "name":    u.get("name", ""),
            "role":    u.get("role", "")
        })
    except Exception as e:
        return jsonify({"success": False, "message": f"error: {e}"}), 500


# Password fallback for operator
@app.route("/api/operator_password_login", methods=["POST"])
def api_operator_password_login():
    data = request.get_json(silent=True) or {}
    pw = (data.get("password") or "").strip()
    if not pw:
        return jsonify({"success": False, "message": "Password required"}), 400
    if not check_admin_password(pw):
        return jsonify({"success": False, "message": "Invalid password"}), 403

    # Password login always grants access (password IS the admin credential)
    # Role is returned as "Admin" so frontend roleLabel() shows correctly
    session["admin_session_active"] = True
    session["admin_emp_id"] = "ADMIN"
    return jsonify({
        "success": True,
        "emp_id":  "ADMIN",
        "name":    "Administrator",
        "role":    "Admin"       # frontend isAdminRole() will accept this
    })

# -----------------------------------------------------------------------------
# Handoff (QR) Import/Export flow
# -----------------------------------------------------------------------------
def _qr_base_url():
    # 1) explicit
    base = (app.config.get("PUBLIC_BASE_URL") or "").strip()
    if base:
        return base.rstrip("/")
    # 2) infer from request; if localhost, use LAN IP
    host = (request.host or "").split(":")[0]
    if host in ("127.0.0.1", "localhost"):
        ip = "127.0.0.1"
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            pass
        finally:
            try:
                if s:
                    s.close()
            except Exception:
                pass
        return f"http://{ip}:{app.config.get('APP_PORT', 5000)}"
    # 3) default to host_url
    return request.host_url.rstrip("/")


@app.route("/api/handoff_begin")
def api_handoff_begin():
    mode = (request.args.get("mode") or "").strip().lower()
    if mode not in ("import", "export"):
        return jsonify({"success": False, "message": "mode must be import or export"}), 400
    token = uuid.uuid4().hex[:8]
    state = {
        "mode": mode,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "pending",
        "have_file": False,
        "original_name": "",
    }
    set_setting(f"handoff:{mode}-{token}", json.dumps(state))
    base = _qr_base_url()
    phone_url = f"{base}/handoff/{mode}/{mode}-{token}"
    return jsonify({"success": True, "token": f"{mode}-{token}", "url": phone_url})


@app.route("/api/handoff_status")
def api_handoff_status_qs():
    token = (request.args.get("token") or "").strip()
    if not token:
        return jsonify({"success": True, "exists": False}), 404
    return api_handoff_status_path(token)


@app.route("/api/handoff_status/<token>")
def api_handoff_status_path(token):
    blob = get_setting(f"handoff:{token}")
    if not blob:
        return jsonify({"success": True, "exists": False}), 404
    try:
        state = json.loads(blob)
    except Exception:
        return jsonify({"success": True, "exists": False}), 404

    out = {
        "success": True,
        "exists": True,
        "mode": state.get("mode"),
        "status": state.get("status", "pending"),
        "have_file": bool(state.get("have_file")),
        "original_name": state.get("original_name", ""),
    }
    return jsonify(out)


@app.route("/handoff/<mode>/<token>")
def handoff_portal(mode, token):
    blob = get_setting(f"handoff:{token}")
    if not blob:
        return render_template("handoff_portal.html", mode="expired", token=token), 403

    try:
        state = json.loads(blob)
    except Exception:
        return render_template("handoff_portal.html", mode="expired", token=token), 403

    created_at = state.get("created_at")
    if created_at:
        try:
            created = datetime.fromisoformat(created_at)
            if (datetime.now() - created).total_seconds() > HANDOFF_TTL_SECONDS:
                return render_template("handoff_portal.html", mode="expired", token=token), 403
        except Exception:
            pass

    mode = (mode or "").strip().lower()
    if mode not in ("import", "export") or state.get("mode") != mode:
        return render_template("handoff_portal.html", mode="expired", token=token), 403

    return render_template("handoff_portal.html", mode=mode, token=token)


# ─────────────────────────────────────────────────────────────────────────────
# Route 3: /api/import_upload  (REPLACE existing)
# Key change: .bin files supported; .dat still accepted for backward compat
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/import_upload", methods=["POST"])
def api_import_upload():
    """
    Import XLSX or ZIP file — now also handles fingerprint_bins/*.bin.
    .dat files in the ZIP are still accepted for backward compatibility.
    """
    try:
        token = (request.form.get("token") or "").strip()
        if not token:
            return jsonify(success=False, message="Missing token"), 400
 
        blob = get_setting(f"handoff:{token}")
        if not blob:
            return jsonify(success=False, message="Invalid or expired token"), 403
        try:
            state = json.loads(blob)
        except Exception as e:
            print(f"[SELF-REPAIR] Token state parse error: {e}")
            return jsonify(success=False, message="Token state corrupt"), 403
 
        if state.get("mode") != "import":
            return jsonify(success=False, message="Token is not for import"), 403
 
        f = request.files.get("file")
        if not f or f.filename == "":
            return jsonify(success=False, message="No file provided"), 400
 
        fname = secure_filename(f.filename)
        is_zip  = fname.lower().endswith(".zip")
        is_xlsx = fname.lower().endswith(".xlsx")
 
        if not (is_zip or is_xlsx):
            return jsonify(success=False, message="Please upload a .xlsx or .zip file"), 400
 
        os.makedirs(HANDOFF_UPLOAD_DIR, exist_ok=True)
        save_path = os.path.join(HANDOFF_UPLOAD_DIR, f"{token}__{fname}")
        try:
            f.save(save_path)
            print(f"[IMPORT] File saved to {save_path}")
        except Exception as e:
            print(f"[SELF-REPAIR] File save error: {e}")
            return jsonify(success=False, message=f"Save failed: {e}"), 500
 
        result = {"ok": True, "updated": {}, "errors": []}
        try:
            if is_zip:
                from pathlib import Path
                with zipfile.ZipFile(save_path, "r") as zf:
                    # ── face images ───────────────────────────────────────
                    face_img_count = 0
                    for member in zf.namelist():
                        if member.startswith("users_img/") and (
                            member.endswith(".jpg") or member.endswith(".png")
                        ):
                            Path("users_img").mkdir(exist_ok=True)
                            zf.extract(member)
                            face_img_count += 1
                    if face_img_count:
                        result["errors"].append(f"Restored {face_img_count} face images")
 
                    # ── face encodings (.dat) ─────────────────────────────
                    face_enc_count = 0
                    for member in zf.namelist():
                        if member.startswith("face_encodings/") and member.endswith(".dat"):
                            Path("face_encodings").mkdir(exist_ok=True)
                            zf.extract(member)
                            face_enc_count += 1
                    if face_enc_count:
                        result["errors"].append(f"Restored {face_enc_count} face encodings")
 
                    # ── fingerprint bins (.bin — NEW) ─────────────────────
                    fp_bin_count = 0
                    bins_dir = Path("fingerprint_bins")
                    bins_dir.mkdir(exist_ok=True)
                    for member in zf.namelist():
                        if member.startswith("fingerprint_bins/") and member.endswith(".bin"):
                            zf.extract(member)
                            fp_bin_count += 1
                    if fp_bin_count:
                        result["errors"].append(f"Restored {fp_bin_count} fingerprint .bin templates")
 
                    # ── fingerprint encodings (.dat — legacy) ─────────────
                    fp_dat_count = 0
                    for member in zf.namelist():
                        if member.startswith("fingerprint_encodings/") and (
                            member.endswith(".dat") or member.endswith(".json")
                        ):
                            Path("fingerprint_encodings").mkdir(exist_ok=True)
                            zf.extract(member)
                            if member.endswith(".dat"):
                                fp_dat_count += 1
 
                    # ── XLSX sheets ───────────────────────────────────────
                    for member in zf.namelist():
                        if member.endswith(".xlsx"):
                            with zf.open(member) as xf:
                                xlsx_result = apply_xlsx_to_db(xf)
                                result["ok"] = result["ok"] and xlsx_result.get("ok", True)
                                result["updated"].update(xlsx_result.get("updated", {}))
                                result["errors"].extend(xlsx_result.get("errors", []))
 
                # Reload face recognizer
                try:
                    recognizer.load_all_encodings()
                    result["errors"].append("Face recognizer reloaded")
                except Exception as e:
                    result["errors"].append(f"Face recognizer reload error: {e}")
 
                # Load fingerprint templates into sensor
                if fp_bin_count or fp_dat_count:
                    try:
                        load_fingerprint_templates_on_startup()
                        result["errors"].append("Fingerprint templates loaded into sensor")
                    except Exception as e:
                        result["errors"].append(f"Fingerprint load error: {e}")
 
            else:
                with open(save_path, "rb") as xf:
                    result = apply_xlsx_to_db(xf)
 
            print(f"[IMPORT] Import result: {result}")
        except Exception as e:
            print(f"[SELF-REPAIR] Import processing error: {e}")
            import traceback; traceback.print_exc()
            return jsonify(success=False, message=f"Import failed: {e}"), 500
 
    except Exception as e:
        print(f"[SELF-REPAIR] Unexpected import error: {e}")
        import traceback; traceback.print_exc()
        return jsonify(success=False, message=f"Unexpected error: {e}"), 500
 
    success = bool(result.get("ok"))
    errors  = [e for e in result.get("errors", []) if e]
    updated = result.get("updated") or {}
 
    msg_parts = []
    if updated:
        msg_parts.append("Updated: " + ", ".join(f"{t}={c}" for t, c in updated.items()))
    if errors:
        msg_parts.append("; ".join(errors))
    message = " | ".join(msg_parts) or ("Import complete" if success else "Import failed")
 
    state.update({
        "status":        "received" if success else "error",
        "have_file":     True,
        "file_path":     save_path,
        "original_name": fname,
        "updated_at":    datetime.now().isoformat(timespec="seconds"),
        "import_result": result,
    })
    set_setting(f"handoff:{token}", json.dumps(state))
 
    return (
        jsonify(success=success, message=message, errors=errors,
                result=result, name=fname),
        200 if success else 400,
    )


def _build_export_zip_bytes():
    """Build a ZIP that contains one .xlsx per SQLite table."""
    try:
        return io.BytesIO(export_all_tables_as_zip_of_xlsx())
    except Exception as e:
        # Fallback to old behavior if absolutely necessary
        print(f"[EXPORT] XLSX pack failed: {e}")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            if os.path.exists(DB_PATH):
                z.write(DB_PATH, arcname="users.db")
        buf.seek(0)
        return buf


@app.route("/api/handoff_prepare_export/<token>", methods=["POST"])
def api_handoff_prepare_export(token):
    blob = get_setting(f"handoff:{token}")
    if not blob:
        return jsonify(success=False, message="Invalid token"), 404
    try:
        state = json.loads(blob)
    except Exception:
        return jsonify(success=False, message="Corrupt token"), 404
    if state.get("mode") != "export":
        return jsonify(success=False, message="Wrong mode"), 400

    try:
        zip_bytes = export_all_tables_as_zip_of_xlsx()
        out_path = os.path.join(HANDOFF_UPLOAD_DIR, f"{token}_export.zip")
        with open(out_path, "wb") as f:
            f.write(zip_bytes)
        state.update({"prepared_zip": out_path, "status": "ready"})
        set_setting(f"handoff:{token}", json.dumps(state))
        return jsonify(success=True)
    except Exception as e:
        return jsonify(success=False, message=str(e)), 500


@app.route("/api/export_zip")
def api_export_zip():
    token = (request.args.get("token") or "").strip()
    if not token:
        abort(400, "Missing token")

    blob = get_setting(f"handoff:{token}")
    if not blob:
        abort(403, "Invalid or expired token")
    try:
        state = json.loads(blob)
    except Exception:
        abort(403, "Token corrupt")

    if state.get("mode") != "export":
        abort(403, "Wrong mode")

    p = state.get("prepared_zip")
    if p and os.path.exists(p):
        return send_file(p, as_attachment=True, download_name="export.zip", mimetype="application/zip")

    # Fallback: build now
    try:
        zip_bytes = export_all_tables_as_zip_of_xlsx()
        buf = io.BytesIO(zip_bytes)
        return send_file(buf, as_attachment=True, download_name="export.zip", mimetype="application/zip")
    except Exception as e:
        abort(500, f"Export failed: {e}")


@app.route("/api/get_all_employee_ids")
def get_all_employee_ids():
    """
    Returns all employee IDs from users table for autocomplete in export page.
    """
    try:
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT emp_id FROM users WHERE emp_id IS NOT NULL AND emp_id != '' ORDER BY emp_id"
        ).fetchall()
        conn.close()
        ids = [r["emp_id"] for r in rows]
        return jsonify({"success": True, "ids": ids})
    except Exception as e:
        return jsonify({"success": False, "ids": [], "message": str(e)}), 500


def _is_numeric_id(val):
    """Return True if the ID value is purely numeric (can be cast to int)."""
    try:
        int(str(val).strip())
        return True
    except (ValueError, TypeError):
        return False

def _build_filtered_export_zip(
    from_id="", to_id="", from_dt="", to_dt="",
    include_card=True, include_thumb=True,
    include_face=True, include_photo=True,
    exclude_logs=True,
) -> bytes:
    """
    Build a filtered ZIP for USER data only. Logs are NEVER included.

    FIX: Uses CAST(emp_id AS INTEGER) for numeric ID ranges so that
    range 10-20 correctly includes 10,11,12...20 instead of just last ID.

    FIX 2: Fingerprint .bin export now uses GT-521F52 native protocol directly
    (same as fp_transfer_all_in_one.py) instead of the generic Fingerprint wrapper.
    The wrapper did not correctly implement the two-phase response (ACK packet +
    separate DATA packet), so only the first employee's template was read correctly.
    Now a single persistent serial connection reads all templates in one session.
    """
    from pathlib import Path

    def norm_date(s):
        s = (s or "").strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                from datetime import datetime as _dt
                return _dt.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return s

    # ── Determine if we should use numeric or text comparison for emp_id ──
    use_numeric = _is_numeric_id(from_id or "0") and _is_numeric_id(to_id or "0")

    conditions = ["u.emp_id IS NOT NULL", "u.emp_id != ''"]
    params = []

    if from_id:
        if use_numeric:
            conditions.append("CAST(TRIM(u.emp_id) AS INTEGER) >= ?")
            params.append(int(str(from_id).strip()))
        else:
            conditions.append("TRIM(u.emp_id) >= ?")
            params.append(str(from_id).strip())

    if to_id:
        if use_numeric:
            conditions.append("CAST(TRIM(u.emp_id) AS INTEGER) <= ?")
            params.append(int(str(to_id).strip()))
        else:
            conditions.append("TRIM(u.emp_id) <= ?")
            params.append(str(to_id).strip())

    if from_dt:
        conditions.append("SUBSTR(COALESCE(u.created_at,''), 1, 10) >= ?")
        params.append(norm_date(from_dt))
    if to_dt:
        conditions.append("SUBSTR(COALESCE(u.created_at,''), 1, 10) <= ?")
        params.append(norm_date(to_dt))

    # ── Biometric presence filter ──────────────────────────────────────────
    presence_conditions = []
    if include_face:
        presence_conditions.append(
            "(u.encoding_path IS NOT NULL AND u.encoding_path != '')"
        )
    if include_photo:
        presence_conditions.append(
            "(u.image_path IS NOT NULL AND u.image_path != '')"
        )
    if include_thumb:
        presence_conditions.append(
            "EXISTS (SELECT 1 FROM fingerprint_map fm WHERE fm.emp_id = u.emp_id)"
        )
    if include_card:
        presence_conditions.append(
            "EXISTS (SELECT 1 FROM rfid_card_map rc WHERE rc.emp_id = u.emp_id)"
        )

    if presence_conditions:
        conditions.append("(" + " OR ".join(presence_conditions) + ")")

    where_sql = " AND ".join(conditions)

    print(f"[EXPORT] Query: SELECT * FROM users WHERE {where_sql} | params={params}")

    # ── Raw sqlite3 connection for pandas ─────────────────────────────────
    raw_conn = get_raw_sqlite_connection()
    try:
        users_df = pd.read_sql_query(
            f"SELECT u.* FROM users u WHERE {where_sql}",
            raw_conn,
            params=params
        )
        matched_emp_ids = users_df["emp_id"].tolist() if not users_df.empty else []
        print(f"[EXPORT] Matched {len(matched_emp_ids)} users: {matched_emp_ids[:10]}")

        fp_df   = pd.DataFrame()
        rfid_df = pd.DataFrame()

        if matched_emp_ids:
            placeholders = ",".join(["?"] * len(matched_emp_ids))

            if include_thumb:
                try:
                    fp_df = pd.read_sql_query(
                        f"SELECT * FROM fingerprint_map WHERE emp_id IN ({placeholders})",
                        raw_conn, params=matched_emp_ids
                    )
                except Exception as e:
                    print(f"[EXPORT] fingerprint_map query error: {e}")

            if include_card:
                try:
                    rfid_df = pd.read_sql_query(
                        f"SELECT * FROM rfid_card_map WHERE emp_id IN ({placeholders})",
                        raw_conn, params=matched_emp_ids
                    )
                except Exception as e:
                    print(f"[EXPORT] rfid_card_map query error: {e}")

    finally:
        raw_conn.close()

    # ── Helpers ───────────────────────────────────────────────────────────
    def df_to_xlsx(df, sheet_name="Sheet1"):
        buf = io.BytesIO()
        df2 = df.copy()
        if "emp_id" in df2.columns:
            df2["emp_id"] = df2["emp_id"].astype(str)
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df2.to_excel(w, index=False, sheet_name=sheet_name)
        buf.seek(0)
        return buf.read()

    def df_to_csv(df):
        return df.to_csv(index=False).encode("utf-8")

    # ── Build ZIP ─────────────────────────────────────────────────────────
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:

        # users.xlsx / users.csv — always included
        if not users_df.empty:
            zf.writestr("users.xlsx", df_to_xlsx(users_df, "users"))
            zf.writestr("users.csv",  df_to_csv(users_df))

        # Face encodings — only when Face checkbox selected
        if include_face and matched_emp_ids:
            face_enc_dir = Path("face_encodings")
            added = 0
            for emp_id in matched_emp_ids:
                db_row = users_df[users_df["emp_id"] == emp_id]
                stored_path = ""
                if not db_row.empty and "encoding_path" in db_row.columns:
                    stored_path = str(db_row.iloc[0].get("encoding_path") or "").strip()

                enc_file = None
                if stored_path:
                    p = Path(stored_path)
                    if not p.is_absolute():
                        p = Path.cwd() / p
                    if p.exists():
                        enc_file = p

                if enc_file is None:
                    fallback = face_enc_dir / f"{emp_id}.dat"
                    if fallback.exists():
                        enc_file = fallback

                if enc_file:
                    zf.write(enc_file, f"face_encodings/{emp_id}.dat")
                    added += 1
                else:
                    print(f"[EXPORT] No face encoding for {emp_id}")
            print(f"[EXPORT] Added {added}/{len(matched_emp_ids)} face encodings")

        # Face photos — only when Photo checkbox selected
        if include_photo and matched_emp_ids:
            users_img_dir = Path("users_img")
            added = 0
            for emp_id in matched_emp_ids:
                db_row = users_df[users_df["emp_id"] == emp_id]
                stored_img = ""
                if not db_row.empty and "image_path" in db_row.columns:
                    stored_img = str(db_row.iloc[0].get("image_path") or "").strip()

                img_file = None
                if stored_img:
                    p = Path(stored_img)
                    if not p.is_absolute():
                        p = Path.cwd() / p
                    if p.exists():
                        img_file = p

                if img_file is None and users_img_dir.exists():
                    for ext in (".jpg", ".jpeg", ".png"):
                        candidate = users_img_dir / f"{emp_id}{ext}"
                        if candidate.exists():
                            img_file = candidate
                            break

                if img_file:
                    zf.write(img_file, f"users_img/{emp_id}{img_file.suffix}")
                    added += 1
                else:
                    print(f"[EXPORT] No photo for {emp_id}")
            print(f"[EXPORT] Added {added}/{len(matched_emp_ids)} photos")

        # ── Fingerprint data — only when Thumb checkbox selected ──────────
        # Exports: fingerprint_map.xlsx + fingerprint_map.csv + fingerprint_bins/*.bin
        if include_thumb:
            # fingerprint_map spreadsheet
            if not fp_df.empty:
                zf.writestr("fingerprint_map.xlsx", df_to_xlsx(fp_df, "fingerprint_map"))
                zf.writestr("fingerprint_map.csv",  df_to_csv(fp_df))
                print(f"[EXPORT] Added fingerprint_map with {len(fp_df)} records")

            if matched_emp_ids:
                # ── Step A: Query fingerprint_map for all matched employees ──
                emp_id_tuple    = tuple(str(e) for e in matched_emp_ids)
                placeholders_fp = ",".join(["?"] * len(emp_id_tuple))

                raw_conn_fp = get_raw_sqlite_connection()
                try:
                    try:
                        fp_rows = raw_conn_fp.execute(
                            f"SELECT emp_id, template_id FROM fingerprint_map "
                            f"WHERE emp_id IN ({placeholders_fp}) "
                            f"AND template_id IS NOT NULL "
                            f"ORDER BY CAST(emp_id AS INTEGER)",
                            emp_id_tuple
                        ).fetchall()
                    except Exception:
                        fp_rows = raw_conn_fp.execute(
                            f"SELECT emp_id, template_id FROM fingerprint_map "
                            f"WHERE emp_id IN ({placeholders_fp}) "
                            f"AND template_id IS NOT NULL "
                            f"ORDER BY emp_id",
                            emp_id_tuple
                        ).fetchall()
                except Exception as e_fq:
                    print(f"[EXPORT] .bin fingerprint_map query error: {e_fq}")
                    fp_rows = []
                finally:
                    raw_conn_fp.close()

                print(f"[EXPORT] .bin: {len(fp_rows)} employees have fingerprints "
                      f"(from {len(matched_emp_ids)} matched users)")

                # ── Step B: Fetch templates using GT-521F52 native protocol ──
                #
                # ROOT CAUSE OF ORIGINAL bug:
                # The generic Fingerprint wrapper's get_template() only reads the
                # first 12-byte ACK/NACK response packet, but the GT-521F52 sends
                # TWO separate packets for GetTemplate:
                #   1. A 12-byte response packet (55 AA ... Ack/Nack)
                #   2. A separate DATA packet    (5A A5 ... 498 bytes ... checksum)
                #
                # The wrapper never read packet #2, leaving 502 bytes sitting in
                # the UART buffer. The next employee's CheckEnrolled command then
                # read those stale bytes instead of a real response — causing all
                # subsequent get_template() calls to fail or return garbage.
                #
                # FIX: Use GT521F52 class directly (same as fp_transfer_all_in_one.py)
                # with ONE persistent serial connection for the entire export loop.
                # The native protocol correctly reads both packets per template.

                bin_added   = 0
                bin_failed  = 0
                bin_skipped = 0

                # ── Inline GT-521F52 constants (matches fp_transfer_all_in_one.py) ──
                _PKT_RES       = b"\x55\xAA"
                _PKT_DATA      = b"\x5A\xA5"
                _DEVICE_ID     = 0x0001
                _TEMPLATE_SIZE = 498
                _RESP_TIMEOUT  = 2.5
                _DATA_TIMEOUT  = 8.0
                _CMD_ACK       = 0x30
                _CMD_OPEN      = 0x01
                _CMD_CHECK     = 0x21
                _CMD_GET_TPL   = 0x70

                def _chk16(data):
                    return sum(data) & 0xFFFF

                def _send_cmd(ser, cmd_code, param=0):
                    pkt = bytearray(12)
                    pkt[0:2]  = _PKT_RES
                    pkt[2:4]  = _DEVICE_ID.to_bytes(2, "little")
                    pkt[4:8]  = (param & 0xFFFFFFFF).to_bytes(4, "little")
                    pkt[8:10] = (cmd_code & 0xFFFF).to_bytes(2, "little")
                    pkt[10:12] = _chk16(pkt[:10]).to_bytes(2, "little")
                    ser.write(bytes(pkt))
                    ser.flush()

                def _read_exact(ser, n, timeout):
                    import time as _time
                    end = _time.time() + timeout
                    buf = bytearray()
                    while len(buf) < n:
                        chunk = ser.read(n - len(buf))
                        if chunk:
                            buf.extend(chunk)
                            continue
                        if _time.time() > end:
                            raise TimeoutError(
                                f"read_exact timeout: wanted={n} got={len(buf)}"
                            )
                    return bytes(buf)

                def _sync_to(ser, pat, timeout):
                    import time as _time
                    end  = _time.time() + timeout
                    win  = bytearray()
                    plen = len(pat)
                    while _time.time() < end:
                        b = ser.read(1)
                        if not b:
                            continue
                        win += b
                        if len(win) > plen:
                            win = win[-plen:]
                        if bytes(win) == pat:
                            return
                    raise TimeoutError(f"sync_to timeout for {pat.hex()}")

                def _read_response(ser):
                    _sync_to(ser, _PKT_RES, _RESP_TIMEOUT)
                    body = _read_exact(ser, 10, _RESP_TIMEOUT)
                    pkt  = _PKT_RES + body
                    rx_chk   = int.from_bytes(pkt[10:12], "little")
                    calc_chk = _chk16(pkt[:10])
                    if rx_chk != calc_chk:
                        raise ValueError(
                            f"Response checksum mismatch: rx={rx_chk:#06x} "
                            f"calc={calc_chk:#06x}"
                        )
                    param = int.from_bytes(pkt[4:8],  "little")
                    resp  = int.from_bytes(pkt[8:10], "little")
                    return resp == _CMD_ACK, param

                def _read_data_packet(ser):
                    _sync_to(ser, _PKT_DATA, _DATA_TIMEOUT)
                    dev_b = _read_exact(ser, 2, _DATA_TIMEOUT)
                    dev   = int.from_bytes(dev_b, "little")
                    if dev != _DEVICE_ID:
                        raise ValueError(
                            f"Data packet DeviceID mismatch: got={dev} "
                            f"expected={_DEVICE_ID}"
                        )
                    data    = _read_exact(ser, _TEMPLATE_SIZE, _DATA_TIMEOUT)
                    chk_b   = _read_exact(ser, 2, _DATA_TIMEOUT)
                    rx_chk  = int.from_bytes(chk_b, "little")
                    calc    = _chk16(_PKT_DATA + dev_b + data)
                    if rx_chk != calc:
                        raise ValueError(
                            f"Data checksum mismatch: rx={rx_chk:#06x} "
                            f"calc={calc:#06x}"
                        )
                    return data

                # ── Auto-detect serial port (same logic as get_fingerprint_sensor) ──
                import serial as _serial
                import glob   as _glob

                def _detect_port():
                    for p in ("/dev/ttyUSB0", "/dev/ttyUSB1",
                              "/dev/ttyACM0", "/dev/ttyACM1"):
                        if os.path.exists(p):
                            return p
                    candidates = (
                        _glob.glob("/dev/ttyUSB*") +
                        _glob.glob("/dev/ttyACM*")
                    )
                    return candidates[0] if candidates else "/dev/ttyUSB0"

                fp_port = _detect_port()
                print(f"[EXPORT] .bin: opening dedicated serial connection on {fp_port}")

                try:
                    ser = _serial.Serial(
                        fp_port, 9600,
                        timeout=0.05,
                        write_timeout=2.0
                    )
                    time.sleep(0.2)
                    ser.reset_input_buffer()
                    ser.reset_output_buffer()

                    # Send Open command once for the whole session
                    _send_cmd(ser, _CMD_OPEN, 0)
                    open_ack, _ = _read_response(ser)
                    if not open_ack:
                        print("[EXPORT] .bin: sensor Open() NACK — skipping binary export")
                        ser.close()
                        fp_rows = []   # skip the loop below
                    else:
                        print("[EXPORT] .bin: sensor opened OK")

                    for fp_row in fp_rows:
                        # ── Parse DB row ──────────────────────────────────
                        try:
                            if hasattr(fp_row, "keys"):
                                fp_emp_id  = str(fp_row["emp_id"])
                                fp_tmpl_id = int(fp_row["template_id"])
                            else:
                                fp_emp_id  = str(fp_row[0])
                                fp_tmpl_id = int(fp_row[1])
                        except Exception as e_parse:
                            print(f"[EXPORT] .bin row parse error: {e_parse}")
                            bin_skipped += 1
                            continue

                        tpl_data = None
                        try:
                            # ── CheckEnrolled ─────────────────────────────
                            _send_cmd(ser, _CMD_CHECK, fp_tmpl_id)
                            chk_ack, _ = _read_response(ser)

                            if not chk_ack:
                                print(f"[EXPORT] .bin skip: emp={fp_emp_id} "
                                      f"slot={fp_tmpl_id} not enrolled on sensor")
                                bin_skipped += 1
                                time.sleep(0.1)
                                continue

                            # ── GetTemplate ───────────────────────────────
                            # Sends command → reads ACK packet → reads DATA packet
                            _send_cmd(ser, _CMD_GET_TPL, fp_tmpl_id)
                            get_ack, _ = _read_response(ser)

                            if not get_ack:
                                print(f"[EXPORT] .bin GetTemplate NACK: "
                                      f"emp={fp_emp_id} slot={fp_tmpl_id}")
                                bin_failed += 1
                                time.sleep(0.1)
                                continue

                            # Read the DATA packet (498 bytes + header + checksum)
                            tpl_data = _read_data_packet(ser)

                        except Exception as e_row:
                            print(f"[EXPORT] .bin error emp={fp_emp_id} "
                                  f"slot={fp_tmpl_id}: {e_row}")
                            bin_failed += 1
                            # Flush stale bytes before next employee
                            try:
                                ser.reset_input_buffer()
                                ser.reset_output_buffer()
                            except Exception:
                                pass
                            time.sleep(0.3)
                            continue

                        # ── Write to ZIP (outside serial I/O) ─────────────
                        if tpl_data and len(tpl_data) == _TEMPLATE_SIZE:
                            bin_filename = f"fingerprint_bins/{fp_emp_id}.bin"
                            zf.writestr(bin_filename, bytes(tpl_data))
                            bin_added += 1
                            print(f"[EXPORT] .bin OK: {bin_filename} "
                                  f"slot={fp_tmpl_id} size={len(tpl_data)}B")
                        else:
                            bin_failed += 1
                            sz = len(tpl_data) if tpl_data else 0
                            print(f"[EXPORT] .bin wrong size: "
                                  f"emp={fp_emp_id} slot={fp_tmpl_id} got={sz}B "
                                  f"expected={_TEMPLATE_SIZE}B")

                        # Match fp_transfer_all_in_one.py inter-employee delay
                        time.sleep(0.2)

                    # Close dedicated serial port
                    try:
                        ser.close()
                    except Exception:
                        pass

                except Exception as e_serial:
                    print(f"[EXPORT] .bin serial open/init error: {e_serial}")

                print(f"[EXPORT] .bin summary: "
                      f"added={bin_added} failed={bin_failed} skipped={bin_skipped}")

            else:
                print("[EXPORT] .bin: no matched employees — skipping sensor reads")

        # RFID data — only when Card checkbox selected
        if include_card and not rfid_df.empty:
            zf.writestr("rfid_card_map.xlsx", df_to_xlsx(rfid_df, "rfid_card_map"))
            zf.writestr("rfid_card_map.csv",  df_to_csv(rfid_df))
            print(f"[EXPORT] Added rfid_card_map with {len(rfid_df)} records")

    zbuf.seek(0)
    return zbuf.read()



# ─────────────────────────────────────────────────────────────────────────────
# Helper: normalise a date string to YYYY-MM-DD (returns "" on failure)
# ─────────────────────────────────────────────────────────────────────────────
def _norm_date_import(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d",
                "%d/%m/%y", "%d-%m-%y"):
        try:
            from datetime import datetime as _dt
            return _dt.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return s[:10]  # best-effort
 
 
def _emp_in_range(emp_id: str, from_id: str, to_id: str) -> bool:
    """
    Return True if emp_id falls within [from_id, to_id].
    Supports both numeric and alphanumeric IDs.
    Empty from_id / to_id means unbounded.
    """
    if not emp_id:
        return False
    # numeric comparison when all three are purely numeric
    def _is_num(v): 
        try:
            int(str(v).strip()); return True
        except Exception:
            return False
 
    if _is_num(emp_id) and (not from_id or _is_num(from_id)) and (not to_id or _is_num(to_id)):
        n = int(str(emp_id).strip())
        if from_id and int(str(from_id).strip()) > n:
            return False
        if to_id and int(str(to_id).strip()) < n:
            return False
        return True
    else:
        if from_id and str(emp_id).strip() < str(from_id).strip():
            return False
        if to_id and str(emp_id).strip() > str(to_id).strip():
            return False
        return True
 
 
def _date_in_range(date_str: str, from_dt: str, to_dt: str) -> bool:
    """Return True if date_str (first 10 chars) is within [from_dt, to_dt]."""
    d = (date_str or "")[:10]
    if from_dt and d and d < _norm_date_import(from_dt):
        return False
    if to_dt and d and d > _norm_date_import(to_dt):
        return False
    return True
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Route 1: /api/get_users_from_device_a
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/get_users_from_device_a", methods=["POST"])
def api_get_users_from_device_a():
    """
    Read the ZIP that the phone uploaded via QR handoff, apply filters
    (emp_id range, date range, checkbox flags) and return a structured
    payload of users + logs + biometric blobs for the sync step.
 
    POST JSON:
        handoff_token   str   – the QR token (e.g. "import-abc123")
        from_id         str   – optional lower emp_id bound
        to_id           str   – optional upper emp_id bound
        from_dt         str   – optional lower date bound  (DD/MM/YYYY or YYYY-MM-DD)
        to_dt           str   – optional upper date bound
        include_card    bool
        include_thumb   bool
        include_face    bool
        include_photo   bool
    """
    data          = request.get_json(force=True) or {}
    token         = (data.get("handoff_token") or "").strip()
    from_id       = (data.get("from_id")  or "").strip()
    to_id         = (data.get("to_id")    or "").strip()
    from_dt       = (data.get("from_dt")  or "").strip()
    to_dt         = (data.get("to_dt")    or "").strip()
    inc_card      = bool(data.get("include_card",  True))
    inc_thumb     = bool(data.get("include_thumb", True))
    inc_face      = bool(data.get("include_face",  True))
    inc_photo     = bool(data.get("include_photo", True))
 
    # ── Validate token ────────────────────────────────────────────────────
    blob = get_setting(f"handoff:{token}")
    if not blob:
        return jsonify(success=False, message="Invalid or expired QR token."), 403
    try:
        state = json.loads(blob)
    except Exception:
        return jsonify(success=False, message="Corrupt token state."), 403
 
    if state.get("mode") != "import":
        return jsonify(success=False, message="Token is not for import."), 403
 
    zip_path = state.get("file_path") or ""
    if not zip_path or not os.path.isfile(zip_path):
        return jsonify(success=False, message="Uploaded file not found. Please re-scan QR."), 404
 
    # ── Read ZIP ──────────────────────────────────────────────────────────
    users        = []   # list of user dicts
    logs         = []   # list of log dicts
    face_encs    = {}   # emp_id -> base64 .dat bytes
    face_imgs    = {}   # emp_id -> base64 jpg bytes
    fp_bins      = {}   # emp_id -> base64 .bin bytes  (NEW: .bin only)
    fp_map_rows  = {}   # emp_id -> {template_id, name}
    rfid_rows    = {}   # emp_id -> {rfid_card, name}
 
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
 
            # ── users spreadsheet ─────────────────────────────────────────
            for xlsx_name in ("users.xlsx", "users.csv"):
                if xlsx_name in names:
                    with zf.open(xlsx_name) as f:
                        try:
                            if xlsx_name.endswith(".xlsx"):
                                df = pd.read_excel(f, dtype=str)
                            else:
                                df = pd.read_csv(f, dtype=str)
                            df.fillna("", inplace=True)
                            for _, row in df.iterrows():
                                eid = str(row.get("emp_id", "")).strip()
                                if not eid:
                                    continue
                                if not _emp_in_range(eid, from_id, to_id):
                                    continue
                                created = str(row.get("created_at", ""))
                                if not _date_in_range(created, from_dt, to_dt):
                                    continue
                                users.append({
                                    "emp_id":    eid,
                                    "name":      str(row.get("name", "")),
                                    "role":      str(row.get("role", "User")),
                                    "birthdate": str(row.get("birthdate", "")),
                                    "created_at": created,
                                    "updated_at": str(row.get("updated_at", "")),
                                })
                        except Exception as e:
                            print(f"[IMPORT_A] parse {xlsx_name} error: {e}")
                    break  # prefer .xlsx over .csv
 
            # ── logs spreadsheet ──────────────────────────────────────────
            print(f"[IMPORT_A] ZIP contents: {names}")
            print(f"[IMPORT_A] Filters: from_id={from_id} to_id={to_id} from_dt={from_dt} to_dt={to_dt}")
            for log_name in ("logs.xlsx", "logs.csv", "canteen_logs.csv"):
                if log_name in names:
                    with zf.open(log_name) as f:
                        try:
                            if log_name.endswith(".xlsx"):
                                df = pd.read_excel(f, dtype=str)
                            else:
                                df = pd.read_csv(f, dtype=str)
                            df.fillna("", inplace=True)
                            for _, row in df.iterrows():
                                eid = str(row.get("emp_id", "")).strip()
                                if not eid:
                                    continue
                                if not _emp_in_range(eid, from_id, to_id):
                                    continue
                                ts = str(row.get("ts", ""))
                                if not _date_in_range(ts, from_dt, to_dt):
                                    continue
                                logs.append({
                                    "emp_id":    eid,
                                    "name":      str(row.get("name", "")),
                                    "device_id": str(row.get("device_id", "")),
                                    "mode":      str(row.get("mode", "")),
                                    "ts":        ts,
                                    "success":   str(row.get("success", "1")),
                                    "item_name": str(row.get("item_name", "")),
                                })
                        except Exception as e:
                            print(f"[IMPORT_A] parse {log_name} error: {e}")
                    break
 
            # ── fingerprint_map spreadsheet ───────────────────────────────
            for fp_name in ("fingerprint_map.xlsx", "fingerprint_map.csv"):
                if fp_name in names:
                    with zf.open(fp_name) as f:
                        try:
                            df = pd.read_excel(f, dtype=str) if fp_name.endswith(".xlsx") else pd.read_csv(f, dtype=str)
                            df.fillna("", inplace=True)
                            for _, row in df.iterrows():
                                eid = str(row.get("emp_id", "")).strip()
                                if eid and _emp_in_range(eid, from_id, to_id):
                                    fp_map_rows[eid] = {
                                        "template_id": str(row.get("template_id", "")),
                                        "name": str(row.get("name", "")),
                                    }
                        except Exception as e:
                            print(f"[IMPORT_A] parse {fp_name} error: {e}")
                    break
 
            # ── rfid_card_map spreadsheet ─────────────────────────────────
            for rfid_name in ("rfid_card_map.xlsx", "rfid_card_map.csv"):
                if rfid_name in names:
                    with zf.open(rfid_name) as f:
                        try:
                            df = pd.read_excel(f, dtype=str) if rfid_name.endswith(".xlsx") else pd.read_csv(f, dtype=str)
                            df.fillna("", inplace=True)
                            for _, row in df.iterrows():
                                eid = str(row.get("emp_id", "")).strip()
                                if eid and _emp_in_range(eid, from_id, to_id):
                                    rfid_rows[eid] = {
                                        "rfid_card": str(row.get("rfid_card", "")),
                                        "name": str(row.get("name", "")),
                                    }
                        except Exception as e:
                            print(f"[IMPORT_A] parse {rfid_name} error: {e}")
                    break
 
            # ── Biometric files (filter to matched emp_ids from users list) ─
            valid_eids = {u["emp_id"] for u in users}
 
            if inc_face:
                for member in names:
                    if member.startswith("face_encodings/") and member.endswith(".dat"):
                        eid = os.path.splitext(os.path.basename(member))[0]
                        if eid in valid_eids:
                            face_encs[eid] = base64.b64encode(zf.read(member)).decode()
 
            if inc_photo:
                for member in names:
                    if member.startswith("users_img/"):
                        eid = os.path.splitext(os.path.basename(member))[0]
                        if eid in valid_eids:
                            face_imgs[eid] = base64.b64encode(zf.read(member)).decode()
 
            if inc_thumb:
                # NEW: .bin files only — ignore .dat fingerprint files
                for member in names:
                    if member.startswith("fingerprint_bins/") and member.endswith(".bin"):
                        eid = os.path.splitext(os.path.basename(member))[0]
                        if eid in valid_eids:
                            fp_bins[eid] = base64.b64encode(zf.read(member)).decode()
 
            # Filter rfid / fp_map to valid eids
            if not inc_card:
                rfid_rows = {}
            else:
                rfid_rows = {k: v for k, v in rfid_rows.items() if k in valid_eids}
 
            if not inc_thumb:
                fp_map_rows = {}
                fp_bins = {}
            else:
                fp_map_rows = {k: v for k, v in fp_map_rows.items() if k in valid_eids}
 
    except zipfile.BadZipFile:
        return jsonify(success=False, message="Uploaded file is not a valid ZIP."), 400
    except Exception as e:
        print(f"[IMPORT_A] ZIP read error: {e}")
        import traceback; traceback.print_exc()
        return jsonify(success=False, message=f"Failed to read uploaded file: {e}"), 500
 
    # Attach per-user biometric blobs
    for u in users:
        eid = u["emp_id"]
        if inc_face:
            u["face_encoding_b64"] = face_encs.get(eid, "")
        if inc_photo:
            u["face_image_b64"] = face_imgs.get(eid, "")
        if inc_thumb:
            u["fingerprint_bin_b64"] = fp_bins.get(eid, "")
            fp_info = fp_map_rows.get(eid, {})
            u["src_template_id"] = fp_info.get("template_id", "")
        if inc_card:
            rfid_info = rfid_rows.get(eid, {})
            u["rfid_card"] = rfid_info.get("rfid_card", "")
 
    print(f"[IMPORT_A] Returning {len(users)} users, {len(logs)} logs from Device A ZIP")
    return jsonify(success=True, users=users, logs=logs)
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Route 2: /api/import_sync_users
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/import_sync_users", methods=["POST"])
def api_import_sync_users():
    """
    Timestamp-aware sync from Device A payload into Device B (local) database.
 
    Rules:
      1. Users (profile): INSERT OR UPDATE (upsert) — always keep latest updated_at.
      2. Logs: per emp_id+ts uniqueness — keep the record with the GREATER ts.
         (Device A ts > Device B ts → Device A wins, else skip.)
      3. Face encoding / image: import if inc_face / inc_photo and not already present
         (or if Device A record is newer).
      4. Fingerprint (.bin):
         a. If emp_id ALREADY has a Template ID in fingerprint_map → SKIP (do not overwrite).
         b. If emp_id exists but has NO Template ID → assign lowest free Template ID → import.
      5. RFID card: INSERT OR IGNORE (do not overwrite existing card).
 
    POST JSON (same as frontend sends):
        handoff_token   str
        from_id / to_id / from_dt / to_dt / include_*  (filters, for validation)
        users           list   – output of /api/get_users_from_device_a
        logs            list   – output of /api/get_users_from_device_a  (optional)
    """
    data          = request.get_json(force=True) or {}
    token         = (data.get("handoff_token") or "").strip()
    users_in      = data.get("users") or []
    logs_in       = data.get("logs")  or []
    inc_card      = bool(data.get("include_card",  True))
    inc_thumb     = bool(data.get("include_thumb", True))
    inc_face      = bool(data.get("include_face",  True))
    inc_photo     = bool(data.get("include_photo", True))
 
    # ── Validate token ────────────────────────────────────────────────────
    blob = get_setting(f"handoff:{token}")
    if not blob:
        return jsonify(success=False, message="Invalid or expired QR token."), 403
    try:
        state = json.loads(blob)
    except Exception:
        return jsonify(success=False, message="Corrupt token state."), 403
    if state.get("mode") != "import":
        return jsonify(success=False, message="Token is not for import."), 403
 
    # ── Prepare result tracking ───────────────────────────────────────────
    results  = []   # per-user log entries for the modal table
    summary  = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}
 
    conn = get_db_connection()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
 
    def _result(emp_id, name, action, reason=""):
        results.append({"user_id": emp_id, "name": name,
                         "action": action, "reason": reason})
        summary[action] = summary.get(action, 0) + 1
 
    # ─────────────────────────────────────────────────────────────────────
    # STEP 1 — Sync user profiles
    # ─────────────────────────────────────────────────────────────────────
    for u in users_in:
        emp_id = (u.get("emp_id") or "").strip()
        name   = (u.get("name")   or "").strip()
        if not emp_id:
            continue
 
        try:
            existing = conn.execute(
                "SELECT emp_id, updated_at FROM users WHERE emp_id=?", (emp_id,)
            ).fetchone()
 
            src_updated = (u.get("updated_at") or u.get("created_at") or "")[:19]
            dst_updated = (existing["updated_at"] if existing and existing["updated_at"] else "")[:19]
 
            if not existing:
                # New user — create
                conn.execute(
                    """INSERT INTO users (emp_id, name, role, birthdate, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (emp_id, name,
                     u.get("role", "User"),
                     u.get("birthdate", ""),
                     u.get("created_at", now_str) or now_str,
                     src_updated or now_str)
                )
                action_tag = "created"
 
            elif src_updated and dst_updated and src_updated > dst_updated:
                # Device A is newer — update
                conn.execute(
                    """UPDATE users SET name=?, role=?, birthdate=?, updated_at=?
                       WHERE emp_id=?""",
                    (name, u.get("role", "User"), u.get("birthdate", ""),
                     src_updated, emp_id)
                )
                action_tag = "updated"
 
            else:
                action_tag = "skipped"
 
            conn.commit()
 
            # ── Face encoding ─────────────────────────────────────────────
            if inc_face and u.get("face_encoding_b64"):
                try:
                    enc_bytes = base64.b64decode(u["face_encoding_b64"])
                    if enc_bytes:
                        save_encoding(emp_id, enc_bytes)
                        conn.execute(
                            "UPDATE users SET encoding_path=? WHERE emp_id=?",
                            (f"face_encodings/{emp_id}.dat", emp_id)
                        )
                        conn.commit()
                except Exception as e:
                    print(f"[IMPORT_SYNC] face_enc error {emp_id}: {e}")
 
            # ── Face photo ────────────────────────────────────────────────
            if inc_photo and u.get("face_image_b64"):
                try:
                    img_bytes = base64.b64decode(u["face_image_b64"])
                    if img_bytes:
                        save_image(emp_id, img_bytes)
                        conn.execute(
                            "UPDATE users SET image_path=? WHERE emp_id=?",
                            (f"users_img/{emp_id}.jpg", emp_id)
                        )
                        conn.commit()
                except Exception as e:
                    print(f"[IMPORT_SYNC] face_img error {emp_id}: {e}")
 
            # ── RFID card — INSERT OR IGNORE (never overwrite) ────────────
            if inc_card and u.get("rfid_card"):
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO rfid_card_map
                           (emp_id, rfid_card, name, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        (emp_id, u["rfid_card"], name, now_str, now_str)
                    )
                    conn.commit()
                except Exception as e:
                    print(f"[IMPORT_SYNC] rfid error {emp_id}: {e}")
 
            # ── Fingerprint .bin — conflict-avoidance logic ───────────────
            if inc_thumb and u.get("fingerprint_bin_b64"):
                _import_fingerprint_bin(conn, emp_id, name, u["fingerprint_bin_b64"])
 
            _result(emp_id, name, action_tag)
 
        except Exception as e:
            print(f"[IMPORT_SYNC] user error {emp_id}: {e}")
            _result(emp_id, name, "error", str(e)[:80])
 
    # ─────────────────────────────────────────────────────────────────────
    # STEP 2 — Sync logs (timestamp-aware: keep latest)
    # ─────────────────────────────────────────────────────────────────────
    logs_created = 0
    logs_skipped = 0
    for log in logs_in:
        emp_id = (log.get("emp_id") or "").strip()
        ts_src = (log.get("ts") or "")[:19]
        if not emp_id or not ts_src:
            continue
        try:
            existing_log = conn.execute(
                "SELECT ts FROM logs WHERE emp_id=? AND ts=?", (emp_id, ts_src)
            ).fetchone()
            if existing_log:
                logs_skipped += 1
                continue
 
            # Check if Device B has a later record for this employee on this date
            date_prefix = ts_src[:10]
            later = conn.execute(
                """SELECT ts FROM logs
                   WHERE emp_id=? AND SUBSTR(ts,1,10)=? AND ts > ?
                   LIMIT 1""",
                (emp_id, date_prefix, ts_src)
            ).fetchone()
            if later:
                # Device B already has a newer record for the same day — skip
                logs_skipped += 1
                continue
 
            conn.execute(
                """INSERT INTO logs (emp_id, name, device_id, mode, ts, success, created_at, item_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (emp_id,
                 log.get("name", ""),
                 log.get("device_id", ""),
                 log.get("mode", ""),
                 ts_src,
                 int(str(log.get("success", "1")).strip() or "1"),
                 now_str,
                 log.get("item_name", ""))
            )
            logs_created += 1
        except Exception as e:
            print(f"[IMPORT_SYNC] log error {emp_id}: {e}")
 
    if logs_in:
        conn.execute("COMMIT") if not conn._conn.in_transaction else None  # type: ignore
        try:
            conn.commit()
        except Exception:
            pass
 
    # ─────────────────────────────────────────────────────────────────────
    # STEP 3 — Reload face recognizer if face data was imported
    # ─────────────────────────────────────────────────────────────────────
    if inc_face and summary.get("created", 0) + summary.get("updated", 0) > 0:
        try:
            recognizer.load_all_encodings()
        except Exception as e:
            print(f"[IMPORT_SYNC] recognizer reload warning: {e}")
 
    total = len(users_in)
    return jsonify(
        success=True,
        message=f"Sync complete. {total} user(s) processed, "
                f"{logs_created} log(s) imported ({logs_skipped} skipped).",
        summary=summary,
        results=results,
        logs_created=logs_created,
        logs_skipped=logs_skipped,
    )
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Helper: import a single fingerprint .bin template with conflict-avoidance
# ─────────────────────────────────────────────────────────────────────────────
def _import_fingerprint_bin(conn, emp_id: str, name: str, bin_b64: str):
    """
    Import a fingerprint .bin template for emp_id.
 
    Rules:
      • If fingerprint_map already has a template_id for this emp_id → SKIP.
      • If no template_id → find lowest free slot → write .bin file →
        upload to sensor → insert into fingerprint_map.
    """
    TEMPLATE_SIZE = 498  # GT-521F52 native template size
 
    try:
        existing = conn.execute(
            "SELECT template_id FROM fingerprint_map WHERE emp_id=?", (emp_id,)
        ).fetchone()
 
        if existing and existing["template_id"] is not None:
            # Rule: already has Template ID → do NOT overwrite
            print(f"[IMPORT_FP] {emp_id} already has template_id={existing['template_id']} — skipping")
            return
 
        # Decode .bin bytes
        try:
            fp_bytes = base64.b64decode(bin_b64)
        except Exception as e:
            print(f"[IMPORT_FP] base64 decode error for {emp_id}: {e}")
            return
 
        if len(fp_bytes) != TEMPLATE_SIZE:
            print(f"[IMPORT_FP] {emp_id}: unexpected .bin size {len(fp_bytes)}, expected {TEMPLATE_SIZE}")
            return
 
        # Find lowest free template slot
        used_ids = set()
        for row in conn.execute("SELECT template_id FROM fingerprint_map WHERE template_id IS NOT NULL"):
            try:
                used_ids.add(int(row[0]))
            except Exception:
                pass
 
        new_tid = 1
        while new_tid in used_ids:
            new_tid += 1
 
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
 
       # ── Save ONLY .bin file — NO .dat ─────────────────────────────────
        bins_dir = os.path.join(os.getcwd(), "fingerprint_bins")
        os.makedirs(bins_dir, exist_ok=True)
        bin_path = os.path.join(bins_dir, f"{emp_id}.bin")
        try:
            with open(bin_path, "wb") as f:
                f.write(fp_bytes)
            print(f"[IMPORT_FP] Saved {bin_path}")
        except Exception as e:
            print(f"[IMPORT_FP] File save error {emp_id}: {e}")
            return

        # ── Insert into fingerprint_map FIRST (before sensor upload) ──────
        conn.execute(
            """INSERT OR REPLACE INTO fingerprint_map
               (emp_id, template_id, name, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (emp_id, new_tid, name or "", now_str, now_str)
        )
        conn.commit()
        print(f"[IMPORT_FP] {emp_id} assigned template_id={new_tid}")

        # ── Upload to sensor using native GT-521F52 two-phase protocol ─────
        # The generic s.set_template() wrapper only reads the first ACK packet
        # and misses the separate DATA packet — causing stale UART bytes for
        # every subsequent employee. Native protocol sends CMD → reads ACK →
        # sends DATA packet → reads final ACK, correctly.
        def _upload_to_sensor(emp_id=emp_id, new_tid=new_tid, fp_bytes=fp_bytes):
            import serial as _serial
            import glob   as _glob
            import time   as _time

            PKT_CMD     = b"\x55\xAA"
            PKT_DATA    = b"\x5A\xA5"
            DEVICE_ID   = 0x0001
            CMD_ACK     = 0x30
            CMD_OPEN    = 0x01
            CMD_SET_TPL = 0x71   # SetTemplate command code

            def chk16(d):
                return sum(d) & 0xFFFF

            def send_cmd(ser, code, param=0):
                pkt = bytearray(12)
                pkt[0:2]   = PKT_CMD
                pkt[2:4]   = DEVICE_ID.to_bytes(2, "little")
                pkt[4:8]   = (param & 0xFFFFFFFF).to_bytes(4, "little")
                pkt[8:10]  = (code & 0xFFFF).to_bytes(2, "little")
                pkt[10:12] = chk16(pkt[:10]).to_bytes(2, "little")
                ser.write(bytes(pkt))
                ser.flush()

            def read_exact(ser, n, timeout=5.0):
                end = _time.time() + timeout
                buf = bytearray()
                while len(buf) < n:
                    chunk = ser.read(n - len(buf))
                    if chunk:
                        buf.extend(chunk)
                    elif _time.time() > end:
                        raise TimeoutError(
                            f"read_exact timeout: want={n} got={len(buf)}"
                        )
                return bytes(buf)

            def sync_to(ser, pat, timeout=5.0):
                end, win, plen = _time.time() + timeout, bytearray(), len(pat)
                while _time.time() < end:
                    b = ser.read(1)
                    if not b:
                        continue
                    win += b
                    if len(win) > plen:
                        win = win[-plen:]
                    if bytes(win) == pat:
                        return
                raise TimeoutError(f"sync_to timeout {pat.hex()}")

            def read_resp(ser):
                sync_to(ser, PKT_CMD, 5.0)
                body = read_exact(ser, 10, 5.0)
                pkt  = PKT_CMD + body
                if chk16(pkt[:10]) != int.from_bytes(pkt[10:12], "little"):
                    raise ValueError("Response checksum mismatch")
                return int.from_bytes(pkt[8:10], "little") == CMD_ACK

            # Auto-detect serial port
            port = "/dev/ttyUSB0"
            for p in ("/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyACM0", "/dev/ttyACM1"):
                if os.path.exists(p):
                    port = p
                    break
            cands = _glob.glob("/dev/ttyUSB*") + _glob.glob("/dev/ttyACM*")
            if cands:
                port = cands[0]

            ser = None
            try:
                ser = _serial.Serial(port, 9600, timeout=0.1, write_timeout=2.0)
                _time.sleep(0.3)
                ser.reset_input_buffer()
                ser.reset_output_buffer()

                # Phase 1: Open sensor
                send_cmd(ser, CMD_OPEN, 0)
                if not read_resp(ser):
                    print(f"[IMPORT_FP] Sensor Open NACK for {emp_id}")
                    return

                # Phase 2: SetTemplate command → ACK
                send_cmd(ser, CMD_SET_TPL, new_tid)
                if not read_resp(ser):
                    print(f"[IMPORT_FP] SetTemplate NACK slot={new_tid}")
                    return

                # Phase 3: Send DATA packet
                # Format: START(2) + DeviceID(2) + Template(498) + Checksum(2) = 504 bytes
                dev_b    = DEVICE_ID.to_bytes(2, "little")
                data_pkt = PKT_DATA + dev_b + bytes(fp_bytes)
                data_pkt += chk16(data_pkt).to_bytes(2, "little")
                ser.write(data_pkt)
                ser.flush()
                _time.sleep(0.2)

                # Phase 4: Final ACK
                if read_resp(ser):
                    print(f"[IMPORT_FP] ✓ Native upload OK: emp={emp_id} slot={new_tid}")
                else:
                    print(f"[IMPORT_FP] ✗ Final NACK: emp={emp_id} slot={new_tid}")

            except Exception as e:
                print(f"[IMPORT_FP] Native upload error {emp_id}: {e}")
            finally:
                try:
                    if ser:
                        ser.close()
                except Exception:
                    pass

        threading.Thread(target=_upload_to_sensor, daemon=True).start()
 
        # ── Insert into fingerprint_map ────────────────────────────────────
        conn.execute(
            """INSERT OR REPLACE INTO fingerprint_map
               (emp_id, template_id, name, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (emp_id, new_tid, name or "", now_str, now_str)
        )
        conn.commit()
        print(f"[IMPORT_FP] {emp_id} assigned template_id={new_tid}")
 
    except Exception as e:
        print(f"[IMPORT_FP] Unexpected error for {emp_id}: {e}")
        import traceback; traceback.print_exc()

# ─────────────────────────────────────────────────────────────────────────────
# REPLACE _build_filtered_datalog_zip with this version
# Key change: uses get_raw_sqlite_connection() instead of get_db_connection()
# ─────────────────────────────────────────────────────────────────────────────

def _build_filtered_datalog_zip(
    from_id="", to_id="", from_dt="", to_dt="",
    include_card=True, include_thumb=True,
    include_face=True, include_photo=True,
    include_in=True, include_out=True,
) -> bytes:
    """
    Build a ZIP containing ONLY attendance logs (logs.xlsx + logs.csv).

    FIX: Uses CAST(emp_id AS INTEGER) for numeric ID ranges so that
    range 10-20 correctly includes 10,11,12...20 instead of just last ID.
    """
    def norm_date(s):
        s = (s or "").strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return s

    # ── Determine numeric vs text comparison ──────────────────────────────
    use_numeric = _is_numeric_id(from_id or "0") and _is_numeric_id(to_id or "0")

    conditions = ["emp_id IS NOT NULL", "emp_id != ''"]
    params = []

    if from_id:
        if use_numeric:
            conditions.append("CAST(TRIM(emp_id) AS INTEGER) >= ?")
            params.append(int(str(from_id).strip()))
        else:
            conditions.append("TRIM(emp_id) >= ?")
            params.append(str(from_id).strip())

    if to_id:
        if use_numeric:
            conditions.append("CAST(TRIM(emp_id) AS INTEGER) <= ?")
            params.append(int(str(to_id).strip()))
        else:
            conditions.append("TRIM(emp_id) <= ?")
            params.append(str(to_id).strip())

    if from_dt:
        conditions.append("SUBSTR(COALESCE(ts,''), 1, 10) >= ?")
        params.append(norm_date(from_dt))
    if to_dt:
        conditions.append("SUBSTR(COALESCE(ts,''), 1, 10) <= ?")
        params.append(norm_date(to_dt))

    # Mode filter
    allowed_modes = []
    if include_face or include_photo:
        allowed_modes.append("face")
    if include_thumb:
        allowed_modes.append("fingerprint")
    if include_card:
        allowed_modes.append("rfid")

    if allowed_modes:
        placeholders_mode = ",".join(["?"] * len(allowed_modes))
        conditions.append(f"LOWER(COALESCE(mode,'')) IN ({placeholders_mode})")
        params.extend([m.lower() for m in allowed_modes])

    # Direction filter
    if include_in and not include_out:
        conditions.append("COALESCE(success,0) = 1")
    elif include_out and not include_in:
        conditions.append("COALESCE(success,0) = 0")

    where_sql = " AND ".join(conditions)

    print(f"[DATALOG_EXPORT] Query: SELECT * FROM logs WHERE {where_sql} | params={params}")

    raw_conn = get_raw_sqlite_connection()
    try:
        logs_df = pd.read_sql_query(
            f"SELECT * FROM logs WHERE {where_sql} ORDER BY ts DESC",
            raw_conn, params=params
        )
        print(f"[DATALOG_EXPORT] Matched {len(logs_df)} log records")
    finally:
        raw_conn.close()

    def df_to_xlsx(df, sheet_name="Sheet1"):
        buf = io.BytesIO()
        df2 = df.copy()
        if "emp_id" in df2.columns:
            df2["emp_id"] = df2["emp_id"].astype(str)
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df2.to_excel(w, index=False, sheet_name=sheet_name)
        buf.seek(0)
        return buf.read()

    def df_to_csv(df):
        return df.to_csv(index=False).encode("utf-8")

    # Always write both files — never skip CSV
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("logs.xlsx", df_to_xlsx(logs_df, "logs"))
        zf.writestr("logs.csv",  df_to_csv(logs_df))

    zbuf.seek(0)
    return zbuf.read()

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: NEW FLASK ENDPOINT — add after api_export_filtered_handoff
# ─────────────────────────────────────────────────────────────────────────────
def _build_canteen_log_filter(data):
    """Build WHERE clauses + params for canteen log filtering."""
    clauses = []
    params = []
    from_id = data.get('from_id', '').strip()
    to_id = data.get('to_id', '').strip()
    from_dt = data.get('from_dt', '').strip()
    to_dt = data.get('to_dt', '').strip()
    if from_id and to_id:
        clauses.append("CAST(emp_id AS INTEGER) BETWEEN ? AND ?")
        params.extend([int(from_id), int(to_id)])
    elif from_id:
        clauses.append("CAST(emp_id AS INTEGER) >= ?")
        params.append(int(from_id))
    elif to_id:
        clauses.append("CAST(emp_id AS INTEGER) <= ?")
        params.append(int(to_id))
    if from_dt:
        parts = from_dt.split('/')
        if len(parts) == 3:
            from_dt = f"{parts[2]}-{parts[1]}-{parts[0]}"
        clauses.append("DATE(ts) >= ?")
        params.append(from_dt)
    if to_dt:
        parts = to_dt.split('/')
        if len(parts) == 3:
            to_dt = f"{parts[2]}-{parts[1]}-{parts[0]}"
        clauses.append("DATE(ts) <= ?")
        params.append(to_dt)
    order_item = data.get('order_item', '').strip()
    menu_code = data.get('menu_code', '').strip()
    slot_code = data.get('slot_code', '').strip()
    shift_code = data.get('shift_code', '').strip()
    if order_item:
        clauses.append("item_name = ?")
        params.append(order_item)
    elif menu_code or slot_code or shift_code:
        try:
            db = get_db_connection()
            if menu_code:
                item_rows = db.execute(
                    "SELECT item_name FROM items WHERE menu_code = ?",
                    (menu_code,)).fetchall()
            elif slot_code:
                item_rows = db.execute(
                    """SELECT i.item_name FROM items i
                       JOIN menu_codes m ON i.menu_code = m.menu_code
                       WHERE m.slot_code = ?""",
                    (slot_code,)).fetchall()
            elif shift_code:
                item_rows = db.execute(
                    """SELECT i.item_name FROM items i
                       JOIN menu_codes m ON i.menu_code = m.menu_code
                       JOIN time_slots ts ON m.slot_code = ts.slot_code
                       WHERE ts.shift_code = ?""",
                    (shift_code,)).fetchall()
            else:
                item_rows = []
            names = [r['item_name'] if hasattr(r, 'keys') else r[0] for r in item_rows]
            if names:
                placeholders = ','.join(['?'] * len(names))
                clauses.append(f"item_name IN ({placeholders})")
                params.extend(names)
            else:
                clauses.append("1 = 0")
        except Exception as e:
            print(f"[filter] Error resolving canteen hierarchy: {e}")
    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params

@app.route('/api/export_filtered_datalog_handoff', methods=['POST'])
def export_filtered_datalog_handoff():
    import csv, io, zipfile, uuid
    data = request.get_json(force=True)
    try:
        where, params = _build_canteen_log_filter(data)
        # Prefix column references with l. since we now alias logs as l
        where = where.replace('emp_id', 'l.emp_id').replace('item_name', 'l.item_name').replace('DATE(ts)', 'DATE(l.ts)')
        conn = get_db_connection()
        rows = conn.execute(
            f"""SELECT l.id, l.emp_id, l.name, l.device_id, l.mode, l.ts,
                       COALESCE(l.item_name, '') AS item_name,
                       COALESCE(s.shift_name, '') AS shift
                FROM logs l
                LEFT JOIN shifts s
                  ON TIME(l.ts) >= TIME(s.from_time)
                 AND TIME(l.ts) <  TIME(s.to_time)
                WHERE {where} ORDER BY l.id""",
            params
        ).fetchall()

        if not rows:
            return jsonify(success=False, message="No logs match the selected filters.")

        # Build CSV
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(['id', 'emp_id', 'name', 'device_id', 'mode', 'ts', 'item_name', 'shift'])
        for r in rows:
            if isinstance(r, dict):
                writer.writerow([r['id'], r['emp_id'], r['name'], r['device_id'], r['mode'], r['ts'], r['item_name'], r['shift']])
            else:
                writer.writerow(list(r))

        csv_bytes = buf.getvalue().encode('utf-8')

        # ZIP it
        token = uuid.uuid4().hex[:12]
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('logs.csv', csv_bytes)
        zip_bytes = zip_buf.getvalue()

        # Store for handoff download
        handoff_dir = os.path.join(os.path.dirname(__file__), 'handoff_files')
        os.makedirs(handoff_dir, exist_ok=True)
        zip_path = os.path.join(handoff_dir, f'{token}.zip')
        with open(zip_path, 'wb') as f:
            f.write(zip_bytes)

       # Store token for handoff system
        base = _qr_base_url()
        handoff_url = f"{base}/handoff/export/{token}"
        state = {
            "mode": "export",
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "status": "ready",
            "have_file": True,
            "prepared_zip": zip_path,
            "original_name": "canteen_logs.zip",
        }
        set_setting(f"handoff:{token}", json.dumps(state))
        qr_url = f"/api/qr_for_handoff/{token}.png"
        # Build summary
        parts = []
        if data.get('shift_name'): parts.append(f"Shift: {data['shift_name']}")
        if data.get('slot_name'): parts.append(f"Slot: {data['slot_name']}")
        if data.get('menu_type'): parts.append(f"Menu: {data['menu_type']}")
        if data.get('order_item'): parts.append(f"Item: {data['order_item']}")
        filter_summary = ' | '.join(parts) if parts else 'All canteen records'

        return jsonify(
            success=True,
            log_count=len(rows),
            zip_size_kb=round(len(zip_bytes) / 1024, 1),
            handoff_url=handoff_url,
            qr_url=qr_url,
            filter_summary=filter_summary
        )
    except Exception as e:
        print(f"[EXPORT] Error: {e}")
        return jsonify(success=False, message=str(e))
# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: NEW FLASK ENDPOINT — Delete filtered logs
#         Add after api_export_filtered_datalog_handoff
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/delete_filtered_logs", methods=["POST"])
def api_delete_filtered_logs():
    data = request.get_json(force=True) or {}
    admin_pw = (data.get("admin_password") or "").strip()

    if not check_admin_password(admin_pw):
        return jsonify({"success": False, "message": "Incorrect admin password."}), 403

    conn = get_db_connection()
    try:
        where, params = _build_canteen_log_filter(data)
        count_row = conn.execute(f"SELECT COUNT(*) FROM logs WHERE {where}", params).fetchone()
        matched = count_row[0] if count_row else 0

        if matched == 0:
            return jsonify({"success": True, "message": "No log records match the selected filters.", "deleted": 0})

        conn.execute(f"DELETE FROM logs WHERE {where}", params)
        conn.commit()
        print(f"[DELETE_FILTERED_LOGS] Deleted {matched} log records")
        return jsonify({"success": True, "message": f"Successfully deleted {matched} log record(s).", "deleted": matched})

    except Exception as e:
        print(f"[DELETE_FILTERED_LOGS] Error: {e}")
        return jsonify({"success": False, "message": f"Delete failed: {str(e)}"}), 500

# -----------------------------------------------------------------------------
# Delete filtered users (used by export.html Delete Users button)
# -----------------------------------------------------------------------------
@app.route("/api/delete_filtered_users", methods=["POST"])
def api_delete_filtered_users():
    """
    Delete users matching filters:
      - emp_id range (from_id .. to_id)
      - date range (from_dt .. to_dt, DD/MM/YYYY)
      - Only deletes users who have the selected biometric data
      - Requires admin password
    Also deletes associated files and biometric records.
    """
    data = request.get_json(force=True) or {}

    admin_pw  = (data.get("admin_password") or "").strip()
    from_id   = (data.get("from_id")  or "").strip()
    to_id     = (data.get("to_id")    or "").strip()
    from_dt   = (data.get("from_dt")  or "").strip()
    to_dt     = (data.get("to_dt")    or "").strip()
    include_card  = bool(data.get("include_card",  True))
    include_thumb = bool(data.get("include_thumb", True))
    include_face  = bool(data.get("include_face",  True))
    include_photo = bool(data.get("include_photo", True))

    # ── Admin password check ───────────────────────────────────────────────
    if not check_admin_password(admin_pw):
        return jsonify({"success": False, "message": "Incorrect admin password."}), 403

    def norm_date(s):
        s = (s or "").strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return s

    # ── Build WHERE clause ─────────────────────────────────────────────────
    conditions = ["u.emp_id IS NOT NULL", "u.emp_id != ''"]
    params = []

    _del_use_numeric = _is_numeric_id(from_id or "0") and _is_numeric_id(to_id or "0")
    if from_id:
        if _del_use_numeric:
            conditions.append("CAST(TRIM(u.emp_id) AS INTEGER) >= ?")
            params.append(int(str(from_id).strip()))
        else:
            conditions.append("TRIM(u.emp_id) >= ?")
            params.append(str(from_id).strip())
    if to_id:
        if _del_use_numeric:
            conditions.append("CAST(TRIM(u.emp_id) AS INTEGER) <= ?")
            params.append(int(str(to_id).strip()))
        else:
            conditions.append("TRIM(u.emp_id) <= ?")
            params.append(str(to_id).strip())
    if from_dt:
        conditions.append("SUBSTR(COALESCE(u.created_at,''), 1, 10) >= ?")
        params.append(norm_date(from_dt))
    if to_dt:
        conditions.append("SUBSTR(COALESCE(u.created_at,''), 1, 10) <= ?")
        params.append(norm_date(to_dt))

    # ── Presence filter: only delete users who have selected data ──────────
    presence_conditions = []
    if include_face:
        presence_conditions.append(
            "(u.encoding_path IS NOT NULL AND u.encoding_path != '')"
        )
    if include_photo:
        presence_conditions.append(
            "(u.image_path IS NOT NULL AND u.image_path != '')"
        )
    if include_thumb:
        presence_conditions.append(
            "EXISTS (SELECT 1 FROM fingerprint_map fm WHERE fm.emp_id = u.emp_id)"
        )
    if include_card:
        presence_conditions.append(
            "EXISTS (SELECT 1 FROM rfid_card_map rc WHERE rc.emp_id = u.emp_id)"
        )

    if presence_conditions:
        conditions.append("(" + " OR ".join(presence_conditions) + ")")

    where_sql = " AND ".join(conditions)

    conn = get_db_connection()
    errors = []
    deleted_count = 0

    try:
        # ── Find matching users ────────────────────────────────────────────
        rows = conn.execute(
            f"SELECT emp_id, name, image_path, encoding_path FROM users u WHERE {where_sql}",
            params
        ).fetchall()

        if not rows:
            return jsonify({
                "success": True,
                "message": "No users found matching the selected filters.",
                "deleted": 0
            })

        emp_ids = [r["emp_id"] for r in rows]
        print(f"[DELETE_FILTERED] Deleting {len(emp_ids)} users: {emp_ids}")

        for row in rows:
            emp_id = row["emp_id"]
            try:
                # ── Delete face encoding file ──────────────────────────────
                enc_path = row["encoding_path"]
                if enc_path:
                    try:
                        from pathlib import Path
                        p = Path(enc_path)
                        if not p.is_absolute():
                            p = Path.cwd() / enc_path
                        if p.exists():
                            p.unlink()
                            print(f"[DELETE_FILTERED] Deleted encoding file: {enc_path}")
                    except Exception as e:
                        errors.append(f"{emp_id}: encoding file delete error: {e}")

                # ── Delete face image file ─────────────────────────────────
                img_path = row["image_path"]
                if img_path:
                    try:
                        from pathlib import Path
                        p = Path(img_path)
                        if not p.is_absolute():
                            p = Path.cwd() / img_path
                        if p.exists():
                            p.unlink()
                            print(f"[DELETE_FILTERED] Deleted image file: {img_path}")
                    except Exception as e:
                        errors.append(f"{emp_id}: image file delete error: {e}")

                # ── Delete fingerprint template file ───────────────────────
                try:
                    delete_fingerprint_template(emp_id)
                except Exception as e:
                    errors.append(f"{emp_id}: fingerprint template delete error: {e}")

                # ── Delete from fingerprint_map ────────────────────────────
                try:
                    conn.execute(
                        "DELETE FROM fingerprint_map WHERE emp_id = ?", (emp_id,)
                    )
                except Exception as e:
                    errors.append(f"{emp_id}: fingerprint_map delete error: {e}")

                # ── Delete from rfid_card_map ──────────────────────────────
                try:
                    conn.execute(
                        "DELETE FROM rfid_card_map WHERE emp_id = ?", (emp_id,)
                    )
                except Exception as e:
                    errors.append(f"{emp_id}: rfid_card_map delete error: {e}")

                # ── Delete from users table ────────────────────────────────
                conn.execute("DELETE FROM users WHERE emp_id = ?", (emp_id,))
                deleted_count += 1
                print(f"[DELETE_FILTERED] Deleted user: {emp_id}")

            except Exception as e:
                errors.append(f"{emp_id}: unexpected error: {e}")
                print(f"[DELETE_FILTERED] Error deleting {emp_id}: {e}")

        conn.commit()

        # ── Reload face recognizer after deletions ─────────────────────────
        try:
            recognizer.load_all_encodings()
            print(f"[DELETE_FILTERED] Face recognizer reloaded after deleting {deleted_count} users")
        except Exception as e:
            errors.append(f"Face recognizer reload error: {e}")

        msg = f"Successfully deleted {deleted_count} user(s)."
        if errors:
            msg += f" ({len(errors)} warning(s))"

        return jsonify({
            "success": True,
            "message": msg,
            "deleted": deleted_count,
            "errors": errors[:10]  # return first 10 errors max
        })

    except Exception as e:
        print(f"[DELETE_FILTERED] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "message": f"Delete failed: {str(e)}"
        }), 500

    finally:
        conn.close()

@app.route("/api/export_filtered_handoff", methods=["POST"])
@require_permission("export")
def api_export_filtered_handoff():
    """
    Export Users endpoint — called by userlog_export.html / export.html.
    Downloads user biometric data ONLY (users.xlsx, users.csv, face encodings,
    face photos, fingerprint templates, rfid map) based on selected filters.
    Logs are NEVER included here.
    """
    data          = request.get_json(force=True) or {}
    from_id       = (data.get("from_id")  or "").strip()
    to_id         = (data.get("to_id")    or "").strip()
    from_dt       = (data.get("from_dt")  or "").strip()
    to_dt         = (data.get("to_dt")    or "").strip()
    include_card  = bool(data.get("include_card",  True))
    include_thumb = bool(data.get("include_thumb", True))
    include_face  = bool(data.get("include_face",  True))
    include_photo = bool(data.get("include_photo", True))

    try:
        zip_bytes = _build_filtered_export_zip(
            from_id=from_id, to_id=to_id,
            from_dt=from_dt, to_dt=to_dt,
            include_card=include_card,
            include_thumb=include_thumb,
            include_face=include_face,
            include_photo=include_photo,
            exclude_logs=True,
        )
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "message": f"Export build failed: {e}"}), 500

    # Count matched users for the response summary
    def _norm_date(s):
        s = (s or "").strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return s

    try:
        cond = ["emp_id IS NOT NULL", "emp_id != ''"]
        cnt_params: list = []
        _cnt_use_numeric = _is_numeric_id(from_id or "0") and _is_numeric_id(to_id or "0")
        if from_id:
            if _cnt_use_numeric:
                cond.append("CAST(TRIM(emp_id) AS INTEGER) >= ?")
                cnt_params.append(int(str(from_id).strip()))
            else:
                cond.append("TRIM(emp_id) >= ?")
                cnt_params.append(str(from_id).strip())
        if to_id:
            if _cnt_use_numeric:
                cond.append("CAST(TRIM(emp_id) AS INTEGER) <= ?")
                cnt_params.append(int(str(to_id).strip()))
            else:
                cond.append("TRIM(emp_id) <= ?")
                cnt_params.append(str(to_id).strip())
        if from_dt:
            cond.append("SUBSTR(COALESCE(created_at,''),1,10) >= ?")
            cnt_params.append(_norm_date(from_dt))
        if to_dt:
            cond.append("SUBSTR(COALESCE(created_at,''),1,10) <= ?")
            cnt_params.append(_norm_date(to_dt))

        conn = get_db_connection()
        user_count = conn.execute(
            f"SELECT COUNT(*) FROM users WHERE {' AND '.join(cond)}", cnt_params
        ).fetchone()[0]
        conn.close()
    except Exception:
        user_count = 0

    token    = "export-" + uuid.uuid4().hex[:8]
    out_path = os.path.join(HANDOFF_UPLOAD_DIR, f"{token}_filtered_export.zip")
    os.makedirs(HANDOFF_UPLOAD_DIR, exist_ok=True)

    with open(out_path, "wb") as fout:
        fout.write(zip_bytes)

    state = {
        "mode":          "export",
        "created_at":    datetime.now().isoformat(timespec="seconds"),
        "status":        "ready",
        "have_file":     True,
        "prepared_zip":  out_path,
        "original_name": "user_export.zip",
        "user_count":    user_count,
        "filter": {
            "from_id":  from_id, "to_id":  to_id,
            "from_dt":  from_dt, "to_dt":  to_dt,
            "card":     include_card,
            "thumb":    include_thumb,
            "face":     include_face,
            "photo":    include_photo,
        },
    }
    set_setting(f"handoff:{token}", json.dumps(state))

    id_part  = f"IDs {from_id}–{to_id}" if (from_id or to_id) else "All IDs"
    dt_part  = f"{from_dt}–{to_dt}"     if (from_dt or to_dt) else "All Dates"
    chk_part = " ".join(
        lbl for lbl, flag in [
            ("Card", include_card), ("Thumb", include_thumb),
            ("Face", include_face), ("Photo", include_photo),
        ] if flag
    ) or "None"
    filter_summary = f"{id_part} | {dt_part} | {chk_part}"

    base        = _qr_base_url()
    handoff_url = f"{base}/handoff/export/{token}"

    return jsonify({
        "success":        True,
        "token":          token,
        "qr_url":         f"/api/qr_for_handoff/{token}.png",
        "handoff_url":    handoff_url,
        "user_count":     user_count,
        "zip_size_kb":    round(len(zip_bytes) / 1024, 1),
        "filter_summary": filter_summary,
    })

 

# -----------------------------------------------------------------------------
# QR image helper for handoff tokens
# -----------------------------------------------------------------------------
import io as _io_for_qr  # avoid shadowing above
import json as _json_for_qr
import qrcode as _qrcode_for_qr
from flask import send_file as _send_file_for_qr, abort as _abort_for_qr

@app.route("/api/qr_for_handoff/<token>.png")
def api_qr_for_handoff_png(token):
    blob = get_setting(f"handoff:{token}")
    if not blob:
        _abort_for_qr(404, "Invalid or expired token")
    try:
        state = _json_for_qr.loads(blob)
    except Exception:
        _abort_for_qr(404, "Corrupt token")
    mode = state.get("mode")
    if mode not in ("import","export"):
        _abort_for_qr(400, "Bad mode")

    # Construct phone URL (match your handoff portal route)
    base = app.config.get("PUBLIC_BASE_URL") or request.host_url.rstrip("/")
    # If kiosk is on 127.0.0.1/localhost, swap to LAN IP (optional)
    host = (request.host or "").split(":")[0]
    if host in ("127.0.0.1","localhost"):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            base = f"http://{ip}:{app.config.get('APP_PORT', 5000)}"
        except Exception:
            pass

    phone_url = f"{base}/handoff/{mode}/{token}"

    buf = _io_for_qr.BytesIO()
    _qrcode_for_qr.make(phone_url).save(buf, format="PNG")
    buf.seek(0)
    return _send_file_for_qr(buf, mimetype="image/png")


# ---------------- XLSX helpers ----------------
def sqlite_list_tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def sqlite_table_info(conn, table):
    rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    cols = []
    for r in rows:
        if isinstance(r, sqlite3.Row):
            cols.append(dict(r))
        else:
            cols.append({"cid": r[0], "name": r[1], "type": r[2], "notnull": r[3], "dflt_value": r[4], "pk": r[5]})
    return cols


def _coerce_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notna(df), None)


def _xlsx_bytes_from_dataframe(df: pd.DataFrame, sheet_name="Sheet1") -> bytes:
    buf = io.BytesIO()
    # Convert emp_id to string to prevent Excel decimal conversion
    if 'emp_id' in df.columns:
        df = df.copy()
        df['emp_id'] = df['emp_id'].astype(str)
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf.read()


def export_all_tables_as_zip_of_xlsx() -> bytes:
    """
    Export all database tables including new columns:
    - users.encoding_path
    - users.image_path  
    - fingerprint_map.updated_at
    """
    conn = get_db_connection()
    try:
        tables = sqlite_list_tables(conn)
        zbuf = io.BytesIO()
        with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            # Export all database tables as XLSX
            for table in tables:
                try:
                    df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
                    xlsx_bytes = _xlsx_bytes_from_dataframe(df, sheet_name=table)
                    zf.writestr(f"{table}.xlsx", xlsx_bytes)
                    print(f"[EXPORT] Exported {table} with {len(df.columns)} columns")
                except Exception as e:
                    print(f"[EXPORT] skip {table}: {e}")

            # Export face images directory
            try:
                from pathlib import Path
                users_img_dir = Path("users_img")
                if users_img_dir.exists():
                    for img_file in users_img_dir.glob("*.jpg"):
                        with open(img_file, 'rb') as f:
                            zf.writestr(f"users_img/{img_file.name}", f.read())
                    print(f"[EXPORT] Added {len(list(users_img_dir.glob('*.jpg')))} face images")
            except Exception as e:
                print(f"[EXPORT] skip face images: {e}")

            # Export face encodings directory
            try:
                face_enc_dir = Path("face_encodings")
                if face_enc_dir.exists():
                    for enc_file in face_enc_dir.glob("*.dat"):
                        with open(enc_file, 'rb') as f:
                            zf.writestr(f"face_encodings/{enc_file.name}", f.read())
                    print(f"[EXPORT] Added {len(list(face_enc_dir.glob('*.dat')))} face encodings")
            except Exception as e:
                print(f"[EXPORT] skip face encodings: {e}")

            # Export fingerprint encodings directory (templates + metadata)
            try:
                fingerprint_enc_dir = Path("fingerprint_encodings")
                if fingerprint_enc_dir.exists():
                    dat_count = 0
                    json_count = 0
                    for enc_file in fingerprint_enc_dir.glob("*.dat"):
                        with open(enc_file, 'rb') as f:
                            zf.writestr(f"fingerprint_encodings/{enc_file.name}", f.read())
                        dat_count += 1
                    for meta_file in fingerprint_enc_dir.glob("*.json"):
                        with open(meta_file, 'rb') as f:
                            zf.writestr(f"fingerprint_encodings/{meta_file.name}", f.read())
                        json_count += 1
                    print(f"[EXPORT] Added {dat_count} fingerprint templates and {json_count} metadata files")
            except Exception as e:
                print(f"[EXPORT] skip fingerprint encodings: {e}")

        zbuf.seek(0)
        return zbuf.read()
    finally:
        conn.close()


def apply_xlsx_to_db(xlsx_file) -> dict:
    """
    Import XLSX with new columns:
    - users.encoding_path
    - users.image_path
    - fingerprint_map.updated_at
    
    Validates columns exist in target table before inserting.
    """
    res = {"ok": True, "updated": {}, "errors": []}
    try:
        xl = pd.ExcelFile(xlsx_file)
    except Exception as e:
        return {"ok": False, "updated": {}, "errors": [f"Failed to read Excel: {e}"]}

    conn = get_db_connection()
    try:
        tables = set(sqlite_list_tables(conn))
        for sheet in xl.sheet_names:
            table = sheet.strip()
            if table not in tables:
                res["errors"].append(f"Skipping sheet '{sheet}': no matching table.")
                continue
            try:
                df = xl.parse(sheet_name=sheet).dropna(how="all")
            except Exception as e:
                res["ok"] = False
                res["errors"].append(f"Sheet '{sheet}': parse error: {e}")
                continue
            if df.empty:
                res["updated"][table] = 0
                continue

            info = sqlite_table_info(conn, table)
            table_cols = [c["name"] for c in info]
            keep = [c for c in df.columns if c in table_cols]
            if not keep:
                res["errors"].append(f"Sheet '{sheet}': no valid columns for '{table}'.")
                continue

            df = df[keep]
            df = _coerce_nan_to_none(df)

            # Fix emp_id: convert to string without decimals (prevent 35322.0 format)
            if 'emp_id' in df.columns:
                df['emp_id'] = df['emp_id'].apply(lambda x: str(int(float(x))) if pd.notna(x) and x != '' else x)

            placeholders = ", ".join(["?"] * len(keep))
            col_list = ", ".join([f'"{c}"' for c in keep])
            sql = f'INSERT OR REPLACE INTO "{table}" ({col_list}) VALUES ({placeholders})'

            cur = conn.cursor()
            count = 0
            try:
                for row in df.itertuples(index=False, name=None):
                    cur.execute(sql, tuple(row))
                    count += 1
                conn.commit()

                # If importing users table, auto-assign template IDs
                if table == 'users' and 'emp_id' in keep:
                    try:
                        emp_id_idx = keep.index('emp_id')
                        for row in df.itertuples(index=False, name=None):
                            emp_id = row[emp_id_idx]
                            if emp_id:
                                try:
                                    get_or_reserve_template_id(str(emp_id))
                                except Exception as e:
                                    print(f"[XLSX] Failed to reserve template ID for {emp_id}: {e}")
                    except Exception as e:
                        print(f"[XLSX] Template ID reservation error: {e}")

                # If importing fingerprints table, also upload templates to sensor
                if table == 'fingerprints':
                    try:
                        # Get all rows that were just imported with templates
                        templates = cur.execute(
                            "SELECT id, template FROM fingerprints WHERE template IS NOT NULL"
                        ).fetchall()

                        if templates:
                            with sensor_lock:
                                s = get_fingerprint_sensor()
                                s.open()
                                try:
                                    uploaded = 0
                                    for row in templates:
                                        template_id = row[0] if isinstance(row, (list, tuple)) else row['id']
                                        template_data = row[1] if isinstance(row, (list, tuple)) else row['template']
                                        if template_data and s.set_template(template_id, template_data):
                                            uploaded += 1
                                    res["errors"].append(f"Fingerprints: Uploaded {uploaded}/{len(templates)} templates to sensor")
                                finally:
                                    s._flush()
                                    s.close()
                    except Exception as e:
                        res["errors"].append(f"Fingerprints: Template upload error: {e}")

            except Exception as e:
                conn.rollback()
                res["ok"] = False
                res["errors"].append(f"Sheet '{sheet}': DB error: {e}")
                continue
            res["updated"][table] = count
    finally:
        conn.close()
    return res

def get_saved_mesh_devices():
    """
    Read saved mesh devices from app_settings.key = 'saved_mesh'.
    The value is expected to be JSON array (e.g. '["192.168.1.12","192.168.1.13"]').
    If not present, returns [].
    """
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT value FROM app_settings WHERE key = ? LIMIT 1", ("saved_mesh",))
        row = c.fetchone()
        conn.close()
        if not row:
            return []
        val = row[0] if isinstance(row, (list, tuple)) else row
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                # older storage might be comma-separated
                return [ip.strip() for ip in val.split(",") if ip.strip()]
        if isinstance(val, list):
            return val
        return []
    except Exception as e:
        print("[MESH] get_saved_mesh_devices error:", e)
        try:
            conn.close()
        except Exception:
            pass
        return []

# ========= NEW: login logging + days-allowed policy =========
def insert_login_log(emp_id: str, name: str, mode: str, success: bool = True, item_name: str = ""):
    """Log user login/attendance to local SQLite database."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = get_db_connection()
    cur = conn.execute(
        """INSERT INTO logs (emp_id, name, device_id, mode, ts, success, created_at, item_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (str(emp_id or ""), name or "", get_device_id(), mode, ts, 1 if success else 0, ts, item_name or "")
    )
    conn.commit()
    index_id = cur.lastrowid
    print(f"[logs] Inserted log id={index_id} emp_id={emp_id} mode={mode} item={item_name}")
    return index_id


def _unique_day(dts: str) -> str:
    return (dts or "")[:10]


def _consecutive_days_count(emp_id: str) -> int:
    ensure_logs_table()
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT ts FROM logs WHERE emp_id=? ORDER BY ts DESC",
            (str(emp_id),)
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return 0
    days = []
    seen = set()
    for r in rows:
        d = _unique_day(r["ts"])
        if d and d not in seen:
            days.append(d)
            seen.add(d)
    if not days:
        return 0
    today = date.today()
    count = 0
    cur = today
    for d in days:
        try:
            y, m, dd = d.split("-")
            ddt = date(int(y), int(m), int(dd))
        except Exception:
            continue
        if ddt == cur:
            count += 1
            cur = cur.fromordinal(cur.toordinal() - 1)
        else:
            break
    return count


def is_login_allowed_by_days(emp_id: str):
    try:
        dal = int(get_setting("days_allowed", "0") or "0")
    except Exception:
        dal = 0
    if dal <= 0:
        return True, None
    consec = _consecutive_days_count(emp_id)
    if consec >= dal:
        return False, f"Consecutive days limit reached ({dal}). User must take leave today."
    return True, None

@app.route("/api/send_mesh_test", methods=["POST"])
def api_send_mesh_test():
    if not NETWORK_ENABLED:
        return jsonify({"success": False, "message": "Networking disabled"})
    data = request.get_json(force=True) or {}
    payload = data.get("payload") or {"type":"mesh_test","ts": time.time()}
    try:
        # use send_udp_json without target so send_reliable will use saved mesh devices
        # send_udp_json signature: (target_ip, port, payload)
        ok = send_udp_json(None, 0, payload)
        # also return which saved targets we attempted (for debugging)
        targets = get_selected_mesh_devices()
        return jsonify({"success": True, "targets": targets})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500





# -----------------------------------------------------------------------------
# Logs UI + APIs
# -----------------------------------------------------------------------------
@app.route("/logs")
@require_page_permission("logs")
def logs_page():
    return render_template("logs.html")
@app.route('/api/logs/item_summary')
def api_logs_item_summary():
    try:
        conn = get_db_connection()
        rows = conn.execute('''
            SELECT
                COALESCE(s.shift_name, 'Unknown') AS shift_name,
                o.item_name,
                SUM(o.qty) as cnt
            FROM orders o
            LEFT JOIN shifts s
                ON TIME(o.order_time) >= TIME(s.from_time)
               AND TIME(o.order_time) < TIME(s.to_time)
            WHERE o.item_name IS NOT NULL AND o.item_name != ''
              AND DATE(o.order_time) = DATE('now','localtime')
            GROUP BY s.shift_name, o.item_name
            ORDER BY s.from_time, cnt DESC
        ''').fetchall()
        conn.close()

        shifts = {}
        for r in rows:
            shift = r['shift_name'] or 'Unknown'
            if shift not in shifts:
                shifts[shift] = {}
            shifts[shift][r['item_name']] = r['cnt']

        return jsonify(success=True, shifts=shifts)
    except Exception as e:
        return jsonify(success=False, error=str(e))
        

@app.route("/api/logs")
def api_logs_list():
    ensure_logs_table()
    q = (request.args.get("q") or "").strip()
    try:
        limit = max(1, min(1000, int(request.args.get("limit", "200"))))
    except Exception:
        limit = 200
    try:
        offset = max(0, int(request.args.get("offset", "0")))
    except Exception:
        offset = 0

    conn = get_db_connection()
    try:
        if q:
            rows = conn.execute(
                """
                SELECT id AS log_id, emp_id, name, device_id, mode, ts,
                    COALESCE(item_name, '') AS item_name
                FROM logs
                WHERE emp_id LIKE ? OR name LIKE ?
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (f"%{q}%", f"%{q}%", limit, offset)
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id AS log_id, emp_id, name, device_id, mode, ts,
                    COALESCE(item_name, '') AS item_name
                FROM logs
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset)
            ).fetchall()
        data = [dict(r) for r in rows]

        # Attach order_id from orders table for canteen logs
        canteen_rows = [d for d in data if (d.get("mode") or "").lower() == "canteen" and d.get("ts")]
        if canteen_rows:
            for d in canteen_rows:
                try:
                    ts = d["ts"]
                    eid = d["emp_id"]
                    matched = conn.execute(
                        """SELECT order_id FROM orders
                           WHERE emp_id = ?
                           AND ABS(strftime('%s', REPLACE(order_time,'T',' ')) - strftime('%s', ?)) <= 5
                           ORDER BY ABS(strftime('%s', REPLACE(order_time,'T',' ')) - strftime('%s', ?)) ASC
                           LIMIT 1""",
                        (eid, ts, ts)
                    ).fetchone()
                    d["order_id"] = (matched["order_id"] or "") if matched else ""
                except Exception:
                    d["order_id"] = ""
            for d in data:
                if "order_id" not in d:
                    d["order_id"] = ""

        return jsonify({"success": True, "rows": data})
    finally:
        conn.close()


@app.route("/api/logs_export_csv")
def api_logs_export_csv():
    """Export attendance logs as CSV, sorted by timestamp DESC (recent first)."""
    ensure_logs_table()
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """SELECT id, emp_id, name, device_id, mode, ts, success, created_at
               FROM logs ORDER BY ts DESC, id DESC"""
        ).fetchall()
        import csv, io as _io2
        buf = _io2.StringIO()
        w = csv.writer(buf)
        w.writerow(["id", "emp_id", "name", "device_id", "mode", "ts", "success", "created_at"])
        for r in rows:
            w.writerow([
                r["id"], r["emp_id"], r["name"], r["device_id"], r["mode"],
                r["ts"], r["success"], r["created_at"]
            ])
        buf.seek(0)
        return Response(
            buf.getvalue(),
            headers={"Content-Disposition": "attachment; filename=logs.csv"},
            mimetype="text/csv"
        )
    finally:
        conn.close()


@app.route("/api/logs", methods=["GET"])
def api_get_logs():
    """
    Get attendance logs with pagination, sorted by timestamp DESC (recent first).

    Query params:
        limit: Max number of records (default 100, max 1000)
        offset: Number of records to skip (default 0)
        emp_id: Filter by employee ID (optional)
    """
    ensure_logs_table()
    limit = min(int(request.args.get('limit', 100)), 1000)
    offset = int(request.args.get('offset', 0))
    emp_id_filter = request.args.get('emp_id', '').strip()

    conn = get_db_connection()
    try:
        if emp_id_filter:
            rows = conn.execute(
                """SELECT id, emp_id, name, device_id, mode, ts, success, created_at
                   FROM logs WHERE emp_id = ?
                   ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?""",
                (emp_id_filter, limit, offset)
            ).fetchall()
            total_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM logs WHERE emp_id = ?", (emp_id_filter,)
            ).fetchone()
        else:
            rows = conn.execute(
                """SELECT id, emp_id, name, device_id, mode, ts, success, created_at
                   FROM logs ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?""",
                (limit, offset)
            ).fetchall()
            total_row = conn.execute("SELECT COUNT(*) as cnt FROM logs").fetchone()

        total = total_row['cnt'] if total_row else 0

        logs = []
        for r in rows:
            entry = {
                "id": r["id"],
                "emp_id": r["emp_id"],
                "name": r["name"],
                "device_id": r["device_id"],
                "mode": r["mode"],
                "ts": r["ts"],
                "success": r["success"],
                "created_at": r["created_at"],
                "order_id": ""
            }
            if (r["mode"] or "").lower() == "canteen" and r["ts"]:
                try:
                    m = conn.execute(
                        """SELECT order_id FROM orders
                           WHERE emp_id = ?
                           AND ABS(strftime('%s', REPLACE(order_time,'T',' ')) - strftime('%s', ?)) <= 5
                           ORDER BY ABS(strftime('%s', REPLACE(order_time,'T',' ')) - strftime('%s', ?)) ASC
                           LIMIT 1""",
                        (r["emp_id"], r["ts"], r["ts"])
                    ).fetchone()
                    entry["order_id"] = (m["order_id"] or "") if m else ""
                except Exception:
                    pass
            logs.append(entry)

        return jsonify({
            "success": True,
            "logs": logs,
            "total": total,
            "limit": limit,
            "offset": offset
        })
    finally:
        conn.close()


def fix_logs_table_nulls():
    """
    Fix existing logs with null values by:
    1. Setting success=1 for all null success values
    2. Setting created_at from ts for all null created_at values
    3. Removing any completely null rows

    Called on startup to fix historical data.
    """
    try:
        ensure_logs_table()
        conn = get_db_connection()
        try:
            # Fix null success values (assume successful)
            conn.execute("UPDATE logs SET success = 1 WHERE success IS NULL")

            # Fix null created_at values (use ts)
            conn.execute("UPDATE logs SET item_name = '' WHERE item_name IS NULL")

            # Delete rows with null emp_id (invalid data)
            deleted = conn.execute("DELETE FROM logs WHERE emp_id IS NULL OR emp_id = ''").rowcount

            conn.commit()
            if deleted > 0:
                print(f"[logs] Cleaned up {deleted} invalid log entries")
            print("[logs] Fixed null values in logs table")
        finally:
            conn.close()
    except Exception as e:
        print(f"[logs] fix_logs_table_nulls error: {e}")


from pathlib import Path
import subprocess, json



# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------
def start_embedded_agent():
    """
    Start the lightweight device agent (heartbeats + OTA trigger) in the background
    so devices get checks without requiring a separate systemd service.
    """
    try:
        t = threading.Thread(target=device_agent.main, name="device-agent", daemon=True)
        t.start()
        print("[BOOT] device_agent started in background")
    except Exception as e:
        print(f"[BOOT] Failed to start device_agent: {e}")


def load_and_reconnect_saved_devices():
    """
    Load saved devices from database and automatically reconnect.
    Called on startup to restore connections.
    """
    if not NETWORK_ENABLED or not device_sync_manager:
        return

    try:
        # Get saved devices from settings
        saved_ips = get_selected_mesh_devices()

        if saved_ips:
            print(f"[STARTUP] Loading {len(saved_ips)} saved devices for auto-reconnection...")
            # Load into sync manager - this will trigger automatic reconnection
            device_sync_manager.load_saved_devices(saved_ips)
            print(f"[STARTUP] Auto-reconnection initiated for saved devices")
        else:
            print("[STARTUP] No saved devices found - skipping auto-reconnection")

    except Exception as e:
        print(f"[STARTUP] Error loading saved devices: {e}")


###############################################################################
# SETTINGS PAGE SUPPORT (templates/settings.html)
# - Required by settings.html fetch calls (/api/*)
###############################################################################

import os
import re
import subprocess
from flask import jsonify, request, render_template


def _route_exists(rule: str, method: str = None) -> bool:
    """Check if a Flask route already exists (avoid duplicate endpoint errors)."""
    try:
        for r in app.url_map.iter_rules():
            if r.rule == rule and (method is None or method in r.methods):
                return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# /settings page route
# ---------------------------------------------------------------------------
if not _route_exists("/settings", "GET"):
    @app.route("/settings")
    def settings():
        return render_template("settings.html")


# ---------------------------------------------------------------------------
# Version + Power (settings.html -> /api/version, /api/reboot, /api/shutdown)
# ---------------------------------------------------------------------------
if not _route_exists("/api/version", "GET"):
    @app.route("/api/version")
    def api_version():
        v = globals().get("APP_VERSION") or globals().get("__version__")
        try:
            if not v and "get_setting" in globals():
                v = get_setting("sw_version", None)
        except Exception:
            pass
        return jsonify({"version": v or "—"})


if not _route_exists("/api/reboot", "POST"):
    @app.route("/api/reboot", methods=["POST"])
    def api_reboot():
        try:
            subprocess.Popen(["sudo", "reboot"])
            return jsonify({"ok": True, "message": "Restarting device..."})
        except Exception as e:
            return jsonify({"ok": False, "message": f"Failed to reboot: {e}"})


if not _route_exists("/api/shutdown", "POST"):
    @app.route("/api/shutdown", methods=["POST"])
    def api_shutdown():
        try:
            subprocess.Popen(["sudo", "shutdown", "-h", "now"])
            return jsonify({"ok": True, "message": "Shutting down device..."})
        except Exception as e:
            return jsonify({"ok": False, "message": f"Failed to shutdown: {e}"})


# ---------------------------------------------------------------------------
# Light control (settings.html -> /api/light/state, /api/light/<on|off|toggle>)
# ---------------------------------------------------------------------------
def _get_light_state() -> bool:
    try:
        if "get_setting" in globals():
            return str(get_setting("light_on", "0")).strip() in ("1", "true", "True", "yes", "on")
    except Exception:
        pass
    return False


def _set_light_state(on: bool):
    # If your newer app has a hardware function, call it here
    try:
        if "set_setting" in globals():
            set_setting("light_on", "1" if on else "0")
    except Exception:
        pass


if not _route_exists("/api/light/state", "GET"):
    @app.route("/api/light/state")
    def api_light_state():
        return jsonify({"on": _get_light_state()})


if not _route_exists("/api/light/<action>", "POST"):
    @app.route("/api/light/<action>", methods=["POST"])
    def api_light_action(action):
        action = (action or "").lower().strip()
        try:
            if action == "on":
                _set_light_state(True)
            elif action == "off":
                _set_light_state(False)
            elif action == "toggle":
                _set_light_state(not _get_light_state())
            else:
                return jsonify({"ok": False, "error": "Invalid action"}), 400
            return jsonify({"ok": True, "on": _get_light_state()})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# Relay control (settings.html -> /api/relay/state, /api/relay/<on|off|toggle>)
# Uses the `relay` OutputDevice defined near the top of app.py
# ---------------------------------------------------------------------------
if not _route_exists("/api/relay/state", "GET"):
    @app.route("/api/relay/state")
    def api_relay_state():
        try:
            return jsonify({"on": bool(relay.value)})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500


if not _route_exists("/api/relay/<action>", "POST"):
    @app.route("/api/relay/<action>", methods=["POST"])
    def api_relay_action(action):
        action = (action or "").lower().strip()
        try:
            if action == "on":
                relay.on()
            elif action == "off":
                relay.off()
            elif action == "toggle":
                relay.toggle()
            else:
                return jsonify({"ok": False, "error": "Invalid action"}), 400
            return jsonify({"ok": True, "on": bool(relay.value)})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# Volume (settings.html -> /api/get_volume, /api/set_volume)
# ---------------------------------------------------------------------------
def _detect_audio_system():
    """
    Detect which audio system is available on this device.
    Returns: 'wpctl' (WirePlumber/PipeWire), 'pactl' (PulseAudio), or 'amixer' (ALSA).
    """
    for cmd, label in [("wpctl", "wpctl"), ("pactl", "pactl"), ("amixer", "amixer")]:
        try:
            subprocess.run([cmd, "--version"], capture_output=True, timeout=3)
            return label
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
        except Exception:
            continue
    return "amixer"  # fallback


def _set_system_volume(vol_int):
    """Set system volume (0-100) using the best available audio backend."""
    audio = _detect_audio_system()

    if audio == "wpctl":
        # WirePlumber (PipeWire) — RPi5 default
        # wpctl uses 0.0-1.0 scale
        wpctl_vol = vol_int / 100.0
        result = subprocess.run(
            ["wpctl", "set-volume", "@DEFAULT_AUDIO_SINK@", str(round(wpctl_vol, 2))],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return True, f"Volume set to {vol_int}% (wpctl)"
        # wpctl failed, try pactl fallback
        audio = "pactl"

    if audio == "pactl":
        # PulseAudio
        result = subprocess.run(
            ["pactl", "set-sink-volume", "@DEFAULT_SINK@", f"{vol_int}%"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return True, f"Volume set to {vol_int}% (pactl)"
        # pactl failed, try amixer fallback
        audio = "amixer"

    if audio == "amixer":
        # ALSA — try multiple mixer controls
        for control in ["PCM", "Master", "Speaker", "Headphone"]:
            try:
                result = subprocess.run(
                    ["amixer", "sset", control, f"{vol_int}%"],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    return True, f"Volume set to {vol_int}% (amixer/{control})"
            except Exception:
                continue

    return False, "No working audio control found"


def _get_system_volume():
    """Get current system volume (0-100) using the best available audio backend."""
    audio = _detect_audio_system()

    if audio == "wpctl":
        try:
            result = subprocess.run(
                ["wpctl", "get-volume", "@DEFAULT_AUDIO_SINK@"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                # Output: "Volume: 0.75" or "Volume: 0.75 [MUTED]"
                m = re.search(r"Volume:\s*([\d.]+)", result.stdout)
                if m:
                    return True, int(round(float(m.group(1)) * 100))
        except Exception:
            pass

    if audio in ("wpctl", "pactl"):
        try:
            result = subprocess.run(
                ["pactl", "get-sink-volume", "@DEFAULT_SINK@"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                m = re.search(r"(\d{1,3})%", result.stdout)
                if m:
                    return True, int(m.group(1))
        except Exception:
            pass

    # ALSA fallback
    for control in ["PCM", "Master", "Speaker", "Headphone"]:
        try:
            output = subprocess.check_output(
                f"amixer get {control}", shell=True, timeout=5
            ).decode()
            m = re.search(r"\[(\d{1,3})%\]", output)
            if m:
                return True, int(m.group(1))
        except Exception:
            continue

    return False, 50  # default fallback


if not _route_exists("/api/set_volume", "POST"):
    @app.route("/api/set_volume", methods=["POST"])
    def set_volume():
        data = request.json or {}
        vol = data.get("volume")
        try:
            vol_int = int(vol)
            if not (0 <= vol_int <= 100):
                raise ValueError("Volume must be 0..100")
            success, message = _set_system_volume(vol_int)
            if success:
                print(f"[VOLUME] {message}")
            else:
                print(f"[VOLUME] Failed: {message}")
            return jsonify({"success": success, "message": message})
        except Exception as e:
            print(f"[VOLUME] set error: {e}")
            return jsonify({"success": False, "message": f"Failed to set volume: {e}"})


if not _route_exists("/api/get_volume", "GET"):
    @app.route("/api/get_volume")
    def get_volume():
        try:
            success, value = _get_system_volume()
            if success:
                return jsonify({"success": True, "volume": value})
            return jsonify({"success": True, "volume": value})  # return default even on failure
        except Exception as e:
            print(f"[VOLUME] get error: {e}")
            return jsonify({"success": False, "volume": 50, "message": str(e)})


# ---------------------------------------------------------------------------
# Admin password change (settings.html -> /api/change_password)
# Requires: check_admin_password(pw) and set_admin_password(new_pw)
# ---------------------------------------------------------------------------
if not _route_exists("/api/change_password", "POST"):
    @app.route("/api/change_password", methods=["POST"])
    def change_password():
        data = request.json or {}
        current = (data.get("current_password") or "").strip()
        newpw = (data.get("new_password") or "").strip()
        conf = (data.get("confirm_password") or "").strip()

        if "check_admin_password" not in globals() or "set_admin_password" not in globals():
            return jsonify({"success": False, "message": "Password helpers missing in app.py"}), 500

        if not check_admin_password(current):
            return jsonify({"success": False, "message": "Current password incorrect."})
        if not newpw or not conf:
            return jsonify({"success": False, "message": "New password cannot be empty."})
        if newpw != conf:
            return jsonify({"success": False, "message": "Passwords do not match."})
        if len(newpw) < 4:
            return jsonify({"success": False, "message": "Password too short."})

        set_admin_password(newpw)
        return jsonify({"success": True, "message": "Password changed successfully."})


# ---------------------------------------------------------------------------
# Wi-Fi (settings.html -> /api/wifi_scan, /api/wifi_save)
# ---------------------------------------------------------------------------
if not _route_exists("/api/wifi_scan", "GET"):
    @app.route("/api/wifi_scan")
    def wifi_scan():
        try:
            # Rescan first to get fresh results
            try:
                subprocess.run(
                    ["nmcli", "dev", "wifi", "rescan"],
                    timeout=10, capture_output=True
                )
                import time as _t; _t.sleep(2)  # give scan time to complete
            except Exception:
                pass
            out = subprocess.check_output(
                "nmcli -t -f SSID dev wifi list",
                shell=True, timeout=10
            ).decode().splitlines()
            ssids = sorted(set([s.strip() for s in out if s.strip()]))
            return jsonify(ssids)
        except Exception as e:
            print(f"[WIFI] scan error: {e}")
            return jsonify([])


if not _route_exists("/api/wifi_save", "POST"):
    @app.route("/api/wifi_save", methods=["POST"])
    def wifi_save():
        data = request.json or {}
        ssid = (data.get("ssid") or "").strip()
        password = (data.get("password") or "").strip()
        if not ssid:
            return jsonify({"success": False, "message": "SSID is required."})

        try:
            # Step 1: Identify the WiFi interface (usually wlan0)
            wifi_iface = "wlan0"
            try:
                iface_out = subprocess.check_output(
                    "nmcli -t -f DEVICE,TYPE dev | grep wifi",
                    shell=True, timeout=5
                ).decode().strip()
                if iface_out:
                    wifi_iface = iface_out.split(":")[0]
            except Exception:
                pass

            # Step 2: Delete any existing connection profile for this SSID
            # to avoid "connection already exists" conflicts
            try:
                existing = subprocess.check_output(
                    ["nmcli", "-t", "-f", "NAME,TYPE", "con", "show"],
                    timeout=5
                ).decode().splitlines()
                for line in existing:
                    parts = line.split(":")
                    if len(parts) >= 2 and parts[0].strip() == ssid:
                        print(f"[WIFI] Deleting existing profile for '{ssid}'")
                        subprocess.run(
                            ["nmcli", "con", "delete", ssid],
                            timeout=10, capture_output=True
                        )
                        break
            except Exception as e:
                print(f"[WIFI] Profile cleanup note: {e}")

            # Step 3: Ensure WiFi radio is enabled and interface is up
            try:
                subprocess.run(["nmcli", "radio", "wifi", "on"], timeout=5, capture_output=True)
            except Exception:
                pass

            # Step 4: Create a new connection and connect
            # Using 'nmcli con add' + 'nmcli con up' is more reliable than
            # 'nmcli dev wifi connect' which often fails on RPi
            if password:
                # WPA/WPA2 network with password
                add_result = subprocess.run(
                    [
                        "nmcli", "con", "add",
                        "type", "wifi",
                        "ifname", wifi_iface,
                        "con-name", ssid,
                        "ssid", ssid,
                        "wifi-sec.key-mgmt", "wpa-psk",
                        "wifi-sec.psk", password
                    ],
                    capture_output=True, text=True, timeout=15
                )
            else:
                # Open network (no password)
                add_result = subprocess.run(
                    [
                        "nmcli", "con", "add",
                        "type", "wifi",
                        "ifname", wifi_iface,
                        "con-name", ssid,
                        "ssid", ssid
                    ],
                    capture_output=True, text=True, timeout=15
                )

            if add_result.returncode != 0:
                err_msg = add_result.stderr.strip() or add_result.stdout.strip()
                print(f"[WIFI] nmcli con add failed: {err_msg}")
                return jsonify({"success": False, "message": f"Failed to create WiFi profile: {err_msg}"})

            print(f"[WIFI] Profile created for '{ssid}', activating...")

            # Step 5: Activate the connection
            up_result = subprocess.run(
                ["nmcli", "con", "up", ssid],
                capture_output=True, text=True, timeout=30
            )

            if up_result.returncode != 0:
                err_msg = up_result.stderr.strip() or up_result.stdout.strip()
                print(f"[WIFI] nmcli con up failed: {err_msg}")
                # Clean up the profile on failure
                try:
                    subprocess.run(["nmcli", "con", "delete", ssid], timeout=5, capture_output=True)
                except Exception:
                    pass
                # Provide user-friendly error message
                if "Secrets were required" in err_msg or "secret" in err_msg.lower():
                    return jsonify({"success": False, "message": "Wrong WiFi password. Please check and try again."})
                elif "No suitable device" in err_msg:
                    return jsonify({"success": False, "message": "WiFi adapter not found. Check hardware connection."})
                elif "not available" in err_msg.lower():
                    return jsonify({"success": False, "message": "WiFi network not in range or adapter busy. Try again."})
                return jsonify({"success": False, "message": f"Connection failed: {err_msg}"})

            print(f"[WIFI] Successfully connected to '{ssid}'")
            return jsonify({"success": True, "message": f"Connected to {ssid}"})

        except subprocess.TimeoutExpired:
            return jsonify({"success": False, "message": "WiFi connection timed out. Network may be out of range."})
        except Exception as e:
            print(f"[WIFI] Unexpected error: {e}")
            return jsonify({"success": False, "message": f"WiFi setup failed: {e}"})


# ---------------------------------------------------------------------------
# MSSQL config (settings.html -> /api/mssql_config GET+POST, /api/mssql_test)
# ---------------------------------------------------------------------------
def _mssql_lib_available() -> bool:
    try:
        import pymssql  # noqa
        return True
    except Exception:
        return False


def _get_mssql_cfg():
    if "get_setting" not in globals():
        return {"server": "", "port": "1433", "database": "", "user": "", "password": "", "available": _mssql_lib_available()}
    return {
        "server": get_setting("mssql_server", ""),
        "port": str(get_setting("mssql_port", "1433") or "1433"),
        "database": get_setting("mssql_database", ""),
        "user": get_setting("mssql_user", ""),
        "password": get_setting("mssql_password", ""),
        "available": _mssql_lib_available(),
    }


if not _route_exists("/api/mssql_config", "GET"):
    @app.route("/api/mssql_config", methods=["GET"])
    def api_mssql_config_get():
        return jsonify({"success": True, "config": _get_mssql_cfg()})


if not _route_exists("/api/mssql_config", "POST"):
    @app.route("/api/mssql_config", methods=["POST"])
    def api_mssql_config_set():
        if "set_setting" not in globals():
            return jsonify({"success": False, "message": "set_setting() missing"}), 500

        data = request.json or {}
        set_setting("mssql_server", (data.get("server") or "").strip())
        set_setting("mssql_port", str(data.get("port") or "1433").strip())
        set_setting("mssql_database", (data.get("database") or "").strip())
        set_setting("mssql_user", (data.get("user") or "").strip())
        set_setting("mssql_password", (data.get("password") or "").strip())

        return jsonify({"success": True, "message": "MSSQL config saved", "config": _get_mssql_cfg()})


if not _route_exists("/api/mssql_test", "POST"):
    @app.route("/api/mssql_test", methods=["POST"])
    def api_mssql_test():
        data = request.json or {}
        try:
            import pymssql

            conn = pymssql.connect(
                server=data.get("server"),
                user=data.get("user"),
                password=data.get("password"),
                database=data.get("database"),
                port=int(data.get("port") or 1433),
                login_timeout=3,
                timeout=3,
            )
            conn.close()
            log_mssql_event("CONNECT", "Connection test successful", {"server": data.get("server")})
            return jsonify({"success": True, "message": "MSSQL connection OK"})
        except Exception as e:
            log_mssql_event("ERROR", f"Connection test failed: {e}", {"server": data.get("server")})
            return jsonify({"success": False, "message": f"MSSQL connection failed: {e}"})


if not _route_exists("/api/mssql_queue_status", "GET"):
    @app.route("/api/mssql_queue_status", methods=["GET"])
    def api_mssql_queue_status():
        """Get MSSQL queue status for monitoring dashboard."""
        status = get_mssql_queue_status()
        return jsonify({"success": True, "status": status})


# ---------------------------------------------------------------------------
# Device config (settings.html -> /api/device_config, /api/next_device_number, /api/check_device_id)
# ---------------------------------------------------------------------------
if not _route_exists("/api/device_config", "GET"):
    @app.route("/api/device_config", methods=["GET"])
    def api_device_config_get():
        if "get_device_id" in globals() and "get_device_type" in globals():
            return jsonify({"success": True, "device_id": get_device_id(), "device_type": get_device_type()})
        if "get_setting" in globals():
            return jsonify(
                {
                    "success": True,
                    "device_id": get_setting("device_id", "IN_001"),
                    "device_type": get_setting("device_type", "IN"),
                }
            )
        return jsonify({"success": False, "message": "Device helpers missing"}), 500


if not _route_exists("/api/device_config", "POST"):
    @app.route("/api/device_config", methods=["POST"])
    def api_device_config_set():
        data = request.json or {}
        device_id = (data.get("device_id") or "").strip()
        device_type = (data.get("device_type") or "").strip()
        try:
            if "set_device_config" in globals():
                set_device_config(device_id, device_type)
            elif "set_setting" in globals():
                set_setting("device_id", device_id.upper())
                set_setting("device_type", device_type)
            else:
                return jsonify({"success": False, "message": "set_setting()/set_device_config missing"}), 500
            return jsonify({"success": True, "message": "Device config saved"})
        except Exception as e:
            return jsonify({"success": False, "message": str(e)})

# ── Canteen Mapping ────────────────────────────────────────────────
@app.route('/api/canteen_mapping', methods=['GET'])
def api_canteen_mapping_get():
    try:
        conn = get_db_connection()
        conn.execute('''CREATE TABLE IF NOT EXISTS canteen_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canteen_id TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        row = conn.execute('SELECT canteen_id FROM canteen_mappings ORDER BY updated_at DESC LIMIT 1').fetchone()
        conn.close()
        if row:
            return jsonify({'success': True, 'canteen_id': row['canteen_id']})
        return jsonify({'success': True, 'canteen_id': None})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/canteen_mapping', methods=['POST'])
def api_canteen_mapping_set():
    try:
        data = request.get_json()
        canteen_id = (data.get('canteen_id') or '').strip()
        if not canteen_id:
            return jsonify({'success': False, 'message': 'Canteen ID is required'}), 400
        conn = get_db_connection()
        conn.execute('''CREATE TABLE IF NOT EXISTS canteen_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canteen_id TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.commit()
        existing = conn.execute('SELECT id FROM canteen_mappings LIMIT 1').fetchone()
        if existing:
            conn.execute('UPDATE canteen_mappings SET canteen_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                         (canteen_id, existing['id']))
        else:
            conn.execute('INSERT INTO canteen_mappings (canteen_id) VALUES (?)', (canteen_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': f'Canteen ID saved: {canteen_id}'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


if not _route_exists("/api/next_device_number", "GET"):
    @app.route("/api/next_device_number", methods=["GET"])
    def api_next_device_number():
        prefix = (request.args.get("prefix") or "").strip()
        if not prefix:
            return jsonify({"success": False, "message": "Prefix required"})
        return jsonify({"success": True, "prefix": prefix, "next_number": 1})


if not _route_exists("/api/check_device_id", "GET"):
    @app.route("/api/check_device_id", methods=["GET"])
    def api_check_device_id():
        device_id = (request.args.get("device_id") or "").strip().upper()
        if not device_id:
            return jsonify({"success": False, "message": "Device ID required"})
        cur = get_device_id() if "get_device_id" in globals() else None
        if cur and device_id == cur:
            return jsonify({"success": True, "duplicate": False, "device_id": device_id})
        return jsonify({"success": True, "duplicate": False, "device_id": device_id})


# ---------------------------------------------------------------------------
# Days Allowed (settings.html -> /api/days_allowed)
# ---------------------------------------------------------------------------
if not _route_exists("/api/days_allowed", "GET"):
    @app.route("/api/days_allowed", methods=["GET"])
    def api_days_allowed_get():
        val = 0
        try:
            if "get_setting" in globals():
                val = int(str(get_setting("days_allowed", "0") or "0"))
        except Exception:
            val = 0
        return jsonify({"success": True, "days_allowed": max(0, val)})


if not _route_exists("/api/days_allowed", "POST"):
    @app.route("/api/days_allowed", methods=["POST"])
    def api_days_allowed_set():
        if "set_setting" not in globals():
            return jsonify({"success": False, "message": "set_setting() missing"}), 500
        data = request.json or {}
        try:
            n = int(str(data.get("days_allowed") or "0"))
            n = max(0, n)
            set_setting("days_allowed", str(n))
            return jsonify({"success": True, "message": "Days allowed saved", "days_allowed": n})
        except Exception as e:
            return jsonify({"success": False, "message": str(e)})


# ---------------------------------------------------------------------------
# Inactive Cleanup Days (settings.html -> /api/inactive_cleanup_days)
# ---------------------------------------------------------------------------
if not _route_exists("/api/inactive_cleanup_days", "GET"):
    @app.route("/api/inactive_cleanup_days", methods=["GET"])
    def api_inactive_cleanup_days_get():
        val = 0
        try:
            if "get_setting" in globals():
                val = int(str(get_setting("inactive_cleanup_days", "0") or "0"))
        except Exception:
            val = 0
        return jsonify({"success": True, "inactive_cleanup_days": max(0, val)})


if not _route_exists("/api/inactive_cleanup_days", "POST"):
    @app.route("/api/inactive_cleanup_days", methods=["POST"])
    def api_inactive_cleanup_days_set():
        if "set_setting" not in globals():
            return jsonify({"success": False, "message": "set_setting() missing"}), 500
        data = request.json or {}
        try:
            n = int(str(data.get("inactive_cleanup_days") or "0"))
            n = max(0, n)
            set_setting("inactive_cleanup_days", str(n))
            return jsonify({"success": True, "message": "Inactive cleanup saved", "inactive_cleanup_days": n})
        except Exception as e:
            return jsonify({"success": False, "message": str(e)})


# ---------------------------------------------------------------------------
# Public Base URL (settings.html -> /api/public_base_url)
# ---------------------------------------------------------------------------
if not _route_exists("/api/public_base_url", "GET"):
    @app.route("/api/public_base_url", methods=["GET"])
    def api_public_base_url_get():
        url = ""
        try:
            if "get_setting" in globals():
                url = (get_setting("public_base_url", "") or "").strip()
        except Exception:
            url = ""
        return jsonify({"success": True, "public_base_url": url})


if not _route_exists("/api/public_base_url", "POST"):
    @app.route("/api/public_base_url", methods=["POST"])
    def api_public_base_url_set():
        if "set_setting" not in globals():
            return jsonify({"success": False, "message": "set_setting() missing"}), 500
        data = request.json or {}
        url = (data.get("public_base_url") or "").strip()
        try:
            set_setting("public_base_url", url)
            return jsonify({"success": True, "message": "Public base URL saved", "public_base_url": url})
        except Exception as e:
            return jsonify({"success": False, "message": str(e)})

APP_VERSION = "4.0.2"


@app.route("/api/import_sync_logs", methods=["POST"])
def api_import_sync_logs():
    data    = request.get_json(force=True) or {}
    token   = (data.get("handoff_token") or "").strip()
    logs_in = data.get("logs") or []

    blob = get_setting(f"handoff:{token}")
    if not blob:
        return jsonify(success=False, message="Invalid or expired QR token."), 403
    try:
        state = json.loads(blob)
    except Exception:
        return jsonify(success=False, message="Corrupt token state."), 403
    if state.get("mode") != "import":
        return jsonify(success=False, message="Token is not for import."), 403
    if not logs_in:
        return jsonify(success=False, message="No log records found in the uploaded file."), 400

    conn = get_db_connection()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logs_created = logs_skipped = logs_errors = 0
    try:
        for log in logs_in:
            emp_id = (log.get("emp_id") or "").strip()
            ts_src = (log.get("ts") or "")[:19]
            if not emp_id or not ts_src:
                continue
            try:
                if conn.execute("SELECT ts FROM logs WHERE emp_id=? AND ts=?", (emp_id, ts_src)).fetchone():
                    logs_skipped += 1
                    continue
                conn.execute(
                    "INSERT INTO logs (emp_id, name, device_id, mode, ts, success, created_at, item_name) VALUES (?,?,?,?,?,?,?,?)",
                    (emp_id, log.get("name",""), log.get("device_id",""), log.get("mode",""),
                     ts_src, int(str(log.get("success","1")).strip() or "1"), now_str,
                     log.get("item_name", ""))
                )
                logs_created += 1
            except Exception as e:
                print(f"[IMPORT_LOGS] error {emp_id}: {e}")
                logs_errors += 1
        conn.commit()
    except Exception as e:
        return jsonify(success=False, message=f"Import failed: {str(e)}"), 500

    return jsonify(
        success=True,
        message=f"Import complete. {logs_created} log(s) imported, {logs_skipped} skipped.",
        logs_created=logs_created, logs_skipped=logs_skipped, logs_errors=logs_errors,
        summary={"created": logs_created, "updated": 0, "skipped": logs_skipped, "errors": logs_errors},
        results=[]
    )

# --- Canteen status ---
@app.route("/api/is_canteen_open")
def api_is_canteen_open():
    return jsonify({"open": is_canteen_open_ui(), "next_time": get_next_opening_ui()})
 
@app.route("/api/current_slot_info")
def api_current_slot_info():
    slot = get_current_slot_row()
    return jsonify({"open": slot is not None, "next_time": get_next_opening_ui(),
                    "slot": dict(slot) if slot else None})

@app.route("/api/allowed_slot_for_user", methods=["POST"])
def api_allowed_slot_for_user():
    data = request.get_json(force=True) or {}
    emp_id = (data.get("emp_id") or "").strip()
    if not emp_id:
        return jsonify(success=False, message="emp_id required"), 400

    slot_code, slot_row = get_active_slot_code()
    if not slot_code:
        return jsonify(success=True, allowed=False, reason="closed", message="Canteen is closed")

    conn = get_db_connection()
    try:
        limits = get_slot_limits(conn, emp_id, slot_code)
        usage = get_usage_today(conn, emp_id, slot_code)
    finally:
        conn.close()

    # Check if user has exceeded daily total
    dtm = limits.get("daily_total_max")
    if dtm is not None and usage["day_total_today"] >= int(dtm):
        return jsonify(success=True, allowed=False, reason="daily_limit",
                       message="Daily order limit reached", limits=limits, usage=usage)

    # Check if user has exceeded slot total
    stm = limits.get("slot_total_max")
    if stm is not None and usage["slot_total_today"] >= int(stm):
        return jsonify(success=True, allowed=False, reason="slot_limit",
                       message="Slot order limit reached", limits=limits, usage=usage)

    return jsonify(success=True, allowed=True, slot_code=slot_code,
                   slot=dict(slot_row) if slot_row else None,
                   limits=limits, usage=usage)

@app.route("/api/items_for_current_slot")
def api_items_for_current_slot():
    slot = get_current_slot_row()
    if not slot: return jsonify(items=[])
    db = get_db_connection()
    rows = db.execute("""
        SELECT i.item_code, i.item_name, m.menu_name AS category
        FROM items i JOIN menu_codes m ON m.menu_code = i.menu_code
        JOIN time_slots ts ON ts.slot_code = m.slot_code
        WHERE ts.slot_code = ? ORDER BY m.menu_name, i.item_name
    """, (slot["slot_code"],)).fetchall()
    db.close()
    return jsonify(items=[dict(r) for r in rows])
 
@app.route("/api/menu_limits")
def api_menu_limits():
    db = get_db_connection()
    cur = db.cursor()
    cur.execute("SELECT category, item_name, item_limit FROM item_limits")
    limits = {}
    for cat, item, lim in cur.fetchall():
        limits.setdefault(cat, {})[item] = lim
    db.close()
    return jsonify(limits)
 
@app.route('/api/categories')
def api_categories():
    db = get_db_connection()
    cats = [r['menu_name'] for r in db.execute("SELECT DISTINCT menu_name FROM menu_codes")]
    db.close()
    return jsonify(categories=cats)
 
@app.route('/api/items_for_category')
def api_items_for_category():
    cat = request.args.get('category')
    db = get_db_connection()
    rows = db.execute("SELECT menu_code FROM menu_codes WHERE menu_name=?", (cat,)).fetchall()
    if not rows: db.close(); return jsonify(items=[])
    items = []
    for r in rows:
        items.extend([row['item_name'] for row in db.execute(
            "SELECT item_name FROM items WHERE menu_code=?", (r['menu_code'],))])
    db.close()
    return jsonify(items=items)
 
# --- Place order ---
@app.route("/api/place_order", methods=["POST"])
def api_place_order():
    data = request.get_json(force=True) or {}
    emp_id = (data.get("emp_id") or "").strip()
    item_code = (data.get("item_code") or "").strip()
    qty = int(data.get("qty") or 1)
    if not emp_id or not item_code:
        return jsonify(success=False, reason="bad_request", message="emp_id and item_code required"), 400
    ok, payload = place_order_core(emp_id, item_code, qty)
    return jsonify(success=ok, **payload) if ok else jsonify(success=False, **payload)
 
# --- Bulk order submit ---
@app.route("/api/order_submit", methods=["POST"])
def order_submit():
    data = request.get_json(silent=True) or {}
    emp_id = str(data.get("emp_id") or data.get("employee_id") or data.get("user_id") or "").strip()
    if not emp_id:
        return jsonify(success=False, message="emp_id missing", echo=data), 400
    items_in = data.get("items")
    if not items_in:
        if any(k in data for k in ("item_code","code","item","item_name","name","label","title","id","item_id")):
            items_in = [data]
        else:
            return jsonify(success=False, message="items required", echo=data), 400
    conn = get_db_connection()
    try:
        slot_code, slot_row = get_active_slot_code()
        if not slot_code:
            return jsonify(success=False, rejected=[{"reason":"closed","message":"Canteen is closed"}], accepted=[]), 400
        grouped = group_items_for_limits(conn, items_in)
        try:
            validate_items_against_limits(conn, emp_id=emp_id, slot_code=slot_code, grouped_items=grouped)
        except SlotLimitError as e:
            return jsonify(success=False, accepted=[], rejected=[{"reason":e.code,"message":str(e),"meta":e.meta}]), 400
        shared_order_id = generate_order_id()
        successes, failures = [], []
        item_idx = 0
        for raw in items_in:
            code, qty = _resolve_item_full(conn, raw)
            if not code:
                failures.append({"input": raw, "reason":"unresolved_item", "message":"Could not resolve item"})
                continue
            item_idx += 1
            row_oid = shared_order_id if item_idx == 1 else f"{shared_order_id}-{item_idx}"
            ok, payload = place_order_core(emp_id, code, qty, skip_log=True, skip_print=True, order_id=row_oid)
            if ok:
                successes.append({"item_code":code,"qty":qty,"order_id":payload.get("order_id"),"item":payload.get("item")})
            else:
                failures.append({"item_code":code,"qty":qty,**payload})
        # Combined log: one row for the entire order
        if successes:
            try:
                parts = []
                for s in successes:
                    q = s.get("qty", 1)
                    nm = (s.get("item") or {}).get("item_name", s.get("item_code", ""))
                    parts.append(f"{nm} x{q}" if q > 1 else nm)
                combined = ", ".join(parts)
                _user_row = conn.execute("SELECT name FROM users WHERE emp_id=?", (emp_id,)).fetchone()
                _user_name = _user_row["name"] if _user_row and _user_row["name"] else ""
                insert_login_log(emp_id, _user_name, "canteen", success=True, item_name=combined)
            except Exception as e:
                print(f"[ORDER] Combined log error: {e}")

        overall = len(successes) > 0
        if overall and successes:
            try:
                from fingerprint import print_user_id_and_cut
                uname = ""
                try:
                    _row = conn.execute("SELECT name FROM users WHERE emp_id=?", (emp_id,)).fetchone()
                    if _row and _row["name"]:
                        uname = _row["name"]
                except Exception:
                    pass
                item_parts = []
                for s in successes:
                    itm = s.get("item") or {}
                    if isinstance(itm, dict):
                        icode = itm.get("item_code", "")
                        iname = _shorten_item(itm.get("item_name", ""))
                        item_parts.append(f"{icode}-{iname}" if icode else iname)
                canteen_id = get_canteen_id()
                last_line = f"{canteen_id}, {get_device_id()}" if canteen_id else get_device_id()
                text = (
                    f"{emp_id}, {_shorten_name(uname)}\n"
                    f"{', '.join(item_parts)}\n"
                    f"{datetime.now().strftime('%d-%m-%Y, %H:%M:%S')}\n"
                    f"{last_line}\n"
                )
                print_user_id_and_cut(text)
            except Exception as e:
                print(f"[Printer] combined receipt error: {e}")
        return jsonify(success=overall, accepted=successes, rejected=failures), (200 if overall else 400)
    finally:
        conn.close()
 
# --- Shift master CRUD ---
@app.route("/api/shift_master/list")
def shift_master_list():
    db = get_db_connection()
    shifts = [dict(r) for r in db.execute("SELECT * FROM shifts")]
    db.close()
    return jsonify(shifts=shifts)
 
@app.route("/api/shift_master/add", methods=["POST"])
def shift_master_add():
    data = request.json
    db = get_db_connection()
    try:
        db.execute("INSERT INTO shifts (shift_code, shift_name, from_time, to_time) VALUES (?,?,?,?)",
                   (data["shift_code"], data["shift_name"], data["from_time"], data["to_time"]))
        db.commit(); return jsonify(success=True)
    except sqlite3.IntegrityError:
        return jsonify(success=False, message="Shift code already exists.")
    finally: db.close()
 
@app.route("/api/shift_master/update", methods=["POST"])
def shift_master_update():
    data = request.json; db = get_db_connection()
    db.execute("UPDATE shifts SET shift_name=?, from_time=?, to_time=? WHERE shift_code=?",
               (data["shift_name"], data["from_time"], data["to_time"], data["shift_code"]))
    db.commit(); db.close(); return jsonify(success=True)
 
@app.route("/api/shift_master/delete", methods=["POST"])
def shift_master_delete():
    data = request.json; db = get_db_connection()
    db.execute("DELETE FROM shifts WHERE shift_code=?", (data["shift_code"],))
    db.commit(); db.close(); return jsonify(success=True)
 
# --- Time-slot master CRUD ---
@app.route("/api/time_slot_master/list")
def time_slot_master_list():
    db = get_db_connection()
    slots = [dict(r) for r in db.execute("SELECT * FROM time_slots")]
    shifts = [dict(r) for r in db.execute("SELECT shift_code, shift_name FROM shifts")]
    db.close()
    return jsonify(slots=slots, shifts=shifts)
 
@app.route("/api/time_slot_master/add", methods=["POST"])
def time_slot_master_add():
    data = request.json; db = get_db_connection()
    try:
        db.execute("INSERT INTO time_slots (slot_code, shift_code, slot_name, from_time, to_time) VALUES (?,?,?,?,?)",
                   (data["slot_code"], data["shift_code"], data["slot_name"], data["from_time"], data["to_time"]))
        db.commit(); return jsonify(success=True)
    except sqlite3.IntegrityError:
        return jsonify(success=False, message="Slot code already exists.")
    finally: db.close()
 
@app.route("/api/time_slot_master/update", methods=["POST"])
def time_slot_master_update():
    data = request.json; db = get_db_connection()
    db.execute("UPDATE time_slots SET shift_code=?, slot_name=?, from_time=?, to_time=? WHERE slot_code=?",
               (data["shift_code"], data["slot_name"], data["from_time"], data["to_time"], data["slot_code"]))
    db.commit(); db.close(); return jsonify(success=True)
 
@app.route("/api/time_slot_master/delete", methods=["POST"])
def time_slot_master_delete():
    data = request.json; db = get_db_connection()
    db.execute("DELETE FROM time_slots WHERE slot_code=?", (data["slot_code"],))
    db.commit(); db.close(); return jsonify(success=True)
 
# --- Menu master CRUD ---
@app.route("/api/menu_master/list")
def menu_master_list():
    db = get_db_connection()
    menus = [dict(r) for r in db.execute("SELECT * FROM menu_codes")]
    slots = [dict(r) for r in db.execute("SELECT slot_code, slot_name FROM time_slots")]
    db.close()
    return jsonify(menus=menus, slots=slots)
 
@app.route("/api/menu_master/add", methods=["POST"])
def menu_master_add():
    data = request.json; db = get_db_connection()
    try:
        db.execute("INSERT INTO menu_codes (menu_code, slot_code, menu_name) VALUES (?,?,?)",
                   (data["menu_code"], data["slot_code"], data["menu_name"]))
        db.commit(); return jsonify(success=True)
    except sqlite3.IntegrityError:
        return jsonify(success=False, message="Menu code already exists.")
    finally: db.close()
 
@app.route("/api/menu_master/update", methods=["POST"])
def menu_master_update():
    data = request.json; db = get_db_connection()
    db.execute("UPDATE menu_codes SET slot_code=?, menu_name=? WHERE menu_code=?",
               (data["slot_code"], data["menu_name"], data["menu_code"]))
    db.commit(); db.close(); return jsonify(success=True)
 
@app.route("/api/menu_master/delete", methods=["POST"])
def api_menu_master_delete():
    data = request.json; db = get_db_connection()
    db.execute("DELETE FROM menu_codes WHERE menu_code=?", (data["menu_code"],))
    db.commit(); db.close(); return jsonify(success=True)
 
# --- Item master CRUD ---
@app.route("/api/item_master/list")
def item_master_list():
    db = get_db_connection()
    items = [dict(r) for r in db.execute("SELECT * FROM items")]
    menus = [dict(r) for r in db.execute("SELECT menu_code, menu_name FROM menu_codes")]
    db.close()
    return jsonify(items=items, menus=menus)
 
@app.route("/api/item_master/add", methods=["POST"])
def item_master_add():
    data = request.json; db = get_db_connection()
    try:
        db.execute("INSERT INTO items (item_code, menu_code, item_name) VALUES (?,?,?)",
                   (data["item_code"], data["menu_code"], data["item_name"]))
        db.commit(); return jsonify(success=True)
    except sqlite3.IntegrityError:
        return jsonify(success=False, message="Item code already exists.")
    finally: db.close()
 
@app.route("/api/item_master/update", methods=["POST"])
def item_master_update():
    data = request.json; db = get_db_connection()
    db.execute("UPDATE items SET menu_code=?, item_name=? WHERE item_code=?",
               (data["menu_code"], data["item_name"], data["item_code"]))
    db.commit(); db.close(); return jsonify(success=True)
 
@app.route("/api/item_master/delete", methods=["POST"])
def item_master_delete():
    data = request.json; db = get_db_connection()
    db.execute("DELETE FROM items WHERE item_code=?", (data["item_code"],))
    db.commit(); db.close(); return jsonify(success=True)
 
# --- Item limit master ---
@app.route("/api/item_limit_master/list")
def item_limit_master_list():
    db = get_db_connection()
    data = [dict(row) for row in db.execute("SELECT * FROM item_limits")]
    db.close()
    return jsonify(limits=data)
 
@app.route("/api/item_limit_master/save", methods=["POST"])
def item_limit_master_save():
    data = request.json; db = get_db_connection()
    if not data:
        return jsonify(success=False, message="No data")
    # Delete old limits for this category, then insert fresh
    cat = data[0].get("category") if data else None
    if cat:
        db.execute("DELETE FROM item_limits WHERE category=?", (cat,))
    for row in data:
        db.execute("INSERT INTO item_limits (category, item_name, item_limit) VALUES (?,?,?)",
                   (row["category"], row["item_name"], row["item_limit"]))
    db.commit(); db.close(); return jsonify(success=True)
 
# --- Menu select page ---
@app.route("/menu_select")
def menu_select():
    return render_template("menu_select.html")
 
# --- Postgres DB config APIs ---
@app.route("/api/db_config", methods=["GET"])
def api_db_config_get():
    return jsonify({
        "host":     get_setting("pg_host", "192.168.1.3"),
        "port":     int(get_setting("pg_port", "5432") or "5432"),
        "dbname":   get_setting("pg_dbname", "postgres"),
        "user":     get_setting("pg_user", "postgres"),
        "password": get_setting("pg_password", "postgres"),
    })
 
@app.route("/api/db_config", methods=["POST"])
def api_db_config_set():
    data = request.get_json(force=True)
    for k in ("host","port","dbname","user","password"):
        v = (data.get(k) or "").strip()
        if k == "port":
            try: int(v)
            except: return jsonify(success=False, message="Port must be an integer"), 400
        set_setting(f"pg_{k}", v)
    load_pgcfg_from_settings()
    try:
        if pg_is_available():
            ensure_pg_table()
            return jsonify(success=True, message="Saved. Connection OK.", config=PGCFG)
        return jsonify(success=False, message="Saved, but DB unreachable.", config=PGCFG)
    except Exception as ex:
        return jsonify(success=False, message=f"Saved, connection failed: {ex}", config=PGCFG)
 
@app.route("/api/db_test", methods=["POST"])
def api_db_test():
    data = request.get_json(silent=True) or {}
    cfg = {k: (data.get(k) or PGCFG.get(k)) for k in ("host","port","dbname","user","password")}
    cfg["port"] = int(cfg.get("port") or 5432)
    try:
        c = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
                              user=cfg["user"], password=cfg["password"] or "",
                              connect_timeout=PG_CONNECT_TIMEOUT)
        c.close()
        return jsonify(success=True, message="Connection OK", tested=cfg)
    except Exception as e:
        return jsonify(success=False, message=str(e), tested=cfg)
 
# --- PG sync status ---
@app.route("/api/pg_sync_status")
def api_pg_sync_status():
    conn = get_db_connection()
    row = conn.execute("SELECT COUNT(*) AS c FROM pg_event_queue").fetchone()
    conn.close()
    return jsonify(pg_available=pg_is_available(), pending=row["c"],
                   last_ok=_last_pg_sync_info.get("last_ok"),
                   last_err=_last_pg_sync_info.get("last_err"))
 
@app.route("/api/pg_sync_drain_now", methods=["POST"])
def api_pg_sync_drain_now():
    try:
        n = drain_pg_queue_once(max_batch=500)
        return jsonify(success=True, drained=n)
    except Exception as e:
        return jsonify(success=False, message=str(e))

def ensure_default_menu_tables():
    """Create default_menu_config and default_menu_enabled tables if missing."""
    conn = get_db_connection()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS default_menu_config (
            shift_code  TEXT NOT NULL,
            item_code   TEXT NOT NULL,
            qty         INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (shift_code, item_code)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS default_menu_enabled (
            shift_code TEXT PRIMARY KEY,
            enabled    INTEGER NOT NULL DEFAULT 0
        )""")
        conn.commit()
    except Exception as e:
        print(f"[DEFAULT_MENU] table creation error: {e}")
    finally:
        conn.close()
 
 
@app.route("/api/default_menu/status", methods=["GET"])
def api_default_menu_status():
    """Get default menu enabled status for all shifts."""
    ensure_default_menu_tables()
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT shift_code, enabled FROM default_menu_enabled"
        ).fetchall()
        status = {r["shift_code"]: bool(r["enabled"]) for r in rows}
        return jsonify(success=True, status=status)
    finally:
        conn.close()
 
 
@app.route("/api/default_menu/get", methods=["GET"])
def api_default_menu_get():
    """Get default menu items for a shift."""
    ensure_default_menu_tables()
    shift_code = (request.args.get("shift_code") or "").strip()
    if not shift_code:
        return jsonify(success=False, message="shift_code required"), 400
 
    conn = get_db_connection()
    try:
        enabled_row = conn.execute(
            "SELECT enabled FROM default_menu_enabled WHERE shift_code=?",
            (shift_code,)
        ).fetchone()
        enabled = bool(enabled_row["enabled"]) if enabled_row else False
 
        items = conn.execute("""
            SELECT d.item_code, d.qty, i.item_name, m.menu_name AS category
            FROM default_menu_config d
            JOIN items i ON i.item_code = d.item_code
            JOIN menu_codes m ON m.menu_code = i.menu_code
            WHERE d.shift_code = ?
            ORDER BY m.menu_name, i.item_name
        """, (shift_code,)).fetchall()
 
        return jsonify(
            success=True,
            enabled=enabled,
            shift_code=shift_code,
            items=[dict(r) for r in items]
        )
    finally:
        conn.close()
 
 
@app.route("/api/default_menu/save", methods=["POST"])
def api_default_menu_save():
    """Save default menu config for a shift."""
    ensure_default_menu_tables()
    data = request.get_json(force=True) or {}
    shift_code = (data.get("shift_code") or "").strip()
    enabled = bool(data.get("enabled", False))
    items = data.get("items") or []
 
    if not shift_code:
        return jsonify(success=False, message="shift_code required"), 400
 
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO default_menu_enabled (shift_code, enabled) VALUES (?, ?) "
            "ON CONFLICT(shift_code) DO UPDATE SET enabled = excluded.enabled",
            (shift_code, 1 if enabled else 0)
        )
        conn.execute(
            "DELETE FROM default_menu_config WHERE shift_code = ?",
            (shift_code,)
        )
        for it in items:
            code = (it.get("item_code") or "").strip()
            qty = max(1, int(it.get("qty", 1)))
            if code:
                conn.execute(
                    "INSERT OR REPLACE INTO default_menu_config (shift_code, item_code, qty) VALUES (?, ?, ?)",
                    (shift_code, code, qty)
                )
        conn.commit()
        return jsonify(success=True, message="Default menu saved")
    except Exception as e:
        return jsonify(success=False, message=str(e)), 500
    finally:
        conn.close()
 
 
@app.route("/api/default_menu/toggle", methods=["POST"])
def api_default_menu_toggle():
    """Toggle default menu enabled/disabled for a shift."""
    ensure_default_menu_tables()
    data = request.get_json(force=True) or {}
    shift_code = (data.get("shift_code") or "").strip()
    enabled = bool(data.get("enabled", False))
 
    if not shift_code:
        return jsonify(success=False, message="shift_code required"), 400
 
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO default_menu_enabled (shift_code, enabled) VALUES (?, ?) "
            "ON CONFLICT(shift_code) DO UPDATE SET enabled = excluded.enabled",
            (shift_code, 1 if enabled else 0)
        )
        conn.commit()
        return jsonify(success=True, enabled=enabled)
    finally:
        conn.close()
 
 
@app.route("/api/default_menu/check_for_slot", methods=["POST"])
def api_default_menu_check_for_slot():
    """
    Check if current time slot's shift has default menu enabled.
    If yes, return the default items so the order page can auto-submit.
    Called by menu_select.html after user authentication.
    """
    ensure_default_menu_tables()
    data = request.get_json(force=True) or {}
    emp_id = (data.get("emp_id") or "").strip()
 
    if not emp_id:
        return jsonify(success=False, message="emp_id required"), 400
 
    conn = get_db_connection()
    try:
        now = get_current_time_ui()
        slot_row = conn.execute("""
            SELECT ts.slot_code, ts.shift_code, ts.slot_name, ts.from_time, ts.to_time,
                   sh.shift_name
            FROM time_slots ts
            JOIN shifts sh ON sh.shift_code = ts.shift_code
            WHERE ts.from_time <= ? AND ts.to_time >= ?
            ORDER BY ts.from_time LIMIT 1
        """, (now, now)).fetchone()
 
        if not slot_row:
            return jsonify(success=True, has_default=False, reason="no_active_slot")
 
        shift_code = slot_row["shift_code"]
 
        enabled_row = conn.execute(
            "SELECT enabled FROM default_menu_enabled WHERE shift_code=?",
            (shift_code,)
        ).fetchone()
 
        if not enabled_row or not enabled_row["enabled"]:
            return jsonify(success=True, has_default=False, reason="not_enabled")
 
        items = conn.execute("""
            SELECT d.item_code, d.qty, i.item_name, m.menu_name AS category
            FROM default_menu_config d
            JOIN items i ON i.item_code = d.item_code
            JOIN menu_codes m ON m.menu_code = i.menu_code
            WHERE d.shift_code = ?
        """, (shift_code,)).fetchall()
 
        if not items:
            return jsonify(success=True, has_default=False, reason="no_items_configured")
 
        return jsonify(
            success=True,
            has_default=True,
            shift_code=shift_code,
            shift_name=slot_row["shift_name"],
            slot_code=slot_row["slot_code"],
            slot_name=slot_row["slot_name"],
            items=[dict(r) for r in items]
        )
    finally:
        conn.close()
 
 
@app.route("/api/default_menu/items_for_shift", methods=["GET"])
def api_default_menu_items_for_shift():
    """Get all available items for a given shift (for the config dropdown)."""
    shift_code = (request.args.get("shift_code") or "").strip()
    if not shift_code:
        return jsonify(success=False, message="shift_code required"), 400
 
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT i.item_code, i.item_name, m.menu_name AS category
            FROM items i
            JOIN menu_codes m ON m.menu_code = i.menu_code
            JOIN time_slots ts ON ts.slot_code = m.slot_code
            WHERE ts.shift_code = ?
            ORDER BY m.menu_name, i.item_name
        """, (shift_code,)).fetchall()
        return jsonify(success=True, items=[dict(r) for r in rows])
    finally:
        conn.close()
        
# ──────────────────────────────────────────────────────
#  Report Routes — add these to your app.py
# ──────────────────────────────────────────────────────




# ──────────────────────────────────────────────────────
#  Report Routes — CORRECTED for your app.py
#  Uses: get_db_connection(), orders table, shifts, time_slots
# ──────────────────────────────────────────────────────
@app.route('/api/report/consumption')
def report_consumption():
    date_val  = request.args.get('date',  '').strip() or None
    shift_val = request.args.get('shift', '').strip() or None
    slot_val  = request.args.get('slot',  '').strip() or None
    item_val  = request.args.get('item',  '').strip() or None

    conn = get_db_connection()
    try:
        where  = []
        params = []

        if date_val:
            where.append("DATE(o.order_time) = ?")
            params.append(date_val)
        if shift_val:
            where.append("sh.shift_name = ?")
            params.append(shift_val)
        if slot_val:
            where.append("ts.slot_name = ?")
            params.append(slot_val)
        if item_val:
            where.append("o.item_name = ?")
            params.append(item_val)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        base_join = """
            FROM orders o
            LEFT JOIN shifts sh ON sh.shift_code = o.shift_code
            LEFT JOIN time_slots ts ON ts.slot_code = o.slot_code
        """

        rows = conn.execute(f"""
            SELECT COALESCE(sh.shift_name, 'Unknown') AS shift_name,
                   o.item_name,
                   SUM(o.qty) AS count
            {base_join}
            {where_sql}
            GROUP BY sh.shift_name, o.item_name
            ORDER BY sh.shift_name, count DESC
        """, params).fetchall()
        by_shift = [dict(r) for r in rows]

        rows = conn.execute(f"""
            SELECT COALESCE(ts.slot_name, 'Unknown') AS slot_label,
                   o.item_name,
                   SUM(o.qty) AS count
            {base_join}
            {where_sql}
            GROUP BY ts.slot_name, o.item_name
            ORDER BY ts.slot_name, count DESC
        """, params).fetchall()
        by_slot = [dict(r) for r in rows]

        rows = conn.execute(f"""
            SELECT DATE(o.order_time) AS order_date,
                   o.item_name,
                   SUM(o.qty) AS count
            {base_join}
            {where_sql}
            GROUP BY DATE(o.order_time), o.item_name
            ORDER BY DATE(o.order_time) DESC, count DESC
        """, params).fetchall()
        by_day = [dict(r) for r in rows]

        summary_row = conn.execute(f"""
            SELECT COALESCE(SUM(o.qty), 0)       AS total_count,
                   COUNT(DISTINCT o.emp_id)       AS unique_users,
                   COUNT(DISTINCT o.item_name)    AS unique_items
            {base_join}
            {where_sql}
        """, params).fetchone()
        summary = dict(summary_row)

        return jsonify({
            'by_shift':     by_shift,
            'by_slot':      by_slot,
            'by_day':       by_day,
            'total_count':  summary['total_count'],
            'unique_users': summary['unique_users'],
            'unique_items': summary['unique_items'],
        })
    finally:
        conn.close()
@app.route('/api/report/print', methods=['POST'])
def report_print():
    """Print consumption report on thermal printer."""
    data = request.get_json(force=True) or {}
    tab     = data.get('tab', 'shift')
    rows    = data.get('rows', [])
    filters = data.get('filters', {})
    summary = data.get('summary', {})
    if not rows:
        return jsonify(success=False, message="No data to print"), 400
    MAX_PRINT_ROWS = 15
    truncated = len(rows) > MAX_PRINT_ROWS
    print_rows = rows[:MAX_PRINT_ROWS]
    title = {'shift': 'shift report', 'slot': 'slot report', 'day': 'day report'}.get(tab, 'report')
    f_date = filters.get('date', '')
    lines = []
    lines.append(f"{title} date: {f_date}")
    lines.append(f"total: {summary.get('total',0)}  users: {summary.get('users',0)}  items: {summary.get('items',0)}")
    for r in print_rows:
        col1  = r.get('col1', '')
        item  = r.get('item', '')
        count = r.get('count', 0)
        lines.append(f"{col1}   {item} : {count}")
    if truncated:
        lines.append(f"... +{len(rows) - MAX_PRINT_ROWS} more")
    lines.append(datetime.now().strftime('%I:%M:%S %p'))
    text = '\n'.join(lines) + '\n'

    def _bg_print():
        try:
            from fingerprint import print_user_id_and_cut
            print_user_id_and_cut(text)
        except Exception as e:
            print(f"[REPORT] Print error: {e}")

    threading.Thread(target=_bg_print, daemon=True).start()
    return jsonify(success=True, message="Printing...")
if __name__ == '__main__':
    # 1. Early migration on raw sqlite (before persistent connection opens)
    migrate_sqlite_schema()

    # 2. Full schema setup on raw sqlite
    create_users_table()

    # 3. Migration on persistent connection
    ensure_schema_migrations()

    # 4. Other table setup
    ensure_logs_table()
    fix_logs_table_nulls()
    ensure_birthday_table()

    # ── NEW: initialise RBAC after DB is ready ──────────────────────────
    init_rbac(app, get_db_connection)
    app.register_blueprint(rbac_bp)
    # ────────────────────────────────────────────────────────────────────

    # 5. Load fingerprint templates into sensor
    load_fingerprint_templates_on_startup()

    # 6. Start embedded agent
    start_embedded_agent()

    # 7. Persist settings
    set_setting("handoff_ttl_seconds", str(HANDOFF_TTL_SECONDS))
    saved_url = get_setting("public_base_url", "")
    if saved_url:
        app.config["PUBLIC_BASE_URL"] = saved_url

    # 8. Auto-reconnect saved devices
    load_and_reconnect_saved_devices()

    print("[STARTUP] Starting Flask app on port", app.config.get("APP_PORT", 5000))
    app.run(host='0.0.0.0', port=app.config.get("APP_PORT", 5000), debug=False, threaded=True)   
      
