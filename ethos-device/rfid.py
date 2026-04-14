# rfid.py — Core RFID functionality only (NO audio/LED effects)
# All effects are centralized in app.py via trigger_effects()
# Uses rfid_card_map table (not users.rfid_cards)

import time
import threading
import sqlite3

from pirc522 import RFID

USERS_DB_PATH = "users.db"

RFID_RST = 22
RFID_BUS = 1
RFID_DEVICE = 0
rfid_sensor_lock = threading.Lock()


def _ensure_rfid_table():
    """Ensure rfid_card_map table exists."""
    conn = sqlite3.connect(USERS_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rfid_card_map (
            emp_id     TEXT PRIMARY KEY,
            rfid_card  TEXT UNIQUE NOT NULL,
            name       TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.commit()
    conn.close()


def rfid_read(timeout=10):
    """
    Blocking RFID tag read with timeout (seconds).
    Returns:
      (True, uid_str)           -> tag PRESENT, UID captured
      (False, "no_tag")         -> no tag detected within timeout
      (False, "<error string>") -> actual error
    NO effects - pure functionality.
    """
    end = time.time() + (timeout if timeout and timeout > 0 else 10)
    try:
        with rfid_sensor_lock:
            rdr = RFID(bus=RFID_BUS, device=RFID_DEVICE, pin_rst=RFID_RST, pin_irq=None)
            try:
                while time.time() < end:
                    err_req, _ = rdr.request()
                    if not err_req:
                        err_uid, uid = rdr.anticoll()
                        if not err_uid and uid:
                            return True, ''.join(str(i) for i in uid)
                        else:
                            return False, "rfid_uid_error"
                    time.sleep(0.05)
                return False, "no_tag"
            finally:
                try:
                    rdr.cleanup()
                except Exception:
                    pass
    except Exception as e:
        return False, f"rfid_exception:{e}"


def rfid_login():
    """
    Fast login - read RFID tag and return user info from rfid_card_map.
    Returns (success: bool, result: dict | str).
    NO effects - all effects handled by app.py via trigger_effects()
    """
    _ensure_rfid_table()
    ok, tag = rfid_read()
    if not ok:
        if tag == "no_tag":
            return False, "No card detected"
        return False, f"RFID read error: {tag}"

    try:
        conn = sqlite3.connect(USERS_DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT emp_id, name FROM rfid_card_map WHERE rfid_card = ?", (tag,)
        ).fetchone()
        conn.close()

        if row:
            return True, {"employee_id": row["emp_id"], "name": row["name"] or ""}
        return False, "Unknown RFID tag!"
    except Exception as e:
        return False, str(e)


def rfid_register(employee_id, name):
    """
    Register RFID tag for employee in rfid_card_map.
    Returns (success: bool, result: str | dict).
    NO effects - pure functionality.
    """
    _ensure_rfid_table()
    ok, tag = rfid_read()
    if not ok:
        if tag == "no_tag":
            return False, "No card detected"
        return False, f"RFID read error: {tag}"

    try:
        conn = sqlite3.connect(USERS_DB_PATH)
        conn.row_factory = sqlite3.Row

        # Check if card already assigned to another employee
        existing = conn.execute(
            "SELECT emp_id, name FROM rfid_card_map WHERE rfid_card = ?", (tag,)
        ).fetchone()

        if existing:
            conn.close()
            if existing["emp_id"] == employee_id:
                return False, "This card is already registered to this employee."
            return False, {
                "duplicate": True,
                "emp_id": existing["emp_id"],
                "name": existing["name"] or "",
                "message": f"RFID already assigned to {existing['name']} ({existing['emp_id']})"
            }

        from datetime import datetime
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(
            """INSERT OR REPLACE INTO rfid_card_map (emp_id, rfid_card, name, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (employee_id, tag, name, now, now)
        )
        conn.commit()
        conn.close()
        return True, {"message": f"Registered {name} with tag {tag}", "uid": tag}
    except Exception as e:
        return False, str(e)


def rfid_edit(employee_id, new_name):
    """
    Edit employee name in rfid_card_map.
    Returns (success: bool, message: str).
    NO effects - pure functionality.
    """
    _ensure_rfid_table()
    conn = sqlite3.connect(USERS_DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT emp_id FROM rfid_card_map WHERE emp_id = ?", (employee_id,)
    ).fetchone()
    if not row:
        conn.close()
        return False, "Employee not found in RFID records."
    conn.execute(
        "UPDATE rfid_card_map SET name = ?, updated_at = datetime('now','localtime') WHERE emp_id = ?",
        (new_name, employee_id)
    )
    conn.commit()
    conn.close()
    return True, "Name updated successfully."


def rfid_delete(employee_id):
    """
    Delete RFID card record for employee from rfid_card_map.
    Returns (success: bool, message: str).
    NO effects - pure functionality.
    """
    _ensure_rfid_table()
    conn = sqlite3.connect(USERS_DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT emp_id FROM rfid_card_map WHERE emp_id = ?", (employee_id,)
    ).fetchone()
    if not row:
        conn.close()
        return False, "Employee not found in RFID records."
    conn.execute("DELETE FROM rfid_card_map WHERE emp_id = ?", (employee_id,))
    conn.commit()
    conn.close()
    return True, "RFID card deleted successfully."
