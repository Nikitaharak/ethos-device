# face_recognizer.py — file-based encodings only (no BLOB in DB)
# Effects (audio/LED/printer) are NEVER triggered from within this file.
# All effects are centralized in app.py via _dispatch_auth_result() → trigger_effects()
# play_wav / led_blink / print_user_id_and_cut kept here ONLY because app.py imports them.

import face_recognition
import sqlite3
import numpy as np
import cv2
import subprocess
import threading
import time
import base64
import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from image_helper import (
    load_image,
    load_encoding,
    save_encoding,
    delete_image,
    delete_encoding,
)

DB_PATH = "users.db"
AUDIO_PATH = "/home/admin/ethos-device/static/audio/"
PRINTER_PORT = "/dev/ttyAMA0"
PRINTER_BAUDRATE = 9600

# =========================
# ---- EXECUTORS/UTILS ----
# =========================
ui_exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ui")
io_exec = ThreadPoolExecutor(max_workers=4, thread_name_prefix="io")

_last_play = {}
_popup_cb = None
_last_popup = {"ts": 0.0, "key": ""}
_popup_cooldown = 0.15

def go_io(fn, *args, **kwargs):
    io_exec.submit(fn, *args, **kwargs)

def go_ui(fn, *args, **kwargs):
    ui_exec.submit(fn, *args, **kwargs)

# ------------------------------------------------------------------
# play_wav / print_user_id_and_cut / led_blink
# Kept ONLY because app.py imports them:
#   from face_recognizer import play_wav, led_blink, print_user_id_and_cut
# NEVER called from within face_recognizer.py itself — app.py owns all calls.
# ------------------------------------------------------------------

def play_wav(file_path, min_interval=0.6):
    try:
        now = time.monotonic()
        last = _last_play.get(file_path, 0.0)
        if now - last < float(min_interval):
            return
        _last_play[file_path] = now
        subprocess.Popen(["pw-play", file_path])
    except Exception as e:
        print(f"[AUDIO] error: {e}")

def print_user_id_and_cut(user_id):
    try:
        import serial
        GS = b"\x1d"
        CUT_FULL = GS + b"V\x00"
        with serial.Serial(PRINTER_PORT, PRINTER_BAUDRATE, timeout=2) as printer:
            printer.write(b"\n\n")
            printer.write(f"User ID: {user_id}\n".encode())
            printer.write(b"\n\n")
            printer.write(CUT_FULL)
            printer.flush()
    except Exception as e:
        print(f"[PRINTER] error: {e}")

def set_popup_callback(cb):
    global _popup_cb
    _popup_cb = cb

def _emit_popup(payload: dict):
    try:
        if _popup_cb is None:
            return
        has_img = '1' if (payload.get('image') or '') else '0'
        key = f"{payload.get('status','')}|{payload.get('emp_id','')}|{has_img}"
        now = time.monotonic()
        if key == _last_popup["key"] and (now - _last_popup["ts"]) < _popup_cooldown:
            return
        _last_popup["key"] = key
        _last_popup["ts"] = now
        go_ui(_popup_cb, payload)
    except Exception as e:
        print(f"[POPUP] error: {e}")

# =========================
# --------- LED -----------
# led_blink kept for app.py import only.
# led_success_bg / led_fail_bg REMOVED — they were the source of double-triggering.
# =========================
_led_available = False
_pixels = None

def _init_led_once():
    global _led_available, _pixels
    if _pixels is not None or _led_available:
        return
    try:
        import board
        import busio
        import neopixel_spi as neopixel
        NUM_PIXELS = 15
        LED_SPI = busio.SPI(board.SCK, MOSI=board.MOSI)
        _pixels = neopixel.NeoPixel_SPI(
            LED_SPI, NUM_PIXELS, auto_write=False, pixel_order=neopixel.GRB,
            frequency=6400000
        )
        _pixels.brightness = 1.0
        _pixels.fill((0, 0, 0))
        _pixels.show()
        _led_available = True
    except Exception as e:
        _pixels = None
        _led_available = False
        print(f"[LED] init skipped: {e}")

_init_led_once()

def _led_fill(color):
    if not _led_available or _pixels is None:
        return
    try:
        _pixels.fill(color)
        _pixels.show()
    except Exception:
        pass

def _led_off():
    _led_fill((0, 0, 0))

def led_blink(color, duration=0.35):
    """Kept for app.py import only. Called exclusively from trigger_effects()."""
    if not _led_available:
        return
    try:
        _led_fill(color)
        time.sleep(float(duration))
    finally:
        _led_off()

# led_success_bg and led_fail_bg intentionally REMOVED.
# They caused double audio+LED firing alongside app.py trigger_effects().

# =========================
# ---- IMAGE HELPERS ------
# =========================
_turbo = None
try:
    from turbojpeg import TurboJPEG
    _turbo = TurboJPEG()
except Exception:
    _turbo = None

def _jpeg_encode_b64(img_bgr, quality=70):
    try:
        if _turbo is not None:
            img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
            buf = _turbo.encode(img_rgb, quality=quality, jpeg_subsample=0)
            b64 = base64.b64encode(buf).decode()
            return "data:image/jpeg;base64," + b64
        ok, buf = cv2.imencode(".jpg", img_bgr, [int(cv2.IMWRITE_JPEG_QUALITY), int(quality)])
        if not ok:
            return ""
        b64 = base64.b64encode(buf).decode()
        return "data:image/jpeg;base64," + b64
    except Exception:
        return ""

def _crop_for_b64(frame, box, max_side=320):
    try:
        top, right, bottom, left = box
        h, w = frame.shape[:2]
        top = max(0, top); left = max(0, left)
        bottom = min(h, bottom); right = min(w, right)
        if bottom <= top or right <= left:
            return None
        roi = frame[top:bottom, left:right]
        rh, rw = roi.shape[:2]
        scale = min(1.0, float(max_side) / max(rh, rw))
        if scale < 1.0:
            roi = cv2.resize(roi, (int(rw*scale), int(rh*scale)), interpolation=cv2.INTER_AREA)
        return roi
    except Exception:
        return None

def _crop_and_b64(frame, box, jpeg_quality=70, max_side=320):
    roi = _crop_for_b64(frame, box, max_side=max_side)
    if roi is None:
        return ""
    return _jpeg_encode_b64(roi, quality=jpeg_quality)

# =========================
# ----- FACE RECOG --------
# =========================
class FaceRecognizer:
    def __init__(self, db_path=DB_PATH):
        self.sql_path = db_path
        self.encodings = []
        self.ids = []
        self.face_cascade = cv2.CascadeClassifier(
            cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        )
        self.index = None
        self._ensure_schema()
        self.load_all_encodings()

    def set_popup_callback(self, cb):
        set_popup_callback(cb)

    def _ensure_schema(self):
        """
        Ensure users table has required columns.
        Does NOT create face_encoding or display_image blobs.
        Does NOT fail if columns already exist.
        """
        conn = sqlite3.connect(self.sql_path)
        c = conn.cursor()

        # Create table if completely missing
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                emp_id        TEXT PRIMARY KEY,
                name          TEXT,
                created_at    TEXT DEFAULT (datetime('now', 'localtime')),
                updated_at    TEXT DEFAULT (datetime('now', 'localtime')),
                rfid_cards    TEXT,
                image_path    TEXT,
                encoding_path TEXT,
                role          TEXT DEFAULT 'User',
                template_id   INTEGER,
                birthdate     TEXT
            )
        """)
        conn.commit()

        # Get existing columns
        c.execute("PRAGMA table_info(users)")
        existing = {r[1] for r in c.fetchall()}

        # Add only missing columns (never add face_encoding or display_image)
        safe_adds = [
            ("name",          "ALTER TABLE users ADD COLUMN name TEXT"),
            ("created_at",    "ALTER TABLE users ADD COLUMN created_at TEXT"),
            ("updated_at",    "ALTER TABLE users ADD COLUMN updated_at TEXT"),
            ("rfid_cards",    "ALTER TABLE users ADD COLUMN rfid_cards TEXT"),
            ("image_path",    "ALTER TABLE users ADD COLUMN image_path TEXT"),
            ("encoding_path", "ALTER TABLE users ADD COLUMN encoding_path TEXT"),
            ("role",          "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'User'"),
            ("template_id",   "ALTER TABLE users ADD COLUMN template_id INTEGER"),
            ("birthdate",     "ALTER TABLE users ADD COLUMN birthdate TEXT"),
        ]
        for col, sql in safe_adds:
            if col not in existing:
                try:
                    c.execute(sql)
                    conn.commit()
                except Exception:
                    pass

        try:
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_emp_id ON users(emp_id)")
            conn.commit()
        except Exception:
            pass

        conn.close()

    def load_all_encodings(self):
        """
        Load all face encodings from .dat files on disk.
        Uses encoding_path from DB, falls back to face_encodings/{emp_id}.dat
        Never reads face_encoding BLOB from DB.
        """
        self.encodings = []
        self.ids = []

        conn = sqlite3.connect(self.sql_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        # Get emp_id + encoding_path from DB
        c.execute("SELECT emp_id, encoding_path FROM users WHERE emp_id IS NOT NULL")
        rows = c.fetchall()
        conn.close()

        for row in rows:
            emp_id = row["emp_id"]
            enc_path = row["encoding_path"]

            enc_bytes = None

            # Try DB-stored path first
            if enc_path:
                p = Path(enc_path)
                if not p.is_absolute():
                    p = Path.cwd() / enc_path
                if p.exists():
                    try:
                        enc_bytes = p.read_bytes()
                    except Exception:
                        enc_bytes = None

            # Fallback: try standard location face_encodings/{emp_id}.dat
            if not enc_bytes:
                fallback = Path("face_encodings") / f"{emp_id}.dat"
                if fallback.exists():
                    try:
                        enc_bytes = fallback.read_bytes()
                    except Exception:
                        enc_bytes = None

            # Also try image_helper load_encoding
            if not enc_bytes:
                try:
                    enc_bytes = load_encoding(emp_id)
                except Exception:
                    enc_bytes = None

            if not enc_bytes:
                continue

            # Parse encoding
            arr = None
            for dtype in (np.float32, np.float64):
                elem = np.dtype(dtype).itemsize
                if len(enc_bytes) % elem == 0:
                    candidate = np.frombuffer(enc_bytes, dtype=dtype)
                    if candidate.size % 128 == 0:
                        arr = candidate.reshape(-1, 128)[0].astype(np.float32)
                        break

            if arr is None:
                continue

            self.encodings.append(arr)
            self.ids.append(emp_id)

        self.build_index()
        print(f"[FACE] Loaded {len(self.encodings)} encodings from disk")

    def build_index(self):
        if len(self.encodings) == 0:
            self.index = None
            return
        encs = np.vstack(self.encodings).astype('float32')
        try:
            import faiss
            self.index = faiss.IndexFlatL2(128)
            self.index.add(encs)
        except ImportError:
            self.index = None
            print("[FACE] faiss not installed, using CPU fallback")

    def detect_faces(self, frame):
        gray = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
        faces = self.face_cascade.detectMultiScale(gray, 1.1, 4)
        boxes = []
        for (x, y, w, h) in faces:
            boxes.append([y, x + w, y + h, x])
        return boxes

    def _to_bgr(self, frame):
        try:
            if frame is None:
                return None
            if len(frame.shape) == 3 and frame.shape[2] == 3:
                return cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
        except Exception:
            pass
        return frame

    # ------------------------------------------------------------------
    # Popup emitters — UI data ONLY, zero audio/LED/printer side-effects
    # ------------------------------------------------------------------

    def _emit_success_instant_then_update(self, user_id, frame, box):
        """
        Emit success popup immediately (no image for speed),
        then fire background task to encode face crop and emit with image.
        NO audio / LED / printer — those fire via app.py _dispatch_auth_result().
        """
        _emit_popup({"status": "success", "emp_id": user_id, "name": "", "image": ""})
        def _encode_and_update():
            img_b64 = _crop_and_b64(frame, box, jpeg_quality=65, max_side=300)
            if img_b64:
                _emit_popup({"status": "success", "emp_id": user_id, "name": "", "image": img_b64})
        go_io(_encode_and_update)

    def _emit_denied_instant_then_update(self, frame, box):
        """
        Emit denied popup immediately (no image for speed),
        then fire background task to encode face crop and emit with image.
        NO audio / LED / printer — those fire via app.py _dispatch_auth_result().
        """
        _emit_popup({"status": "denied", "emp_id": "", "name": "", "image": ""})
        def _encode_and_update():
            img_b64 = _crop_and_b64(frame, box, jpeg_quality=65, max_side=300)
            if img_b64:
                _emit_popup({"status": "denied", "emp_id": "", "name": "", "image": img_b64})
        go_io(_encode_and_update)

    # ------------------------------------------------------------------
    # _parallel_success / _parallel_denied
    #
    # BEFORE: triggered audio + LED directly → caused double-firing with
    #         app.py trigger_effects() running the same effects simultaneously.
    # NOW:    emit popup event only. All effects come exclusively from app.py
    #         face_login → _dispatch_auth_result() → trigger_effects().
    #
    # trigger_effects param kept for call-site signature compatibility only.
    # It is intentionally ignored — do NOT restore the old behaviour.
    # ------------------------------------------------------------------

    def _parallel_success(self, user_id, frame, box, trigger_effects=False):
        """
        Popup event only. Audio + LED + Printer handled by app.py.
        trigger_effects param accepted but ignored.
        """
        self._emit_success_instant_then_update(user_id, frame, box)

    def _parallel_denied(self, frame, box, trigger_effects=False):
        """
        Popup event only. Audio + LED handled by app.py.
        trigger_effects param accepted but ignored.
        """
        self._emit_denied_instant_then_update(frame, box)

    # ------------------------------------------------------------------
    # Core recognition
    # ------------------------------------------------------------------

    def recognize(self, frame, trigger_effects=False):
        """
        Identify face in frame.

        Returns:
            str  — emp_id on successful match
            ""   — face detected but not matched / no encodings loaded
            None — no face detected at all (silent, caller should not show banner)

        trigger_effects param accepted for call-site compatibility but ignored.
        All effects fire upstream: app.py face_login → _dispatch_auth_result().
        """
        boxes = self.detect_faces(frame)
        if len(boxes) == 0:
            return None  # No face — silent

        box = boxes[0]
        face_encodings = face_recognition.face_encodings(frame, boxes)

        if len(face_encodings) == 0:
            self._parallel_denied(frame, box)
            return ""

        if self.index is None or not self.encodings:
            self._parallel_denied(frame, box)
            return ""

        query = face_encodings[0].astype('float32')

        try:
            # FAISS fast path
            D, I = self.index.search(np.expand_dims(query, 0), 1)
            if D[0][0] < 0.37 * 0.37:
                user_id = self.ids[I[0][0]]
                self._parallel_success(user_id, frame, box)
                return user_id
            else:
                self._parallel_denied(frame, box)
                return ""
        except Exception:
            # CPU fallback
            encs = np.vstack([e.astype('float32') for e in self.encodings])
            diffs = encs - query
            dists = np.sqrt((diffs * diffs).sum(axis=1))
            if dists.size:
                min_idx = int(np.argmin(dists))
                if dists[min_idx] < 0.50:
                    user_id = self.ids[min_idx]
                    self._parallel_success(user_id, frame, box)
                    return user_id
            self._parallel_denied(frame, box)
            return ""

    def save_face(self, frame):
        """
        Extract face encoding from frame.
        Returns (True, encoding_array) or (False, error_message).
        Does NOT save anything to DB or disk — caller handles saving.
        NO effects of any kind.
        """
        boxes = self.detect_faces(frame)
        if len(boxes) == 0:
            return False, "No face detected."
        if len(boxes) != 1:
            return False, "Multiple faces detected. Please ensure only one face is visible."

        encoding = face_recognition.face_encodings(frame, boxes)
        if not encoding:
            return False, "Face could not be encoded. Please try again with better lighting."

        return True, encoding[0]

    def find_duplicate(self, frame, tolerance=0.37):
        """
        Returns dict {emp_id, name} if face already registered, else None.
        Uses FAISS index for speed. NO effects of any kind.
        tolerance matches the login threshold (0.37).
        """
        boxes = self.detect_faces(frame)
        if len(boxes) != 1 or self.index is None:
            return None

        encoding = face_recognition.face_encodings(frame, boxes)
        if not encoding:
            return None

        query = encoding[0].astype('float32')

        # FAISS fast path
        D, I = self.index.search(np.expand_dims(query, 0), 1)
        if D[0][0] < (tolerance * tolerance):
            emp_id = self.ids[I[0][0]]
            conn = sqlite3.connect(self.sql_path)
            row = conn.execute(
                "SELECT name FROM users WHERE emp_id=?", (emp_id,)
            ).fetchone()
            conn.close()
            return {
                "emp_id": emp_id,
                "name": row[0] if row else "Unknown",
            }
        return None

    def update_user_encoding(self, frame, emp_id):
        """
        Re-encode face and save .dat file + update DB encoding_path.
        Does NOT write any BLOB to DB.
        Emits popup only — NO audio/LED/printer (handled by app.py caller).
        """
        boxes = self.detect_faces(frame)
        if len(boxes) != 1:
            if len(boxes) > 1:
                self._parallel_denied(frame, boxes[0] if boxes else [0, 1, 1, 0])
            return False

        encoding = face_recognition.face_encodings(frame, boxes)
        if not encoding:
            self._parallel_denied(frame, boxes[0])
            return False

        # Save encoding .dat file
        encoding_bytes = encoding[0].astype(np.float32).tobytes()
        enc_path = save_encoding(emp_id, encoding_bytes)

        # Update DB with file path only — no BLOBs
        conn = sqlite3.connect(self.sql_path)
        c = conn.cursor()
        now = __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        c.execute(
            "UPDATE users SET encoding_path=?, updated_at=? WHERE emp_id=?",
            (enc_path, now, emp_id),
        )
        conn.commit()
        result = c.rowcount
        conn.close()

        self.load_all_encodings()

        if result > 0:
            box = boxes[0]
            # Popup only — NO play_wav / led_success_bg / print_user_id_and_cut here.
            # app.py face_edit route calls trigger_effects('registered', emp_id) after this returns.
            _emit_popup({"status": "success", "emp_id": emp_id, "name": "", "image": ""})
            def _encode_only():
                img_b64 = _crop_and_b64(frame, box, jpeg_quality=65, max_side=300)
                if img_b64:
                    _emit_popup({"status": "success", "emp_id": emp_id, "name": "", "image": img_b64})
            go_io(_encode_only)
        else:
            self._parallel_denied(frame, boxes[0])

        return result > 0

    def delete_user(self, emp_id):
        """
        Delete user from DB and remove .dat + image files from disk.
        Emits popup only — NO audio/LED/printer (handled by app.py delete_user route).
        """
        conn = sqlite3.connect(self.sql_path)
        c = conn.cursor()
        c.execute("DELETE FROM users WHERE emp_id=?", (emp_id,))
        conn.commit()
        result = c.rowcount
        conn.close()

        delete_image(emp_id)
        delete_encoding(emp_id)
        self.load_all_encodings()

        if result > 0:
            # Popup only — app.py delete_user route calls trigger_effects('deleted') after this.
            _emit_popup({"status": "success", "emp_id": emp_id, "name": "", "image": ""})

        return result > 0
