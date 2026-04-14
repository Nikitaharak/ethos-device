# fingerprint.py — Core fingerprint functionality only (NO audio/LED effects)
# All effects are centralized in app.py via trigger_effects()
# FIXED: Persistent serial connection, no open/close per call
# FIXED: Correct GT-521F52 DATA packet format for get_template/set_template
#   DATA packet: 5A A5 | DeviceID(2) | Data(N) | Checksum(2)

import os
import time
import serial
import logging

logger = logging.getLogger("fp_transfer")

# =========================
# GT-521F52 Constants
# =========================
TEMPLATE_SIZE = 498      # bytes per fingerprint template
DEVICE_ID     = 0x0001   # fixed in GT-521F52 datasheet

# =========================
# ---- Fingerprint I/O ----
# =========================
class Fingerprint:
    """
    GT-521Fxx UART protocol minimal driver.
    Core functionality ONLY - all effects handled by app.py

    FIXED VERSION:
    - Serial port stays open persistently (no open/close per identify call)
    - identify() is lean: capture + identify only, no side effects
    - is_open property to check connection state
    - get_template/set_template use correct GT-521F52 DATA packet format:
      5A A5 | DeviceID(2) | Data(498) | Checksum(2)

    - identify():
        * returns user_id (int) on recognized (present + verified)
        * returns -1 on present but NOT recognized
        * returns None on no finger / capture fail / timeouts (silent)
    """
    COMMANDS = {
        'Open': 0x01, 'Close': 0x02, 'CmosLed': 0x12,
        'GetEnrollCount': 0x20, 'CheckEnrolled': 0x21,
        'EnrollStart': 0x22, 'Enroll1': 0x23, 'Enroll2': 0x24, 'Enroll3': 0x25,
        'IsPressFinger': 0x26, 'CaptureFinger': 0x60,
        'DeleteID': 0x40, 'DeleteAll': 0x41,
        'Verify1_1': 0x50, 'Identify1_N': 0x51,
        'GetTemplate': 0x70, 'SetTemplate': 0x71,
        'Ack': 0x30, 'Nack': 0x31
    }
    PKT_RES = (0x55, 0xAA)
    PKT_DATA = (0x5A, 0xA5)

    def __init__(self, port, baud=9600, timeout=2):
        self.port = port
        self.baud = baud
        self.timeout = timeout
        self.ser = None
        self._opened = False   # tracks whether Open command was sent

    @property
    def is_open(self):
        return self.ser is not None and self.ser.is_open and self._opened

    # ---------- Serial helpers ----------
    def _ensure_open(self):
        if self.ser is None or not self.ser.is_open:
            self._open_serial()

    def _open_serial(self):
        try:
            if self.ser and self.ser.is_open:
                self.ser.close()
            self.ser = serial.Serial(
                self.port, self.baud,
                timeout=self.timeout,
                write_timeout=self.timeout
            )
            time.sleep(0.3)   # reduced from 0.5 — still safe for GT-521
        except Exception as e:
            print(f"[FP] serial open error: {e}")
            self.ser = None
            self._opened = False
            raise

    def init(self):
        """Quick sanity check — open serial, send Open/Close, return True/False."""
        try:
            self._open_serial()
            self.open()
            self._flush()
            # Don't call close() here so the port stays ready
            return True
        except Exception as e:
            print(f"[FP] init failed: {e}")
            self.force_reset()
            return False

    def open_persistent(self):
        """
        Open serial port AND send the Open command once.
        Call this once at startup instead of open()/close() on every login.
        Returns True on success.
        """
        try:
            self._ensure_open()
            ok = self._send_and_ack('Open', silent=True)
            if ok:
                self._flush()
                self._opened = True
            return ok
        except Exception as e:
            print(f"[FP] open_persistent failed: {e}")
            self._opened = False
            return False

    def _send_packet(self, cmd, param=0):
        try:
            self._ensure_open()
            code = self.COMMANDS[cmd]
            p = [(param >> (8 * i)) & 0xFF for i in range(4)]
            pkt = bytearray(12)
            pkt[0:2] = b'\x55\xAA'
            pkt[2:4] = b'\x01\x00'
            pkt[4:8] = bytes(p)
            pkt[8] = code & 0xFF
            pkt[9] = (code >> 8) & 0xFF
            chk = sum(pkt[:10])
            pkt[10] = chk & 0xFF
            pkt[11] = (chk >> 8) & 0xFF
            if self.ser and self.ser.writable():
                self.ser.write(pkt)
                return True
        except Exception as e:
            print(f"[FP] send error: {e}")
            self.force_reset()
        return False

    def _read(self, silent=False):
        try:
            if self.ser is None or not self.ser.is_open:
                return None
            b = self.ser.read(1)
            if not b:
                return None
            return b[0]
        except Exception as e:
            if not silent:
                print(f"[FP] read byte error: {e}")
            return None

    def _read_header(self, silent=False):
        return self._read(silent), self._read(silent)

    def _read_packet(self, silent=False):
        try:
            if self.ser is None or not self.ser.is_open:
                return False, 0, 0, None

            max_header_reads = 24
            attempts = 0
            while attempts < max_header_reads:
                h1, h2 = self._read_header(silent=True)
                if h1 is None or h2 is None:
                    if not silent:
                        print("[FP] read packet: timeout waiting for response")
                    return False, 0, 0, None
                if (h1, h2) == self.PKT_RES:
                    break
                attempts += 1

            if attempts >= max_header_reads:
                if not silent:
                    print("[FP] read packet: no valid header found")
                return False, 0, 0, None

            body = self.ser.read(10)
            if len(body) < 10:
                if not silent:
                    print("[FP] read packet: timeout reading body")
                return False, 0, 0, None

            pkt = bytes([h1, h2]) + body
            ack = (pkt[8] == self.COMMANDS['Ack'])
            param = int.from_bytes(pkt[4:8], 'little')
            res = int.from_bytes(pkt[8:10], 'little')
            # NOTE: Do NOT try to read data packets here.
            # Commands that return data (GetTemplate) handle it explicitly.
            return ack, param, res, None

        except Exception as e:
            if not silent:
                print(f"[FP] read packet error: {e}")
            return None, None, None, None

    def _flush(self):
        if not self.ser:
            return
        try:
            while self.ser.in_waiting:
                self.ser.read(self.ser.in_waiting)
        except Exception:
            self.force_reset()

    def _send_and_ack(self, cmd, param=0, silent=False):
        if self._send_packet(cmd, param):
            ack, _, _, _ = self._read_packet(silent=silent)
            return ack if ack is not None else False
        return False

    # ─── New helpers matching fptest.py GT-521F52 protocol ───────────────

    def _read_exact_bytes(self, n, timeout=5.0):
        """Read exactly n bytes from serial with timeout."""
        end = time.time() + timeout
        buf = bytearray()
        while len(buf) < n:
            chunk = self.ser.read(n - len(buf))
            if chunk:
                buf.extend(chunk)
            elif time.time() > end:
                raise TimeoutError(f"read_exact: wanted={n} got={len(buf)}")
        return bytes(buf)

    def _sync_to_header(self, header_bytes, timeout=5.0):
        """Scan serial input until 2-byte header pattern is found."""
        end = time.time() + timeout
        win = bytearray()
        plen = len(header_bytes)
        while time.time() < end:
            b = self.ser.read(1)
            if not b:
                continue
            win += b
            if len(win) > plen:
                win = win[-plen:]
            if bytes(win) == bytes(header_bytes):
                return True
        raise TimeoutError(f"sync_to_header timeout for {bytes(header_bytes).hex()}")

    def _read_response_ack(self, timeout=5.0):
        """
        Read a GT-521F52 response packet.
        Returns (ack: bool, param: int).
        Does NOT try to consume any following data packet.
        """
        self._sync_to_header(b'\x55\xAA', timeout)
        body = self._read_exact_bytes(10, timeout)
        pkt = b'\x55\xAA' + body

        rx_chk = int.from_bytes(pkt[10:12], 'little')
        calc_chk = sum(pkt[:10]) & 0xFFFF
        if rx_chk != calc_chk:
            raise ValueError(f"Response checksum mismatch: rx={rx_chk} calc={calc_chk}")

        ack = (pkt[8] == self.COMMANDS['Ack'])
        param = int.from_bytes(pkt[4:8], 'little')
        return ack, param

    def check_enrolled(self, idx):
        """Check if template slot is enrolled on sensor. Returns True/False."""
        try:
            return self._send_and_ack('CheckEnrolled', int(idx), silent=True)
        except Exception:
            return False

    def is_finger_pressed(self):
        """
        Check if a finger is physically present on the sensor.
        Uses IsPressFinger (0x26) command.
        Returns True if finger IS pressed, False otherwise.
        NOTE: param == 0 means finger IS pressed (per GT-521F52 datasheet).
        """
        try:
            self._ensure_open()
            if not self._send_packet('IsPressFinger', 0):
                return False
            ack, param, _, _ = self._read_packet(silent=True)
            if ack is None or not ack:
                return False
            # GT-521F52: param == 0 means finger IS pressed
            return param == 0
        except Exception:
            return False

    # ---------- High-level commands ----------
    def open(self):
        try:
            ok = self._send_and_ack('Open', silent=True)
            if ok:
                self._flush()
                self._opened = True
            return ok
        except Exception:
            self.force_reset()
            return False

    def close(self):
        try:
            self._opened = False
            return self._send_and_ack('Close', silent=True)
        except Exception:
            self.force_reset()
            return False

    def set_led(self, on):
        try:
            return self._send_and_ack('CmosLed', 1 if on else 0)
        except Exception:
            self.force_reset()
            return False

    # ---------- Enroll sequence ----------
    def start_enroll(self, idx):
        try:
            return self._send_and_ack('EnrollStart', int(idx))
        except Exception:
            self.force_reset()
            return False

    def enroll1(self):
        try:
            return self._send_and_ack('Enroll1')
        except Exception:
            self.force_reset()
            return False

    def enroll2(self):
        try:
            return self._send_and_ack('Enroll2')
        except Exception:
            self.force_reset()
            return False

    def enroll3(self):
        try:
            return self._send_and_ack('Enroll3')
        except Exception:
            self.force_reset()
            return False

    # ---------- Capture / Identify ----------
    def capture_finger(self, best=False):
        """
        Capture a finger image. Returns True on success.
        LED on during capture, off after.
        """
        try:
            self.set_led(True)
            if self._send_packet('CaptureFinger', 1 if best else 0):
                ack, _, _, _ = self._read_packet()
                self.set_led(False)
                return bool(ack)
            self.set_led(False)
        except Exception:
            try:
                self.set_led(False)
            except Exception:
                pass
            self.force_reset()
        return False

    def identify(self):
        """
        Identify finger against DB in sensor.

        Flow (optimised with finger-presence check):
          1. LED ON  — optical sensor needs backlight to detect finger
          2. IsPressFinger — lightweight presence check
          3. If no finger → return None (LED stays on for next poll)
          4. If finger present → CaptureFinger → Identify1_N → LED OFF

        Returns:
        - user_id (int) on success (present + verified)
        - -1 on present but NOT verified
        - None if no finger / capture failed / timeouts

        NO effects - all effects handled by app.py via trigger_effects()
        """
        try:
            # ── Step 0: Ensure sensor is initialized (Open command sent) ──
            if not self._opened:
                print("[FP-DEBUG] _opened=False, re-sending Open command")
                self._ensure_open()
                ok = self._send_and_ack('Open', silent=True)
                if ok:
                    self._flush()
                    self._opened = True
                    print("[FP-DEBUG] Open command OK — sensor ready")
                else:
                    print("[FP-DEBUG] Open command FAILED")
                    return None

            # ── Step 1: LED ON — sensor needs backlight for optical detection ──
            led_ok = self.set_led(True)
            print(f"[FP-DEBUG] set_led(True)={led_ok}  ser={self.ser is not None}  opened={self._opened}")

            if not led_ok:
                # LED failed — fall back to old capture_finger flow
                print("[FP-DEBUG] LED failed, falling back to capture_finger()")
                captured = self.capture_finger()
                if not captured:
                    return None
                if self._send_packet('Identify1_N'):
                    ack, p, _, _ = self._read_packet()
                    if ack and p != 0xFFFFFFFF and p >= 0:
                        return p
                    else:
                        return -1
                return None

            # ── Step 2: Check if finger is on the sensor ──────────────────
            pressed = self.is_finger_pressed()
            print(f"[FP-DEBUG] is_finger_pressed={pressed}")

            if not pressed:
                # No finger — LED stays on for next poll cycle
                return None

            # ── Step 3: Finger detected — capture + identify ─────────────
            # LED is already on, send CaptureFinger directly
            print("[FP-DEBUG] Finger detected! Capturing...")
            if not self._send_packet('CaptureFinger', 0):
                self.set_led(False)
                return None

            ack, _, _, _ = self._read_packet()
            if not ack:
                self.set_led(False)
                print("[FP-DEBUG] CaptureFinger failed (NACK)")
                return None  # capture failed (finger lifted too fast, etc.)

            # ── Step 4: Identify against enrolled templates ───────────────
            if self._send_packet('Identify1_N'):
                ack_id, p, _, _ = self._read_packet()
                self.set_led(False)
                print(f"[FP-DEBUG] Identify result: ack={ack_id} id={p}")
                if ack_id and p != 0xFFFFFFFF and p >= 0:
                    return p       # success — return user ID
                else:
                    return -1      # present but not verified

            self.set_led(False)
            return None

        except Exception as e:
            print(f"[FP-DEBUG] identify EXCEPTION: {e}")
            try:
                self.set_led(False)
            except Exception:
                pass
            msg = str(e).lower()
            if "timeout" in msg or "read timeout" in msg:
                self.force_reset()
                return None
            self.force_reset()
            return -1

    # ---------- Delete / Template ----------
    def delete(self, idx=None):
        try:
            if idx is None:
                self._send_packet('DeleteAll')
            else:
                self._send_packet('DeleteID', int(idx))
            ack, _, _, _ = self._read_packet()
            return bool(ack)
        except Exception:
            self.force_reset()
            return False

    def delete_all_fingers(self):
        return self.delete(idx=None)

    def get_template(self, idx):
        """
        Download template from sensor slot.
        Uses correct GT-521F52 DATA packet format:
          CMD packet  → sensor ACK
          sensor sends DATA packet: 5A A5 | DeviceID(2) | Data(498) | Checksum(2)
        Returns 498 bytes on success, None on failure.
        """
        try:
            self._ensure_open()
            self._flush()

            if not self._send_packet('GetTemplate', int(idx)):
                logger.error(f"get_template: send GetTemplate failed for slot {idx}")
                return None

            # Phase 1: Read ACK response
            ack, param = self._read_response_ack(timeout=5.0)
            if not ack:
                logger.error(f"get_template: NACK for slot {idx}, param={param}")
                return None

            # Phase 2: Read DATA packet
            # Format: 5A A5 | DeviceID(2) | Data(498) | Checksum(2)
            self._sync_to_header(b'\x5A\xA5', timeout=8.0)

            dev_bytes = self._read_exact_bytes(2, timeout=8.0)
            data = self._read_exact_bytes(TEMPLATE_SIZE, timeout=8.0)
            chk_bytes = self._read_exact_bytes(2, timeout=8.0)

            # Verify checksum: sum of (5A A5 + DeviceID + Data)
            pkt_for_chk = b'\x5A\xA5' + dev_bytes + data
            calc_chk = sum(pkt_for_chk) & 0xFFFF
            rx_chk = int.from_bytes(chk_bytes, 'little')
            if calc_chk != rx_chk:
                logger.error(f"get_template: data checksum mismatch "
                             f"rx={rx_chk} calc={calc_chk} for slot {idx}")
                return None

            logger.info(f"get_template: OK slot={idx} size={len(data)}")
            return data

        except Exception as e:
            logger.error(f"get_template error slot={idx}: {e}")
            print(f"[FP] get_template error: {e}")
            self.force_reset()
            return None

    def set_template(self, idx, template_data):
        """
        Upload template to sensor slot.
        Uses correct GT-521F52 two-phase protocol:
          Phase 1: SetTemplate CMD → ACK
          Phase 2: DATA packet: 5A A5 | DeviceID(2) | Data(498) | Checksum(2) → ACK
        Returns True on success, False on failure.
        """
        try:
            if not isinstance(template_data, (bytes, bytearray)):
                logger.error(f"set_template: invalid type {type(template_data)}")
                return False

            template_data = bytes(template_data)
            if len(template_data) != TEMPLATE_SIZE:
                logger.error(f"set_template: bad size {len(template_data)} "
                             f"(need {TEMPLATE_SIZE}) for slot {idx}")
                return False

            self._ensure_open()
            self._flush()

            # Delete existing template if present (ignore errors)
            try:
                self._send_packet('DeleteID', int(idx))
                self._read_packet(silent=True)
                time.sleep(0.1)
            except Exception:
                pass
            self._flush()

            # Phase 1: SetTemplate CMD
            # param: LOWORD=ID, HIWORD!=0 disables duplicate check
            param = (int(idx) & 0xFFFF) | (1 << 16)
            if not self._send_packet('SetTemplate', param):
                logger.error(f"set_template: send SetTemplate failed for slot {idx}")
                return False

            # Phase 1 ACK
            ack, resp_param = self._read_response_ack(timeout=5.0)
            if not ack:
                logger.error(f"set_template: SetTemplate NACK slot={idx} param={resp_param}")
                return False

            # Phase 2: Send DATA packet
            # Format: 5A A5 | DeviceID(2) | Data(498) | Checksum(2)
            dev_bytes = DEVICE_ID.to_bytes(2, 'little')
            data_pkt = b'\x5A\xA5' + dev_bytes + template_data
            checksum = (sum(data_pkt) & 0xFFFF).to_bytes(2, 'little')
            self.ser.write(data_pkt + checksum)
            self.ser.flush()
            time.sleep(0.3)

            # Phase 2 final ACK
            ack2, param2 = self._read_response_ack(timeout=5.0)
            if not ack2:
                logger.error(f"set_template: final NACK slot={idx} param={param2}")
                return False

            logger.info(f"set_template: OK slot={idx} size={len(template_data)}")
            return True

        except Exception as e:
            logger.error(f"set_template error slot={idx}: {e}")
            print(f"[FP] set_template error: {e}")
            self.force_reset()
            return False

    # ---------- Recovery ----------
    def force_reset(self):
        """Close and reopen the serial port + re-send Open command."""
        self._opened = False
        try:
            if self.ser and self.ser.is_open:
                self.ser.close()
        except Exception:
            pass
        self.ser = None
        time.sleep(0.3)
        try:
            self._open_serial()
            # Re-send Open command — sensor ignores all commands without it
            ok = self._send_and_ack('Open', silent=True)
            if ok:
                self._flush()
                self._opened = True
                print("[FP] force_reset: sensor re-opened successfully")
            else:
                print("[FP] force_reset: Open command failed (NACK)")
        except Exception as e:
            print(f"[FP] force_reset: recovery failed: {e}")

def print_user_id_and_cut(text):
    """
    Print text to thermal printer (POS EC58 on /dev/serial0) and cut paper.
    Falls back to USB device paths, then logs to console if nothing found.
    """
    try:
        import serial
        GS = b"\x1d"
        CUT_FULL = GS + b"V\x00"
        import time
        with serial.Serial("/dev/ttyAMA0", 9600, timeout=2) as printer:
            printer.write(text.encode("ascii", "replace"))
            printer.write(b"\n\n\n")
            printer.flush()
            time.sleep(0.5)
            printer.write(CUT_FULL)
            printer.flush()
        print(f"[Printer] Printed to /dev/serial0")
        return
    except Exception as e:
        print(f"[Printer] serial0 failed: {e}")
    try:
        for dev in ("/dev/usb/lp0", "/dev/usb/lp1", "/dev/ttyUSB1"):
            if os.path.exists(dev):
                with open(dev, "wb") as printer:
                    printer.write(text.encode("ascii", "replace"))
                    printer.write(b"\n\n\n")
                    printer.write(b"\x1d\x56\x00")  # ESC/POS full cut
                    printer.flush()
                print(f"[Printer] Printed to {dev}")
                return
        print(f"[Printer] No device found. Receipt:\n{text}")
    except Exception as e:
        print(f"[Printer] Error: {e}")
