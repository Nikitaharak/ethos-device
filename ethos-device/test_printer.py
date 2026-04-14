"""
HS-EC58 Thermal Printer — Diagnostic & Test Script
Run: python3 test_printer.py
"""
import os
import glob
import subprocess

def find_devices():
    """List all possible printer device paths."""
    print("=" * 50)
    print("STEP 1: Scanning for connected devices")
    print("=" * 50)

    # Check serial ports
    serial_ports = [
        "/dev/serial0", "/dev/serial1",
        "/dev/ttyS0", "/dev/ttyS1",
        "/dev/ttyAMA0", "/dev/ttyAMA1",
    ]
    # Check USB serial
    usb_serial = glob.glob("/dev/ttyUSB*") + glob.glob("/dev/ttyACM*")
    # Check USB printer (lp)
    usb_lp = glob.glob("/dev/usb/lp*")

    all_ports = serial_ports + usb_serial + usb_lp

    found = []
    for dev in all_ports:
        exists = os.path.exists(dev)
        status = "FOUND" if exists else "---"
        if exists:
            found.append(dev)
        print(f"  {dev:25s} [{status}]")

    # Also check via ls
    print("\n-- /dev/serial* links --")
    try:
        result = subprocess.run(["ls", "-la", "/dev/serial0", "/dev/serial1"],
                                capture_output=True, text=True)
        print(result.stdout or "  (none)")
        if result.stderr:
            print(result.stderr.strip())
    except Exception:
        print("  (could not list)")

    print("\n-- USB devices (lsusb) --")
    try:
        result = subprocess.run(["lsusb"], capture_output=True, text=True)
        for line in result.stdout.strip().split("\n"):
            print(f"  {line}")
    except Exception:
        print("  (lsusb not available)")

    print("\n-- dmesg USB/serial (last 20 lines) --")
    try:
        result = subprocess.run(["dmesg"], capture_output=True, text=True)
        lines = [l for l in result.stdout.split("\n")
                 if any(k in l.lower() for k in ["usb", "tty", "serial", "printer", "lp", "ch341", "cp210", "ftdi", "pl2303"])]
        for line in lines[-20:]:
            print(f"  {line}")
        if not lines:
            print("  (no USB/serial/printer entries found)")
    except Exception:
        print("  (dmesg not available)")

    return found


def test_serial_port(dev, baud):
    """Try printing a test receipt on a serial port."""
    import serial
    print(f"\n  Trying serial: {dev} @ {baud} baud ... ", end="", flush=True)
    try:
        p = serial.Serial(dev, baud, timeout=2)
        ESC = b"\x1b"
        GS = b"\x1d"
        p.write(ESC + b"@")          # init
        p.write(b"\n\n")
        p.write(b"=== PRINTER TEST ===\n")
        p.write(b"999, GAPKING\n")
        p.write(b"ITM_04-Biryani\n")
        p.write(b"12-04-2026, 18:07:15\n")
        p.write(b"Hall_A, CAN_002\n")
        p.write(b"\n\n\n")
        p.write(GS + b"V\x00")       # full cut
        p.flush()
        p.close()
        print("SENT OK")
        return True
    except Exception as e:
        print(f"FAILED ({e})")
        return False


def test_file_device(dev):
    """Try printing via direct file write (for /dev/usb/lp*)."""
    print(f"\n  Trying file write: {dev} ... ", end="", flush=True)
    try:
        with open(dev, "wb") as p:
            p.write(b"\x1b@")            # init
            p.write(b"\n\n")
            p.write(b"=== PRINTER TEST ===\n")
            p.write(b"999, GAPKING\n")
            p.write(b"ITM_04-Biryani\n")
            p.write(b"12-04-2026, 18:07:15\n")
            p.write(b"Hall_A, CAN_002\n")
            p.write(b"\n\n\n")
            p.write(b"\x1d\x56\x00")     # full cut
            p.flush()
        print("SENT OK")
        return True
    except Exception as e:
        print(f"FAILED ({e})")
        return False


def main():
    found = find_devices()

    if not found:
        print("\n" + "=" * 50)
        print("NO DEVICES FOUND!")
        print("=" * 50)
        print("Check:")
        print("  1. Is the printer USB cable plugged in?")
        print("  2. Is the printer powered on?")
        print("  3. Run: sudo chmod 666 /dev/ttyUSB0  (or the correct port)")
        print("  4. If USB printer, try: sudo apt install printer-driver-escpr")
        return

    print("\n" + "=" * 50)
    print(f"STEP 2: Testing {len(found)} device(s)")
    print("=" * 50)

    bauds = [9600, 115200, 19200]

    for dev in found:
        if "usb/lp" in dev:
            test_file_device(dev)
        else:
            for baud in bauds:
                ok = test_serial_port(dev, baud)
                if ok:
                    print(f"\n  >> If receipt printed, your config is:")
                    print(f"     PORT = \"{dev}\"")
                    print(f"     BAUD = {baud}")
                    # Ask user
                    try:
                        ans = input(f"\n  Did the printer print? (y/n): ").strip().lower()
                        if ans == "y":
                            print(f"\n  SUCCESS! Use PORT=\"{dev}\" BAUD={baud}")
                            return
                    except EOFError:
                        pass

    print("\n" + "=" * 50)
    print("STEP 3: Summary")
    print("=" * 50)
    print("If nothing printed, check:")
    print("  1. Printer power and paper")
    print("  2. Cable connection (try a different USB port)")
    print("  3. Permissions: sudo chmod 666 /dev/ttyUSB0")
    print("  4. Add user to dialout group: sudo usermod -aG dialout $USER")
    print("  5. Reboot after permission changes")


if __name__ == "__main__":
    main()
