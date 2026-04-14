#!/usr/bin/env python3
"""
Device-side agent to:
- Send periodic heartbeats with identity and version
- Optionally trigger OTA checks
- Poll for control commands (e.g., remote shell/screen hooks to be added server-side)
"""
import json
import os
import subprocess
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from device_identity import load_identity, persist_identity

APP_DIR = Path(__file__).resolve().parent
VERSION_FILE = APP_DIR / "version.txt"
CURRENT_VERSION = VERSION_FILE.read_text().strip() if VERSION_FILE.exists() else "0.0.0"

# Default to current ngrok tunnel if no env vars are provided (replace when you have a fixed domain)
DEFAULT_CONTROL_BASE = "https://141b1fd0bde5.ngrok-free.app"
SERVER_BASE = (os.getenv("CONTROL_SERVER_BASE") or os.getenv("OTA_SERVER_BASE") or DEFAULT_CONTROL_BASE).rstrip("/")
HEARTBEAT_URL = os.getenv("HEARTBEAT_URL") or (f"{SERVER_BASE}/api/heartbeat" if SERVER_BASE else "")
COMMAND_URL = os.getenv("COMMAND_URL") or (f"{SERVER_BASE}/api/commands/pull" if SERVER_BASE else "")
AUTH_TOKEN = os.getenv("CONTROL_SERVER_TOKEN") or os.getenv("OTA_AUTH_TOKEN")
POLL_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "15"))  # faster for responsive actions
RUN_OTA_ON_START = os.getenv("RUN_OTA_ON_START", "true").lower() == "true"
SERVICE_NAME = os.getenv("APP_SERVICE_NAME", "ethos-device.service")

# Optional remote access commands (reverse SSH / screen share) are driven by envs below.
# Provide a ready-to-run command via SSH_TUNNEL_CMD, or set the parts:
# SSH_TUNNEL_HOST, SSH_TUNNEL_USER, SSH_TUNNEL_PORT (default 22), SSH_REMOTE_PORT (remote bind, e.g., 22222),
# SSH_KEY_PATH (path to private key). Screen share commands via SCREEN_SHARE_CMD_START / SCREEN_SHARE_CMD_STOP.
SSH_TUNNEL_CMD = os.getenv("SSH_TUNNEL_CMD")
SSH_TUNNEL_HOST = os.getenv("SSH_TUNNEL_HOST")
SSH_TUNNEL_USER = os.getenv("SSH_TUNNEL_USER")
SSH_TUNNEL_PORT = os.getenv("SSH_TUNNEL_PORT", "22")
SSH_REMOTE_PORT = os.getenv("SSH_REMOTE_PORT", "22222")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")
SCREEN_SHARE_CMD_START = os.getenv("SCREEN_SHARE_CMD_START")  # e.g., "x11vnc -forever -display :0"
SCREEN_SHARE_CMD_STOP = os.getenv("SCREEN_SHARE_CMD_STOP")    # e.g., "pkill x11vnc"

# Keep handles so we can stop tunnels/screen-share
SSH_TUNNEL_PROC = None
SCREEN_SHARE_PROC = None


def _headers(identity):
    h = {
        "User-Agent": "ethos-agent/1.0",
        "Content-Type": "application/json",
    }
    if AUTH_TOKEN:
        h["Authorization"] = f"Bearer {AUTH_TOKEN}"
    if identity.get("device_id"):
        h["X-Device-Id"] = identity["device_id"]
    if identity.get("uuid"):
        h["X-Device-Uuid"] = identity["uuid"]
    return h


def call_json(url, payload, identity):
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers=_headers(identity))
    with urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def send_heartbeat(identity):
    if not HEARTBEAT_URL:
        print("[agent] HEARTBEAT_URL not configured; skipping.")
        return None
    payload = {
        "device_id": identity.get("device_id"),
        "group": identity.get("group"),
        "serial": identity.get("serial"),
        "uuid": identity.get("uuid"),
        "version": CURRENT_VERSION,
        "online": True,
    }
    try:
        resp = call_json(HEARTBEAT_URL, payload, identity)
        print(f"[agent] Heartbeat sent -> {resp}")
        return resp
    except Exception as e:
        print(f"[agent] Heartbeat failed: {e}")
        return None


def handle_commands(commands):
    if not commands:
        return
    for cmd in commands:
        ctype = cmd.get("type")
        print(f"[agent] Received command: {ctype}")
        if ctype == "ota_check":
            subprocess.Popen(["/usr/bin/python3", str(APP_DIR / "updater.py")])
        elif ctype == "reboot":
            subprocess.Popen(["sudo", "systemctl", "reboot"])
        elif ctype == "restart_app":
            subprocess.Popen(["sudo", "systemctl", "restart", SERVICE_NAME])
        elif ctype == "shell":
            print("[agent] Shell command received, but transport is not implemented yet.")
        elif ctype == "ssh_tunnel":
            start_ssh_tunnel(cmd.get("payload"))
        elif ctype == "ssh_tunnel_stop":
            stop_ssh_tunnel()
        elif ctype == "screen_share":
            start_screen_share(cmd.get("payload"))
        elif ctype == "screen_share_stop":
            stop_screen_share()
        else:
            print(f"[agent] Unknown command type: {ctype}")


def pull_commands(identity):
    if not COMMAND_URL:
        return
    payload = {
        "device_id": identity.get("device_id"),
        "uuid": identity.get("uuid"),
    }
    try:
        resp = call_json(COMMAND_URL, payload, identity)
        cmds = resp.get("commands") if isinstance(resp, dict) else None
        handle_commands(cmds)
    except HTTPError as e:
        if e.code != 404:
            print(f"[agent] Command poll failed: {e}")
    except URLError as e:
        print(f"[agent] Command poll network error: {e}")
    except Exception as e:
        print(f"[agent] Command poll error: {e}")


def start_ssh_tunnel(payload=None):
    """Start a reverse SSH tunnel for remote shell. Requires SSH_* envs or SSH_TUNNEL_CMD."""
    global SSH_TUNNEL_PROC
    if SSH_TUNNEL_PROC and SSH_TUNNEL_PROC.poll() is None:
        print("[agent] SSH tunnel already running")
        return

    cmd = None
    if payload and isinstance(payload, dict) and payload.get("cmd"):
        cmd = payload["cmd"]
    elif SSH_TUNNEL_CMD:
        cmd = SSH_TUNNEL_CMD
    elif SSH_TUNNEL_HOST and SSH_TUNNEL_USER:
        # Build a basic reverse tunnel command: remote_port -> local 22
        base = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-N",
            "-R", f"{SSH_REMOTE_PORT}:localhost:22",
            "-p", str(SSH_TUNNEL_PORT),
        ]
        if SSH_KEY_PATH:
            base.extend(["-i", SSH_KEY_PATH])
        base.append(f"{SSH_TUNNEL_USER}@{SSH_TUNNEL_HOST}")
        cmd = " ".join(base)

    if not cmd:
        print("[agent] SSH tunnel command not configured; set SSH_TUNNEL_CMD or SSH_TUNNEL_HOST/USER/etc.")
        return

    try:
        print(f"[agent] Starting SSH tunnel: {cmd}")
        SSH_TUNNEL_PROC = subprocess.Popen(cmd, shell=True)
    except Exception as e:
        print(f"[agent] Failed to start SSH tunnel: {e}")


def stop_ssh_tunnel():
    global SSH_TUNNEL_PROC
    if SSH_TUNNEL_PROC and SSH_TUNNEL_PROC.poll() is None:
        SSH_TUNNEL_PROC.terminate()
        print("[agent] SSH tunnel stopped")
    SSH_TUNNEL_PROC = None


def start_screen_share(payload=None):
    """Start screen sharing using provided command or env SCREEN_SHARE_CMD_START."""
    global SCREEN_SHARE_PROC
    if SCREEN_SHARE_PROC and SCREEN_SHARE_PROC.poll() is None:
        print("[agent] Screen share already running")
        return

    cmd = None
    if payload and isinstance(payload, dict) and payload.get("cmd"):
        cmd = payload["cmd"]
    elif SCREEN_SHARE_CMD_START:
        cmd = SCREEN_SHARE_CMD_START
    if not cmd:
        print("[agent] Screen share command not configured; set SCREEN_SHARE_CMD_START or provide payload.cmd")
        return

    try:
        print(f"[agent] Starting screen share: {cmd}")
        SCREEN_SHARE_PROC = subprocess.Popen(cmd, shell=True)
    except Exception as e:
        print(f"[agent] Failed to start screen share: {e}")


def stop_screen_share():
    global SCREEN_SHARE_PROC
    if SCREEN_SHARE_PROC and SCREEN_SHARE_PROC.poll() is None:
        SCREEN_SHARE_PROC.terminate()
        print("[agent] Screen share stopped")
    SCREEN_SHARE_PROC = None


def main():
    identity = load_identity()
    persist_identity(identity)
    print(f"[agent] Starting with device_id={identity.get('device_id')} group={identity.get('group')} version={CURRENT_VERSION}")

    if RUN_OTA_ON_START:
        try:
            subprocess.Popen(["/usr/bin/python3", str(APP_DIR / "updater.py")])
        except Exception as e:
            print(f"[agent] Failed to trigger updater: {e}")

    while True:
        send_heartbeat(identity)
        pull_commands(identity)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
