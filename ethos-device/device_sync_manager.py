"""
Device Synchronization Manager - World-Class Edition
Handles device discovery, auto-reconnection, template transferring, and database synchronization.

Features:
- Automatic reconnection on boot/restart
- Connection health monitoring with auto-recovery
- Intelligent retry logic with exponential backoff
- Persistent connection state management
- Enhanced device discovery
- Chunked message support for large payloads
"""

import os
import json
import time
import socket
import threading
import logging
import base64
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Callable
from collections import defaultdict

logger = logging.getLogger(__name__)

class DeviceSyncManager:
    """
    World-class device synchronization manager with automatic reconnection
    and connection health monitoring.
    """

    def __init__(self, get_db_conn_func, udp_port=5006, broadcast_addr="255.255.255.255",
                 on_template_received=None):
        """
        Initialize the DeviceSyncManager.

        Args:
            get_db_conn_func: Function to get database connection
            udp_port: UDP port for communication
            broadcast_addr: Broadcast address for discovery
            on_template_received: Optional callback function called after template is received
        """
        self.get_db_conn = get_db_conn_func
        self.udp_port = udp_port
        self.broadcast_addr = broadcast_addr
        self.on_template_received = on_template_received

        # Device tracking
        self.known_devices = {}  # ip -> device_info
        self.connected_devices = set()  # Set of IPs that are connected
        self.devices_lock = threading.Lock()

        # Connection health tracking
        self.device_health = defaultdict(lambda: {
            "last_ping": 0,
            "last_pong": 0,
            "failures": 0,
            "consecutive_failures": 0
        })
        self.health_lock = threading.Lock()

        # Sync tracking
        self.last_sync_date = None
        self.sync_in_progress = False
        self.sync_lock = threading.Lock()

        # Chunked message reassembly
        self.message_chunks = {}  # chunk_id -> {chunks, total_chunks, timestamp}
        self.chunks_lock = threading.Lock()

        # Offline sync queue - stores templates for offline devices
        self.offline_sync_queue = defaultdict(list)  # device_ip -> [list of emp_ids to sync]
        self.offline_queue_lock = threading.Lock()

        # File paths
        self.face_encodings_dir = "./face_encodings"
        self.users_img_dir = "./users_img"
        self.fingerprint_dir = "./fingerprint_encodings"

        # Ensure directories exist
        os.makedirs(self.face_encodings_dir, exist_ok=True)
        os.makedirs(self.users_img_dir, exist_ok=True)
        os.makedirs(self.fingerprint_dir, exist_ok=True)

        # Auto-reconnection settings
        self.auto_reconnect_enabled = True
        self.reconnect_interval = 30  # seconds
        self.max_reconnect_attempts = 5
        self.reconnect_backoff = 1.5  # exponential backoff multiplier

        # Saved devices (loaded from database)
        self.saved_devices = set()  # Set of IPs that should auto-connect

        # Background thread control
        self.running = True
        self.threads = []

        # Start background monitors
        self._start_background_threads()

        logger.info("DeviceSyncManager initialized with auto-reconnection enabled")

    def _start_background_threads(self):
        """Start all background monitoring threads"""
        # Connection health monitor
        health_thread = threading.Thread(
            target=self._connection_health_monitor,
            daemon=True,
            name="ConnectionHealthMonitor"
        )
        health_thread.start()
        self.threads.append(health_thread)

        # Auto-reconnection monitor
        reconnect_thread = threading.Thread(
            target=self._auto_reconnect_monitor,
            daemon=True,
            name="AutoReconnectMonitor"
        )
        reconnect_thread.start()
        self.threads.append(reconnect_thread)

        # Midnight sync monitor
        midnight_thread = threading.Thread(
            target=self._midnight_sync_monitor,
            daemon=True,
            name="MidnightSyncMonitor"
        )
        midnight_thread.start()
        self.threads.append(midnight_thread)

        # Chunk cleanup monitor
        cleanup_thread = threading.Thread(
            target=self._chunk_cleanup_monitor,
            daemon=True,
            name="ChunkCleanupMonitor"
        )
        cleanup_thread.start()
        self.threads.append(cleanup_thread)

        logger.info(f"Started {len(self.threads)} background monitoring threads")

    def load_saved_devices(self, device_ips: List[str]):
        """
        Load saved devices from persistent storage.
        These devices will be auto-reconnected on startup and after disconnection.

        Args:
            device_ips: List of device IPs to auto-connect
        """
        with self.devices_lock:
            self.saved_devices = set(ip for ip in device_ips if ip)
            logger.info(f"Loaded {len(self.saved_devices)} saved devices for auto-reconnection")

        # Immediately try to connect to all saved devices
        if device_ips:
            self._reconnect_saved_devices()

    def _reconnect_saved_devices(self):
        """Attempt to reconnect to all saved devices"""
        if not self.saved_devices:
            return

        logger.info(f"Attempting to reconnect to {len(self.saved_devices)} saved devices...")

        for ip in list(self.saved_devices):
            try:
                # Check if already connected
                if ip in self.connected_devices:
                    logger.debug(f"Device {ip} already connected, skipping")
                    continue

                # Attempt connection
                success, error = self.connect_device(ip)
                if success:
                    logger.info(f"✓ Auto-reconnected to {ip}")
                else:
                    logger.warning(f"✗ Failed to auto-reconnect to {ip}: {error}")

            except Exception as e:
                logger.error(f"Error reconnecting to {ip}: {e}")

    def _connection_health_monitor(self):
        """
        Background thread that monitors connection health.
        Sends periodic pings and checks for stale connections.
        """
        ping_interval = 15  # seconds
        stale_threshold = 60  # seconds

        logger.info("Connection health monitor started")

        while self.running:
            try:
                time.sleep(ping_interval)

                current_time = time.time()

                with self.devices_lock:
                    connected_ips = list(self.connected_devices)

                for ip in connected_ips:
                    try:
                        # Send ping
                        self._send_ping(ip)

                        with self.health_lock:
                            health = self.device_health[ip]
                            health["last_ping"] = current_time

                            # Check if device is stale (no pong response)
                            time_since_pong = current_time - health["last_pong"]

                            if time_since_pong > stale_threshold:
                                health["consecutive_failures"] += 1
                                logger.warning(
                                    f"Device {ip} not responding (failures: {health['consecutive_failures']})"
                                )

                                # If too many failures, disconnect and trigger reconnect
                                if health["consecutive_failures"] >= 3:
                                    logger.error(f"Device {ip} unresponsive, disconnecting")
                                    self.disconnect_device(ip)

                    except Exception as e:
                        logger.error(f"Error pinging device {ip}: {e}")

            except Exception as e:
                logger.error(f"Error in connection health monitor: {e}", exc_info=True)

    def _send_ping(self, ip: str):
        """Send ping message to device"""
        payload = {
            "type": "ping",
            "timestamp": time.time()
        }
        self._send_udp_message(ip, payload)

    def handle_ping(self, source_ip: str):
        """Handle incoming ping - respond with pong"""
        payload = {
            "type": "pong",
            "timestamp": time.time()
        }
        self._send_udp_message(source_ip, payload)

    def handle_pong(self, source_ip: str):
        """Handle incoming pong - update health status"""
        with self.health_lock:
            health = self.device_health[source_ip]
            health["last_pong"] = time.time()
            health["consecutive_failures"] = 0  # Reset failure counter

    def _auto_reconnect_monitor(self):
        """
        Background thread that automatically reconnects to saved devices.
        Implements intelligent retry logic with exponential backoff.
        """
        logger.info("Auto-reconnect monitor started")

        retry_delays = {}  # ip -> next_retry_time

        while self.running:
            try:
                time.sleep(5)  # Check every 5 seconds

                if not self.auto_reconnect_enabled or not self.saved_devices:
                    continue

                current_time = time.time()

                with self.devices_lock:
                    disconnected_devices = self.saved_devices - self.connected_devices

                for ip in disconnected_devices:
                    try:
                        # Check if it's time to retry this device
                        if ip in retry_delays and current_time < retry_delays[ip]:
                            continue

                        # Attempt reconnection
                        success, error = self.connect_device(ip)

                        if success:
                            logger.info(f"✓ Auto-reconnected to {ip}")
                            # Remove from retry tracking
                            retry_delays.pop(ip, None)

                            with self.health_lock:
                                # Reset health counters
                                self.device_health[ip]["consecutive_failures"] = 0
                                self.device_health[ip]["failures"] = 0

                        else:
                            with self.health_lock:
                                health = self.device_health[ip]
                                health["failures"] += 1

                            # Calculate next retry time with exponential backoff
                            failures = self.device_health[ip]["failures"]
                            delay = min(
                                self.reconnect_interval * (self.reconnect_backoff ** min(failures, 10)),
                                600  # Max 10 minutes
                            )
                            retry_delays[ip] = current_time + delay

                            logger.debug(
                                f"Failed to reconnect to {ip} (attempt #{failures}), "
                                f"next retry in {delay:.1f}s"
                            )

                    except Exception as e:
                        logger.error(f"Error in auto-reconnect for {ip}: {e}")

            except Exception as e:
                logger.error(f"Error in auto-reconnect monitor: {e}", exc_info=True)

    def _midnight_sync_monitor(self):
        """Monitor for date changes and trigger automatic sync"""
        logger.info("Midnight sync monitor started")

        while self.running:
            try:
                current_date = date.today()

                if self.last_sync_date != current_date:
                    logger.info(f"Date changed to {current_date}, triggering automatic database sync")
                    self.sync_database()
                    self.last_sync_date = current_date

                time.sleep(60)  # Check every minute

            except Exception as e:
                logger.error(f"Error in midnight sync monitor: {e}", exc_info=True)
                time.sleep(60)

    def _chunk_cleanup_monitor(self):
        """Clean up old incomplete message chunks"""
        logger.info("Chunk cleanup monitor started")

        while self.running:
            try:
                time.sleep(300)  # Clean up every 5 minutes
                current_time = time.time()

                with self.chunks_lock:
                    expired_chunks = [
                        chunk_id for chunk_id, data in self.message_chunks.items()
                        if current_time - data["timestamp"] > 300  # 5 minute timeout
                    ]

                    for chunk_id in expired_chunks:
                        logger.warning(f"Cleaning up expired chunks for message {chunk_id}")
                        del self.message_chunks[chunk_id]

            except Exception as e:
                logger.error(f"Error in chunk cleanup monitor: {e}", exc_info=True)

    # =========================================================================
    # Device Management
    # =========================================================================

    def validate_device_id(self, device_id: str, current_ip: str = None) -> Tuple[bool, Optional[str]]:
        """Validate that device_id is unique across the network"""
        with self.devices_lock:
            for ip, info in self.known_devices.items():
                if current_ip and ip == current_ip:
                    continue

                device_info = info.get("info", {})
                if device_info.get("device_id") == device_id:
                    return False, f"Device ID '{device_id}' already exists on device at {ip}"

        return True, None

    def validate_device_name(self, device_name: str, current_ip: str = None) -> Tuple[bool, Optional[str]]:
        """Validate that device_name is unique across the network"""
        with self.devices_lock:
            for ip, info in self.known_devices.items():
                if current_ip and ip == current_ip:
                    continue

                device_info = info.get("info", {})
                if device_info.get("device_name") == device_name:
                    return False, f"Device name '{device_name}' already exists on device at {ip}"

        return True, None

    def update_device(self, ip: str, device_info: dict):
        """Update information about a discovered device"""
        with self.devices_lock:
            self.known_devices[ip] = {
                "ip": ip,
                "last_seen": time.time(),
                "info": device_info
            }

    def connect_device(self, ip: str) -> Tuple[bool, Optional[str]]:
        """
        Connect to a device.
        Sends initial handshake and adds to connected devices set.
        """
        if not ip:
            logger.warning("connect_device called with None/empty IP, ignoring")
            return False, "No IP address provided"

        with self.devices_lock:
            # Add to known devices if not present
            if ip not in self.known_devices:
                logger.info(f"Adding undiscovered device {ip} to known_devices")
                self.known_devices[ip] = {
                    "ip": ip,
                    "last_seen": time.time(),
                    "info": {"device_id": None, "device_name": "Unknown"}
                }

            self.connected_devices.add(ip)

        # Send connection handshake
        try:
            payload = {
                "type": "connection_request",
                "timestamp": time.time()
            }
            self._send_udp_message(ip, payload)

            with self.health_lock:
                # Initialize health tracking
                self.device_health[ip]["last_ping"] = time.time()
                self.device_health[ip]["last_pong"] = time.time()

        except Exception as e:
            logger.error(f"Error sending connection handshake to {ip}: {e}")

        logger.info(f"✓ Connected to device: {ip}")

        # Process offline sync queue for this device
        self._process_offline_queue(ip)

        return True, None

    def disconnect_device(self, ip: str):
        """Disconnect from a device"""
        with self.devices_lock:
            self.connected_devices.discard(ip)
            logger.info(f"Disconnected from device: {ip}")

    def _process_offline_queue(self, device_ip: str):
        """
        Process offline sync queue when a device comes back online.
        Transfers all queued templates to the device.
        """
        with self.offline_queue_lock:
            queued_emp_ids = self.offline_sync_queue.get(device_ip, [])
            if not queued_emp_ids:
                return

            logger.info(f"🔄 Processing offline queue for {device_ip}: {len(queued_emp_ids)} templates pending")

        # Process queue in background thread to avoid blocking
        def process_queue():
            success_count = 0
            failed_ids = []

            for emp_id in queued_emp_ids:
                try:
                    result = self.transfer_user_template(emp_id, [device_ip])
                    if result.get(device_ip, False):
                        success_count += 1
                    else:
                        failed_ids.append(emp_id)
                    time.sleep(0.1)  # Small delay between transfers
                except Exception as e:
                    logger.error(f"Error processing offline queue for {emp_id}: {e}")
                    failed_ids.append(emp_id)

            # Update queue - keep only failed items
            with self.offline_queue_lock:
                if failed_ids:
                    self.offline_sync_queue[device_ip] = failed_ids
                    logger.warning(f"⚠️ Offline queue for {device_ip}: {len(failed_ids)} items still pending")
                else:
                    self.offline_sync_queue.pop(device_ip, None)
                    logger.info(f"✓ Offline queue cleared for {device_ip}: {success_count} templates synced")

        # Start background processing
        thread = threading.Thread(target=process_queue, daemon=True)
        thread.start()

    def get_connected_devices(self) -> List[Dict]:
        """Get list of connected devices with their info"""
        with self.devices_lock:
            return [
                {
                    "ip": ip,
                    "info": self.known_devices[ip]["info"],
                    "last_seen": self.known_devices[ip]["last_seen"]
                }
                for ip in self.connected_devices
                if ip in self.known_devices
            ]

    def get_online_devices(self, timeout_seconds: int = 30) -> List[Dict]:
        """Get list of online devices (seen recently)"""
        current_time = time.time()
        with self.devices_lock:
            return [
                {
                    "ip": info["ip"],
                    "info": info["info"],
                    "last_seen": info["last_seen"],
                    "connected": info["ip"] in self.connected_devices
                }
                for info in self.known_devices.values()
                if current_time - info["last_seen"] < timeout_seconds
            ]

    # =========================================================================
    # Data Synchronization
    # =========================================================================

    def transfer_user_template(self, emp_id: str, target_ips: List[str] = None) -> Dict[str, bool]:
        """
        Transfer user template (face encoding, image, fingerprint, database entry) to target devices.
        """
        if target_ips is None:
            with self.devices_lock:
                target_ips = list(self.connected_devices)

        if not target_ips:
            logger.warning(f"No target devices for template transfer of {emp_id}")
            return {}

        # Get user data from database
        user_data = self._get_user_data(emp_id)
        if not user_data:
            logger.error(f"User {emp_id} not found in database")
            return {ip: False for ip in target_ips}

        # Read face encoding file
        face_encoding_data = self._read_face_encoding(emp_id)
        if face_encoding_data:
            logger.info(f"[SYNC] Read face encoding for {emp_id}: {len(face_encoding_data)} bytes")
        else:
            logger.warning(f"[SYNC] No face encoding found for {emp_id}")

        # Read user image file and compress for network transfer
        user_image_data = self._read_user_image(emp_id)
        if user_image_data:
            original_size = len(user_image_data)
            user_image_data = self._compress_image(user_image_data, max_kb=80)
            logger.info(f"[SYNC] Read user image for {emp_id}: {original_size} -> {len(user_image_data)} bytes")
        else:
            logger.warning(f"[SYNC] No user image found for {emp_id}")

        # Read fingerprint template if exists
        fingerprint_data = self._read_fingerprint_template(emp_id)
        if fingerprint_data:
            logger.info(f"[SYNC] Read fingerprint for {emp_id}: {len(fingerprint_data)} bytes")

        # Look up template_id from fingerprint_map (no longer in users table)
        template_id = None
        try:
            conn = self.get_db_conn()
            row = conn.execute(
                "SELECT template_id FROM fingerprint_map WHERE emp_id = ?", (emp_id,)
            ).fetchone()
            if row:
                template_id = row[0]
        except Exception as e:
            logger.warning(f"[SYNC] Could not look up template_id for {emp_id}: {e}")

        # Prepare payload
        payload = {
            "type": "template_transfer",
            "emp_id": emp_id,
            "user_data": user_data,
            "template_id": template_id,
            "face_encoding": base64.b64encode(face_encoding_data).decode('utf-8') if face_encoding_data else None,
            "user_image": base64.b64encode(user_image_data).decode('utf-8') if user_image_data else None,
            "fingerprint": base64.b64encode(fingerprint_data).decode('utf-8') if fingerprint_data else None,
            "timestamp": time.time()
        }

        # Send to each target
        results = {}
        for target_ip in target_ips:
            # Check if device is currently connected
            with self.devices_lock:
                is_connected = target_ip in self.connected_devices

            if not is_connected:
                # Device is offline - add to queue for later sync
                with self.offline_queue_lock:
                    if emp_id not in self.offline_sync_queue[target_ip]:
                        self.offline_sync_queue[target_ip].append(emp_id)
                        logger.info(f"⏳ Device {target_ip} offline - queued {emp_id} for later sync")
                results[target_ip] = False
                continue

            success = self._send_udp_message(target_ip, payload)
            results[target_ip] = success

            status = "✓" if success else "✗"
            logger.info(f"{status} Template transfer for {emp_id} to {target_ip}")

        return results

    def incremental_sync(self, target_ips: List[str] = None) -> Dict[str, Dict]:
        """
        Perform incremental sync - only transfer new templates that don't exist on target device.
        Returns dict of {ip: {transferred: count, skipped: count, failed: count}}
        """
        if target_ips is None:
            with self.devices_lock:
                target_ips = list(self.connected_devices)

        if not target_ips:
            logger.warning("No target devices for incremental sync")
            return {}

        results = {}

        # Get all local users
        local_users = self._get_all_users()
        if not local_users:
            logger.warning("No users found for incremental sync")
            return {ip: {"transferred": 0, "skipped": 0, "failed": 0} for ip in target_ips}

        local_emp_ids = {user['emp_id'] for user in local_users}

        for target_ip in target_ips:
            logger.info(f"🔄 Starting incremental sync with {target_ip}")

            # Request existing user list from target device
            remote_emp_ids = self._request_user_list(target_ip)

            if remote_emp_ids is None:
                logger.error(f"Failed to get user list from {target_ip}")
                results[target_ip] = {"transferred": 0, "skipped": 0, "failed": 1}
                continue

            # Find users that need to be transferred (exist locally but not on remote)
            new_emp_ids = local_emp_ids - remote_emp_ids

            logger.info(f"📊 {target_ip}: Local={len(local_emp_ids)}, Remote={len(remote_emp_ids)}, New={len(new_emp_ids)}")

            transferred = 0
            failed = 0

            # Transfer only new templates
            for emp_id in new_emp_ids:
                try:
                    result = self.transfer_user_template(emp_id, [target_ip])
                    if result.get(target_ip, False):
                        transferred += 1
                    else:
                        failed += 1
                    time.sleep(0.05)  # Small delay to avoid overwhelming the network
                except Exception as e:
                    logger.error(f"Error transferring {emp_id} to {target_ip}: {e}")
                    failed += 1

            skipped = len(local_emp_ids) - len(new_emp_ids)
            results[target_ip] = {
                "transferred": transferred,
                "skipped": skipped,
                "failed": failed
            }

            logger.info(f"✓ Incremental sync {target_ip}: {transferred} transferred, {skipped} skipped, {failed} failed")

        return results

    def _request_user_list(self, target_ip: str) -> Optional[set]:
        """
        Request list of existing user IDs from target device.
        Returns set of emp_ids or None on failure.
        """
        try:
            # Send request for user list
            payload = {
                "type": "request_user_list",
                "timestamp": time.time()
            }

            # Create socket for receiving response
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(('', 0))  # Bind to any available port
            response_port = sock.getsockname()[1]

            payload["response_port"] = response_port

            # Send request
            self._send_udp_message(target_ip, payload)

            # Wait for response (5 second timeout)
            sock.settimeout(5.0)
            data, addr = sock.recvfrom(65535)
            sock.close()

            response = json.loads(data.decode('utf-8'))

            if response.get("type") == "user_list_response":
                emp_ids = set(response.get("emp_ids", []))
                logger.info(f"Received user list from {target_ip}: {len(emp_ids)} users")
                return emp_ids
            else:
                logger.warning(f"Unexpected response type from {target_ip}: {response.get('type')}")
                return None

        except socket.timeout:
            logger.warning(f"Timeout waiting for user list from {target_ip}")
            return None
        except Exception as e:
            logger.error(f"Error requesting user list from {target_ip}: {e}")
            return None

    def sync_database(self, target_ips: List[str] = None) -> Dict[str, bool]:
        """Synchronize entire database (users and logs) to target devices"""
        if target_ips is None:
            with self.devices_lock:
                target_ips = list(self.connected_devices)

        if not target_ips:
            logger.warning("No target devices for database sync")
            return {}

        with self.sync_lock:
            if self.sync_in_progress:
                logger.warning("Database sync already in progress")
                return {ip: False for ip in target_ips}

            self.sync_in_progress = True

        try:
            # Get all users
            users = self._get_all_users()

            # Get all logs from today
            logs = self._get_logs_for_sync()

            # Prepare payload
            payload = {
                "type": "database_sync",
                "users": users,
                "logs": logs,
                "timestamp": time.time()
            }

            # Send to each target
            results = {}
            for target_ip in target_ips:
                success = self._send_udp_message(target_ip, payload)
                results[target_ip] = success
                status = "✓" if success else "✗"
                logger.info(f"{status} Database sync to {target_ip}")

            return results

        finally:
            with self.sync_lock:
                self.sync_in_progress = False

    def sync_mesh_connections(self, mesh_ips: List[str]) -> Dict[str, bool]:
        """
        Broadcast mesh network topology to all devices.
        Ensures all devices know about each other and are connected.
        """
        results = {}

        # First, connect to all devices locally
        for ip in mesh_ips:
            success, error = self.connect_device(ip)
            if not success:
                logger.warning(f"Failed to connect to {ip}: {error}")

        # Prepare mesh sync message
        payload = {
            "type": "mesh_sync",
            "mesh_devices": mesh_ips,
            "timestamp": time.time()
        }

        # Broadcast to all devices in mesh
        for target_ip in mesh_ips:
            success = self._send_udp_message(target_ip, payload)
            results[target_ip] = success

            status = "✓" if success else "✗"
            logger.info(f"{status} Mesh sync to {target_ip}")

        return results

    # =========================================================================
    # Message Handlers
    # =========================================================================

    def handle_template_transfer(self, payload: dict) -> bool:
        """Handle incoming template transfer message"""
        try:
            emp_id = payload.get("emp_id")
            user_data = payload.get("user_data")
            face_encoding_b64 = payload.get("face_encoding")
            user_image_b64 = payload.get("user_image")
            fingerprint_b64 = payload.get("fingerprint")

            if not emp_id or not user_data:
                logger.error("Invalid template transfer payload")
                return False

            # Save files and track paths
            encoding_path = None
            image_path = None

            # Save face encoding file
            if face_encoding_b64:
                face_encoding_data = base64.b64decode(face_encoding_b64)
                self._write_face_encoding(emp_id, face_encoding_data)
                # Set the local path for this device
                encoding_path = f"face_encodings/{emp_id}.dat"
                logger.info(f"[SYNC] Saved face encoding to {encoding_path}")

            # Save user image file
            if user_image_b64:
                user_image_data = base64.b64decode(user_image_b64)
                self._write_user_image(emp_id, user_image_data)
                # Set the local path for this device
                image_path = f"users_img/{emp_id}.jpg"
                logger.info(f"[SYNC] Saved user image to {image_path}")

            # Save fingerprint template file and update fingerprint tables/sensor
            template_id = payload.get("template_id")
            if fingerprint_b64:
                fingerprint_data = base64.b64decode(fingerprint_b64)
                self._write_fingerprint_template(emp_id, fingerprint_data)
                logger.info(f"[SYNC] Saved fingerprint to fingerprint_encodings/{emp_id}.dat")

                # Update fingerprint tables in database
                if template_id:
                    try:
                        conn = self.get_db_conn()
                        # Save to fingerprints table
                        conn.execute(
                            "INSERT OR REPLACE INTO fingerprints (id, username, template) VALUES (?, ?, ?)",
                            (int(template_id), user_data.get("name", ""), fingerprint_data)
                        )
                        # Update fingerprint_map
                        conn.execute(
                            "INSERT OR REPLACE INTO fingerprint_map(emp_id, template_id, name) VALUES (?,?,?)",
                            (emp_id, int(template_id), user_data.get("name"))
                        )
                        # Update user_finger_map
                        conn.execute(
                            "INSERT OR REPLACE INTO user_finger_map(emp_id, template_id) VALUES (?,?)",
                            (emp_id, int(template_id))
                        )
                        conn.commit()
                        logger.info(f"[SYNC] Updated fingerprint tables for emp_id={emp_id}, template_id={template_id}")
                    except Exception as e:
                        logger.error(f"[SYNC] Error updating fingerprint tables: {e}")

            # CRITICAL: Update user_data with the LOCAL file paths
            if encoding_path:
                user_data["encoding_path"] = encoding_path
            if image_path:
                user_data["image_path"] = image_path

            # Upsert user in database with updated paths
            self._upsert_user(user_data)

            # Trigger callback to reload face recognizer if face encoding was received
            if self.on_template_received and encoding_path:
                try:
                    self.on_template_received(emp_id)
                    logger.info(f"[SYNC] Triggered recognizer reload for {emp_id}")
                except Exception as e:
                    logger.error(f"[SYNC] Error in template received callback: {e}")

            logger.info(f"✓ Template transfer completed for {emp_id} (encoding_path={encoding_path}, image_path={image_path})")
            return True

        except Exception as e:
            logger.error(f"Error handling template transfer: {e}", exc_info=True)
            return False

    def handle_database_sync(self, payload: dict) -> bool:
        """Handle incoming database sync message"""
        try:
            users = payload.get("users", [])
            logs = payload.get("logs", [])

            # Sync users (with conflict resolution)
            for user in users:
                self._upsert_user(user)

            # Sync logs (avoiding duplicates)
            for log in logs:
                self._insert_log_if_not_exists(log)

            logger.info(f"✓ Database sync completed: {len(users)} users, {len(logs)} logs")
            return True

        except Exception as e:
            logger.error(f"Error handling database sync: {e}", exc_info=True)
            return False

    def handle_mesh_sync(self, payload: dict) -> bool:
        """Handle incoming mesh sync message - connect to all devices in mesh"""
        try:
            mesh_devices = payload.get("mesh_devices", [])

            if not mesh_devices:
                logger.warning("Received empty mesh_devices list")
                return False

            # Connect to all devices in the mesh
            connected_count = 0
            for ip in mesh_devices:
                success, error = self.connect_device(ip)
                if success:
                    connected_count += 1
                else:
                    logger.warning(f"Failed to connect to mesh device {ip}: {error}")

            logger.info(f"✓ Mesh sync: connected to {connected_count}/{len(mesh_devices)} devices")
            return True

        except Exception as e:
            logger.error(f"Error handling mesh sync: {e}", exc_info=True)
            return False

    def handle_chunked_message(self, payload: dict) -> Optional[dict]:
        """Handle incoming chunked message and reassemble when complete"""
        try:
            chunk_id = payload.get("chunk_id")
            chunk_num = payload.get("chunk_num")
            total_chunks = payload.get("total_chunks")
            chunk_data = payload.get("data")

            if chunk_id is None or chunk_num is None or total_chunks is None or chunk_data is None:
                logger.error("Invalid chunked message payload")
                return None

            with self.chunks_lock:
                # Initialize chunk storage for this message if needed
                if chunk_id not in self.message_chunks:
                    self.message_chunks[chunk_id] = {
                        "chunks": {},
                        "total_chunks": total_chunks,
                        "timestamp": time.time()
                    }

                # Store this chunk
                chunk_bytes = base64.b64decode(chunk_data)
                self.message_chunks[chunk_id]["chunks"][chunk_num] = chunk_bytes

                # Check if we have all chunks
                if len(self.message_chunks[chunk_id]["chunks"]) == total_chunks:
                    # Reassemble message
                    chunks = self.message_chunks[chunk_id]["chunks"]
                    complete_message = b"".join([chunks[i] for i in range(total_chunks)])

                    # Clean up
                    del self.message_chunks[chunk_id]

                    # Parse and return the complete message
                    try:
                        return json.loads(complete_message.decode('utf-8'))
                    except Exception as e:
                        logger.error(f"Error parsing reassembled message: {e}")
                        return None

            return None

        except Exception as e:
            logger.error(f"Error handling chunked message: {e}", exc_info=True)
            return None

    # =========================================================================
    # Database Operations
    # =========================================================================

    def _get_user_data(self, emp_id: str) -> Optional[Dict]:
        """Get user data from database"""
        try:
            conn = self.get_db_conn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT emp_id, name, role, birthdate,
                       encoding_path, image_path, created_at, updated_at
                FROM users WHERE emp_id = ?
            """, (emp_id,))
            row = cursor.fetchone()

            if not row:
                return None

            return {
                "emp_id": row[0],
                "name": row[1],
                "role": row[2],
                "birthdate": row[3],
                "encoding_path": row[4],
                "image_path": row[5],
                "created_at": row[6],
                "updated_at": row[7]
            }
        except Exception as e:
            logger.error(f"Error getting user data for {emp_id}: {e}")
            return None

    def _get_all_users(self) -> List[Dict]:
        """Get all users from database"""
        try:
            conn = self.get_db_conn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT emp_id, name, role, birthdate,
                       encoding_path, image_path, created_at, updated_at
                FROM users
            """)
            rows = cursor.fetchall()

            return [
                {
                    "emp_id": row[0],
                    "name": row[1],
                    "role": row[2],
                    "birthdate": row[3],
                    "encoding_path": row[4],
                    "image_path": row[5],
                    "created_at": row[6],
                    "updated_at": row[7]
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []

    def _get_logs_for_sync(self, days: int = 1) -> List[Dict]:
        """Get logs for synchronization"""
        try:
            conn = self.get_db_conn()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, emp_id, name, device_id, mode, ts, success
                FROM logs
                WHERE ts >= datetime('now', '-' || ? || ' days')
                ORDER BY ts DESC
            """, (days,))
            rows = cursor.fetchall()

            return [
                {
                    "id": row[0],
                    "emp_id": row[1],
                    "name": row[2],
                    "device_id": row[3],
                    "mode": row[4],
                    "ts": row[5],
                    "success": row[6]
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error getting logs for sync: {e}")
            return []

    def _upsert_user(self, user_data: Dict):
        """Insert or update user in database"""
        try:
            conn = self.get_db_conn()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO users (emp_id, name, role, birthdate,
                                  encoding_path, image_path, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(emp_id) DO UPDATE SET
                    name = excluded.name,
                    role = excluded.role,
                    birthdate = excluded.birthdate,
                    encoding_path = excluded.encoding_path,
                    image_path = excluded.image_path,
                    updated_at = excluded.updated_at
            """, (
                user_data["emp_id"],
                user_data.get("name"),
                user_data.get("role", "User"),
                user_data.get("birthdate"),
                user_data.get("encoding_path"),
                user_data.get("image_path"),
                user_data.get("created_at"),
                user_data.get("updated_at")
            ))

            conn.commit()
            logger.debug(f"Upserted user: {user_data['emp_id']}")

        except Exception as e:
            logger.error(f"Error upserting user {user_data.get('emp_id')}: {e}")

    def _insert_log_if_not_exists(self, log_data: Dict):
        """Insert log entry if it doesn't already exist"""
        try:
            conn = self.get_db_conn()
            cursor = conn.cursor()

            # Check if log already exists
            cursor.execute("""
                SELECT COUNT(*) FROM logs
                WHERE emp_id = ? AND device_id = ? AND ts = ?
            """, (log_data["emp_id"], log_data["device_id"], log_data["ts"]))

            count = cursor.fetchone()[0]

            if count == 0:
                # Insert log
                cursor.execute("""
                    INSERT INTO logs (emp_id, name, device_id, mode, ts, success)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    log_data["emp_id"],
                    log_data["name"],
                    log_data["device_id"],
                    log_data["mode"],
                    log_data["ts"],
                    log_data.get("success", 1)
                ))

                conn.commit()
                logger.debug(f"Inserted log: {log_data['emp_id']} at {log_data['ts']}")

        except Exception as e:
            logger.error(f"Error inserting log: {e}")

    # =========================================================================
    # File Operations
    # =========================================================================

    @staticmethod
    def _compress_image(image_bytes: bytes, max_kb: int = 80) -> bytes:
        """Compress a JPEG image for network transfer, targeting max_kb size."""
        if not image_bytes:
            return image_bytes
        try:
            import cv2
            import numpy as np
            max_bytes = max_kb * 1024
            if len(image_bytes) <= max_bytes:
                return image_bytes

            nparr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if img is None:
                return image_bytes

            h, w = img.shape[:2]
            # Resize if too large
            max_dim = 320
            if max(h, w) > max_dim:
                scale = max_dim / max(h, w)
                img = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)

            for quality in (70, 55, 40, 25, 15):
                ok, buf = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, quality])
                if ok and len(buf) <= max_bytes:
                    logger.info(f"Image compressed: {len(image_bytes)} -> {len(buf)} bytes (q={quality})")
                    return buf.tobytes()

            ok, buf = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 10])
            return buf.tobytes() if ok else image_bytes
        except ImportError:
            logger.warning("cv2 not available for image compression, sending raw")
            return image_bytes
        except Exception as e:
            logger.error(f"Image compression error: {e}")
            return image_bytes

    def _read_face_encoding(self, emp_id: str) -> Optional[bytes]:
        """Read face encoding file"""
        file_path = os.path.join(self.face_encodings_dir, f"{emp_id}.dat")
        try:
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    return f.read()
        except Exception as e:
            logger.error(f"Error reading face encoding for {emp_id}: {e}")
        return None

    def _write_face_encoding(self, emp_id: str, data: bytes):
        """Write face encoding file"""
        file_path = os.path.join(self.face_encodings_dir, f"{emp_id}.dat")
        try:
            with open(file_path, "wb") as f:
                f.write(data)
            logger.info(f"Wrote face encoding: {file_path} ({len(data)} bytes)")
        except Exception as e:
            logger.error(f"Error writing face encoding for {emp_id}: {e}")

    def _read_user_image(self, emp_id: str) -> Optional[bytes]:
        """Read user image file"""
        file_path = os.path.join(self.users_img_dir, f"{emp_id}.jpg")
        try:
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    return f.read()
        except Exception as e:
            logger.error(f"Error reading user image for {emp_id}: {e}")
        return None

    def _write_user_image(self, emp_id: str, data: bytes):
        """Write user image file"""
        file_path = os.path.join(self.users_img_dir, f"{emp_id}.jpg")
        try:
            with open(file_path, "wb") as f:
                f.write(data)
            logger.info(f"Wrote user image: {file_path} ({len(data)} bytes)")
        except Exception as e:
            logger.error(f"Error writing user image for {emp_id}: {e}")

    def _read_fingerprint_template(self, emp_id: str) -> Optional[bytes]:
        """Read fingerprint template file"""
        file_path = os.path.join(self.fingerprint_dir, f"{emp_id}.dat")
        try:
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    return f.read()
        except Exception as e:
            logger.error(f"Error reading fingerprint for {emp_id}: {e}")
        return None

    def _write_fingerprint_template(self, emp_id: str, data: bytes):
        """Write fingerprint template file"""
        file_path = os.path.join(self.fingerprint_dir, f"{emp_id}.dat")
        try:
            with open(file_path, "wb") as f:
                f.write(data)
            logger.info(f"Wrote fingerprint: {file_path} ({len(data)} bytes)")
        except Exception as e:
            logger.error(f"Error writing fingerprint for {emp_id}: {e}")

    # =========================================================================
    # Network Communication
    # =========================================================================

    def _send_udp_message(self, target_ip: str, payload: dict) -> bool:
        """
        Send UDP message to target IP with automatic chunking for large payloads.

        Payloads under 60KB are sent as a single UDP datagram.
        Larger payloads are split into chunks, each under 60KB on the wire,
        and reassembled by the receiver's _reassemble_chunked_message().
        """
        if not target_ip:
            logger.warning(f"Skipping UDP send — target_ip is None/empty (payload type: {payload.get('type', '?')})")
            return False
        try:
            message = json.dumps(payload, separators=(',', ':')).encode('utf-8')
            message_size = len(message)

            MAX_SINGLE_PACKET = 60000  # safe limit under 65535 UDP max

            # If message fits in single packet, send directly
            if message_size <= MAX_SINGLE_PACKET:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(5)
                sock.sendto(message, (target_ip, self.udp_port))
                sock.close()
                return True

            # Message too large - send in chunks
            # Raw chunk size must be small enough that after base64 + JSON wrapper
            # the on-wire datagram stays under MAX_SINGLE_PACKET.
            # base64 expands by 4/3, and wrapper JSON is ~250 bytes.
            CHUNK_RAW_SIZE = 44000
            import uuid as _uuid
            chunk_id = f"{int(time.time()*1000)}_{_uuid.uuid4().hex[:8]}"
            total_chunks = (message_size + CHUNK_RAW_SIZE - 1) // CHUNK_RAW_SIZE

            logger.info(f"Sending large message to {target_ip}: {message_size} bytes in {total_chunks} chunks (id={chunk_id})")

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5)

            for chunk_num in range(total_chunks):
                start_idx = chunk_num * CHUNK_RAW_SIZE
                end_idx = min(start_idx + CHUNK_RAW_SIZE, message_size)
                chunk_data = message[start_idx:end_idx]

                chunk_payload = {
                    "type": "chunked_message",
                    "chunk_id": chunk_id,
                    "chunk_num": chunk_num,
                    "total_chunks": total_chunks,
                    "data": base64.b64encode(chunk_data).decode('utf-8')
                }

                chunk_message = json.dumps(chunk_payload, separators=(',', ':')).encode('utf-8')
                sock.sendto(chunk_message, (target_ip, self.udp_port))
                time.sleep(0.015)  # Delay between chunks to reduce packet loss

            sock.close()
            logger.info(f"✓ Sent {total_chunks} chunks to {target_ip} (id={chunk_id})")
            return True

        except Exception as e:
            logger.error(f"Error sending UDP message to {target_ip}: {e}")
            return False

    def shutdown(self):
        """Gracefully shutdown the manager"""
        logger.info("Shutting down DeviceSyncManager...")
        self.running = False

        # Wait for threads to finish
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=2)

        logger.info("DeviceSyncManager shutdown complete")
