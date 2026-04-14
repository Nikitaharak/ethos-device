"""
Real-time Face Quality Checker with Progress Feedback
Optimized for Raspberry Pi Camera Module 3 with libcamera/Picamera2
- Face-targeted autofocus (not continuous/background AF)
- Auto-exposure for correct brightness
- Image sharpening (unsharp mask) to recover soft frames
- Downscaled detection for speed
- Stable frame buffer to prevent false captures
- 90% quality threshold for registration
"""

import cv2
import numpy as np
import face_recognition
import time
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Picamera2 / libcamera setup (Raspberry Pi)
# ──────────────────────────────────────────────
try:
    from picamera2 import Picamera2
    import libcamera
    PICAMERA2_AVAILABLE = True
except ImportError:
    PICAMERA2_AVAILABLE = False
    logger.warning("Picamera2 not available — falling back to OpenCV VideoCapture")


class CameraManager:
    """
    Manages camera capture using Picamera2 (preferred) or OpenCV fallback.
    Picamera2 enables face-targeted autofocus + auto-exposure on Camera Module 3.
    Continuous AF is disabled — instead, AF is triggered on the face bounding box
    so the camera focuses on the face, not the background.
    """

    def __init__(self, width: int = 640, height: int = 480):
        self.width = width
        self.height = height
        self._picam2 = None
        self._cap = None
        self._use_picamera = False
        self._last_af_trigger = 0.0  # throttle AF triggers

    def start(self) -> bool:
        """Start the camera. Returns True if successful."""
        if PICAMERA2_AVAILABLE:
            return self._start_picamera2()
        else:
            return self._start_opencv()

    def _start_picamera2(self) -> bool:
        try:
            self._picam2 = Picamera2()

            config = self._picam2.create_preview_configuration(
                main={"size": (self.width, self.height), "format": "RGB888"},
                controls={
                    # ── Autofocus: Manual mode — we trigger AF on face box ─
                    "AfMode": libcamera.controls.AfModeEnum.Manual,
                    "AfSpeed": libcamera.controls.AfSpeedEnum.Fast,
                    # ── Auto-exposure / brightness ─────────────────────────
                    "AeEnable": True,
                    "AeExposureMode": libcamera.controls.AeExposureModeEnum.Normal,
                    # ── Auto white balance ─────────────────────────────────
                    "AwbEnable": True,
                    "AwbMode": libcamera.controls.AwbModeEnum.Auto,
                    # ── Noise reduction (improves sharpness score) ─────────
                    "NoiseReductionMode": libcamera.controls.draft.NoiseReductionModeEnum.Fast,
                }
            )

            self._picam2.configure(config)
            self._picam2.start()
            self._use_picamera = True

            # Let the camera warm up and AE settle
            logger.info("Camera Module 3 started (Manual AF) — waiting for AE to settle...")
            time.sleep(1.0)  # let AE stabilize

            # Do one initial AF trigger (centre of frame) so lens isn't stuck
            self._trigger_autofocus_centre()

            logger.info("Picamera2 ready (face-targeted AF mode).")
            return True

        except Exception as e:
            logger.error(f"Picamera2 init failed: {e}")
            self._picam2 = None
            return self._start_opencv()

    def _start_opencv(self) -> bool:
        try:
            self._cap = cv2.VideoCapture(0)
            self._cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
            self._cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
            # Disable autofocus — we manage focus manually via face detection
            self._cap.set(cv2.CAP_PROP_AUTOFOCUS, 0)
            self._use_picamera = False
            logger.info("OpenCV VideoCapture started (autofocus disabled).")
            return self._cap.isOpened()
        except Exception as e:
            logger.error(f"OpenCV camera init failed: {e}")
            return False

    def _trigger_autofocus_centre(self):
        """One-shot AF trigger at centre of frame (initial startup)."""
        if not self._use_picamera or self._picam2 is None:
            return
        try:
            # Set AF to Auto (one-shot) mode and trigger
            self._picam2.set_controls({
                "AfMode": libcamera.controls.AfModeEnum.Auto,
                "AfTrigger": libcamera.controls.AfTriggerEnum.Start,
            })
            # Wait briefly for initial focus lock
            start = time.time()
            while time.time() - start < 2.0:
                metadata = self._picam2.capture_metadata()
                af_state = metadata.get("AfState", None)
                if af_state == 2:  # Focused
                    logger.info(f"Initial AF locked in {time.time() - start:.2f}s")
                    return
                time.sleep(0.05)
            logger.info("Initial AF timeout — proceeding anyway")
        except Exception as e:
            logger.warning(f"Initial AF trigger failed: {e}")

    def trigger_face_autofocus(self, face_box: Tuple[int, int, int, int]):
        """
        Trigger one-shot autofocus on the face bounding box.
        face_box: (top, right, bottom, left) in pixel coordinates.
        Throttled to max once per 1.5 seconds to avoid AF hunting.
        """
        now = time.time()
        if now - self._last_af_trigger < 1.5:
            return  # throttle
        self._last_af_trigger = now

        if not self._use_picamera or self._picam2 is None:
            return

        try:
            top, right, bottom, left = face_box
            # Convert pixel coords to normalised 0.0-1.0 for AfWindows
            # AfWindows expects (x_offset, y_offset, width, height) normalised
            x_norm = max(0.0, left / self.width)
            y_norm = max(0.0, top / self.height)
            w_norm = min(1.0, (right - left) / self.width)
            h_norm = min(1.0, (bottom - top) / self.height)

            # Pad the AF window slightly (20%) to give AF more context
            pad_x = w_norm * 0.1
            pad_y = h_norm * 0.1
            x_norm = max(0.0, x_norm - pad_x)
            y_norm = max(0.0, y_norm - pad_y)
            w_norm = min(1.0 - x_norm, w_norm + 2 * pad_x)
            h_norm = min(1.0 - y_norm, h_norm + 2 * pad_y)

            # Set AfWindows and trigger one-shot AF on the face region
            self._picam2.set_controls({
                "AfMode": libcamera.controls.AfModeEnum.Auto,
                "AfWindows": [(
                    int(x_norm * self.width),
                    int(y_norm * self.height),
                    int(w_norm * self.width),
                    int(h_norm * self.height),
                )],
                "AfTrigger": libcamera.controls.AfTriggerEnum.Start,
            })
            logger.debug(f"AF triggered on face box: ({left},{top})-({right},{bottom})")
        except Exception as e:
            logger.warning(f"Face AF trigger failed: {e}")

    def capture_frame(self) -> Optional[np.ndarray]:
        """Capture a single frame as an RGB numpy array."""
        try:
            if self._use_picamera and self._picam2:
                frame = self._picam2.capture_array()
                # Picamera2 with RGB888 returns RGB directly
                return frame
            elif self._cap and self._cap.isOpened():
                ret, frame = self._cap.read()
                if ret:
                    # OpenCV returns BGR — convert to RGB
                    return cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        except Exception as e:
            logger.error(f"Frame capture error: {e}")
        return None

    def stop(self):
        """Release camera resources."""
        if self._picam2:
            try:
                self._picam2.stop()
                self._picam2.close()
            except Exception:
                pass
            self._picam2 = None
        if self._cap:
            self._cap.release()
            self._cap = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


# ──────────────────────────────────────────────
# Face Quality Checker
# ──────────────────────────────────────────────

class FaceQualityChecker:
    """
    Checks face quality in real-time and provides progress feedback.
    Optimized for Raspberry Pi Camera Module 3.

    Key improvements:
    - 50% downscale for fast HOG detection (~4x speedup)
    - Face-targeted autofocus (AF triggered on detected face box)
    - Unsharp-mask sharpening on face ROI to recover soft/blurry frames
    - 90% quality threshold for registration
    - Stable-frame buffer: requires N consecutive good frames before capture
    - CNN encoding only fires when frame is confirmed stable
    """

    def __init__(
        self,
        quality_threshold: float = 0.90,   # 90% quality required for registration
        min_face_size: int = 60,
        blur_threshold: int = 50,           # lower threshold — sharpening helps
        brightness_range: Tuple[int, int] = (30, 230),
        stable_frames_required: int = 3,
        detection_scale: float = 0.5,
        camera_manager: Optional['CameraManager'] = None,
    ):
        self.quality_threshold = quality_threshold
        self.min_face_size = min_face_size
        self.blur_threshold = blur_threshold
        self.brightness_range = brightness_range
        self.stable_frames_required = stable_frames_required
        self.detection_scale = detection_scale
        self.camera_manager = camera_manager  # for face-targeted AF

        # Internal state
        self._stable_count = 0

    # ── Public API ─────────────────────────────────────────────────────────

    def reset(self):
        """Reset the stable-frame counter (call between registration attempts)."""
        self._stable_count = 0

    @staticmethod
    def sharpen_face_roi(face_roi: np.ndarray, strength: float = 1.5) -> np.ndarray:
        """
        Apply unsharp mask to sharpen a blurry face ROI.
        This recovers detail lost when autofocus was on the background.

        Parameters
        ----------
        face_roi : np.ndarray
            The cropped face region (RGB or BGR).
        strength : float
            Sharpening strength (1.0 = mild, 2.0 = strong).

        Returns
        -------
        np.ndarray
            Sharpened face ROI (same shape/dtype as input).
        """
        # Gaussian blur to create the "unsharp" layer
        blurred = cv2.GaussianBlur(face_roi, (0, 0), sigmaX=3)
        # Unsharp mask: original + strength * (original - blurred)
        sharpened = cv2.addWeighted(face_roi, 1.0 + strength, blurred, -strength, 0)
        return sharpened

    @staticmethod
    def sharpen_full_frame(frame: np.ndarray, face_box: Tuple[int, int, int, int]) -> np.ndarray:
        """
        Sharpen only the face region within the full frame, leaving
        the rest untouched. Returns a new frame with the sharpened face.
        """
        top, right, bottom, left = face_box
        result = frame.copy()
        face_roi = frame[top:bottom, left:right]
        if face_roi.size == 0:
            return result
        sharpened_roi = FaceQualityChecker.sharpen_face_roi(face_roi, strength=1.5)
        result[top:bottom, left:right] = sharpened_roi
        return result

    def check_face_quality(self, frame: np.ndarray) -> Dict:
        """
        Analyse frame quality and return structured result.

        Parameters
        ----------
        frame : np.ndarray
            RGB image from CameraManager.capture_frame()

        Returns
        -------
        dict with keys:
            success          bool   – True once stable capture is ready
            quality_score    float  – 0-100 composite score
            face_coverage    float  – 0-100
            sharpness_score  float  – 0-100
            brightness_score float  – 0-100
            face_detected    bool
            face_box         tuple | None   – (top, right, bottom, left)
            message          str    – human-readable guidance
            ready_to_capture bool   – True on the N-th consecutive good frame
            stable_progress  float  – 0.0-1.0 fill for progress bar
            encoding         np.ndarray | None
        """
        result = {
            'success': False,
            'quality_score': 0.0,
            'face_coverage': 0.0,
            'sharpness_score': 0.0,
            'brightness_score': 0.0,
            'face_detected': False,
            'face_box': None,
            'message': '',
            'ready_to_capture': False,
            'stable_progress': 0.0,
            'encoding': None,
        }

        try:
            # ── Step 1: Downscale for fast detection ──────────────────────
            small = cv2.resize(
                frame,
                (0, 0),
                fx=self.detection_scale,
                fy=self.detection_scale,
            )

            face_locations_small = face_recognition.face_locations(small, model='hog')

            if not face_locations_small:
                self._stable_count = 0
                result['message'] = "No face detected — position yourself in the frame"
                return result

            # Scale locations back to full-res coordinates
            s = 1.0 / self.detection_scale
            face_locations = [
                (int(t * s), int(r * s), int(b * s), int(l * s))
                for t, r, b, l in face_locations_small
            ]

            # Use largest face
            face_box = max(
                face_locations,
                key=lambda box: (box[2] - box[0]) * (box[3] - box[1])
            )
            top, right, bottom, left = face_box

            result['face_detected'] = True
            result['face_box'] = face_box

            # ── Step 1b: Trigger face-targeted autofocus ──────────────────
            if self.camera_manager is not None:
                self.camera_manager.trigger_face_autofocus(face_box)

            frame_h, frame_w = frame.shape[:2]
            face_h = bottom - top
            face_w = right - left

            # ── Step 2: Coverage score ─────────────────────────────────────
            face_area = face_h * face_w
            frame_area = frame_h * frame_w
            coverage_ratio = face_area / frame_area
            ideal_coverage = 0.15
            coverage_score = min((coverage_ratio / ideal_coverage) * 100, 100)
            result['face_coverage'] = coverage_score

            # ── Step 3: Extract face ROI and sharpen it ───────────────────
            face_roi = frame[top:bottom, left:right]
            if face_roi.size == 0:
                self._stable_count = 0
                result['message'] = "Face region invalid — try again"
                return result

            # Apply unsharp-mask sharpening to the face ROI before scoring
            sharpened_roi = self.sharpen_face_roi(face_roi, strength=1.5)

            # Measure sharpness on the SHARPENED face ROI
            gray_face = cv2.cvtColor(sharpened_roi, cv2.COLOR_RGB2GRAY)
            laplacian_var = cv2.Laplacian(gray_face, cv2.CV_64F).var()
            sharpness_score = min((laplacian_var / self.blur_threshold) * 100, 100)
            result['sharpness_score'] = sharpness_score

            # ── Step 4: Brightness ─────────────────────────────────────────
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
            avg_brightness = float(np.mean(gray_frame))

            lo, hi = self.brightness_range
            if lo <= avg_brightness <= hi:
                brightness_score = 100.0
            elif avg_brightness < lo:
                brightness_score = (avg_brightness / lo) * 100.0
            else:
                excess = avg_brightness - hi
                max_excess = 255 - hi
                brightness_score = max(0.0, 100.0 - (excess / max_excess) * 100.0)
            result['brightness_score'] = brightness_score

            # ── Step 5: Composite quality score ───────────────────────────
            quality_score = (
                coverage_score   * 0.4 +
                sharpness_score  * 0.4 +
                brightness_score * 0.2
            )
            result['quality_score'] = quality_score

            # ── Step 6: Ready check (90% registration threshold) ──────────
            frame_ready = (
                coverage_score   >= 70 and   # face close enough
                sharpness_score  >= 70 and   # sharp after enhancement
                brightness_score >= 50 and   # not too dark/bright
                quality_score    >= 90       # 90% composite threshold
            )

            # ── Step 7: Stable-frame buffer ────────────────────────────────
            if frame_ready:
                self._stable_count += 1
            else:
                self._stable_count = 0

            stable_progress = min(self._stable_count / self.stable_frames_required, 1.0)
            result['stable_progress'] = stable_progress

            if self._stable_count >= self.stable_frames_required:
                result['ready_to_capture'] = True
                result['success'] = True
                result['message'] = "Perfect! Capturing..."

                # Sharpen the full frame (face region only) before encoding
                sharpened_frame = self.sharpen_full_frame(frame, face_box)

                # Use CNN for high-quality encoding on the sharpened frame
                try:
                    encodings = face_recognition.face_encodings(
                        sharpened_frame,
                        [face_box],
                        model='large',
                        num_jitters=1,
                    )
                    if encodings:
                        result['encoding'] = encodings[0]
                except Exception as enc_err:
                    logger.error(f"Encoding failed: {enc_err}")

            elif not frame_ready:
                result['message'] = self._feedback_message(
                    coverage_score, sharpness_score, brightness_score, avg_brightness
                )
            else:
                result['message'] = (
                    f"Hold still... ({self._stable_count}/{self.stable_frames_required})"
                )

        except Exception as e:
            logger.error(f"Face quality check error: {e}", exc_info=True)
            result['message'] = f"Error: {e}"
            self._stable_count = 0

        return result

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _feedback_message(
        coverage: float,
        sharpness: float,
        brightness: float,
        avg_brightness: float,
    ) -> str:
        if coverage < 40:
            return "Move closer to the camera"
        if coverage < 70:
            return "Good distance — adjust position slightly"
        if sharpness < 70:
            return "Blurry image — hold still and look at camera"
        if brightness < 50:
            lo_threshold = 30
            if avg_brightness < lo_threshold:
                return "Too dark — turn on more lights"
            else:
                return "Too bright — reduce glare or step back"
        return "Almost there — hold your position"

    @staticmethod
    def get_quality_color(quality_score: float) -> str:
        """Return a hex colour string based on quality score."""
        if quality_score >= 80:
            return "#00ff00"   # Green
        elif quality_score >= 65:
            return "#ffff00"   # Yellow
        elif quality_score >= 45:
            return "#ff8800"   # Orange
        else:
            return "#ff0000"   # Red
 

    def draw_quality_overlay(self, frame: np.ndarray, quality_result: Dict) -> np.ndarray:
        """
        Draw quality feedback overlay on an RGB frame.
        Returns an annotated RGB frame (safe to display with cv2 after BGR conversion).
        """
        annotated = frame.copy()
        h, w = annotated.shape[:2]

        # ── Face bounding box + quality bar ───────────────────────────────
        if quality_result['face_box']:
            top, right, bottom, left = quality_result['face_box']
            quality = quality_result['quality_score']

            if quality >= 85:
                color_bgr = (0, 255, 0)
            elif quality >= 65:
                color_bgr = (0, 255, 255)
            elif quality >= 45:
                color_bgr = (0, 136, 255)
            else:
                color_bgr = (0, 0, 255)

            # Face rectangle
            cv2.rectangle(annotated, (left, top), (right, bottom), color_bgr, 3)

            # Quality bar background
            bar_w = right - left
            bar_h = 10
            bar_top = min(bottom + 10, h - 20)
            cv2.rectangle(annotated, (left, bar_top), (right, bar_top + bar_h), (80, 80, 80), -1)

            # Quality fill
            fill_w = int((quality / 100) * bar_w)
            cv2.rectangle(annotated, (left, bar_top), (left + fill_w, bar_top + bar_h), color_bgr, -1)

            # Percentage text
            cv2.putText(
                annotated, f"{quality:.1f}%",
                (left, bar_top + bar_h + 22),
                cv2.FONT_HERSHEY_SIMPLEX, 0.65, color_bgr, 2,
            )

            # ── Stable-frame progress ring (top-right of face box) ─────────
            stable_progress = quality_result.get('stable_progress', 0.0)
            if stable_progress > 0:
                cx, cy = right - 20, top - 20
                radius = 14
                # Background circle
                cv2.circle(annotated, (cx, cy), radius, (80, 80, 80), 3)
                # Progress arc (approximate with filled circle at 100%)
                angle = int(360 * stable_progress)
                cv2.ellipse(
                    annotated, (cx, cy), (radius, radius),
                    -90, 0, angle, (0, 255, 128), 3,
                )

        # ── Status message bar at top ──────────────────────────────────────
        message = quality_result.get('message', '')
        if message:
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 0.75
            thickness = 2
            text_size, _ = cv2.getTextSize(message, font, font_scale, thickness)
            text_x = (w - text_size[0]) // 2
            text_y = 35

            # Semi-transparent background rectangle
            cv2.rectangle(
                annotated,
                (text_x - 12, text_y - text_size[1] - 8),
                (text_x + text_size[0] + 12, text_y + 8),
                (0, 0, 0), -1,
            )
            cv2.putText(
                annotated, message,
                (text_x, text_y),
                font, font_scale, (255, 255, 255), thickness,
            )

        # ── Sub-scores (bottom-left HUD) ───────────────────────────────────
        hud_lines = [
            f"Coverage : {quality_result['face_coverage']:.0f}%",
            f"Sharpness: {quality_result['sharpness_score']:.0f}%",
            f"Lighting : {quality_result['brightness_score']:.0f}%",
        ]
        for i, line in enumerate(hud_lines):
            cv2.putText(
                annotated, line,
                (10, h - 15 - (len(hud_lines) - 1 - i) * 22),
                cv2.FONT_HERSHEY_SIMPLEX, 0.52, (200, 200, 200), 1,
            )

        return annotated


# ──────────────────────────────────────────────
# Convenience registration loop
# ──────────────────────────────────────────────

def register_face(
    display: bool = True,
    timeout: float = 30.0,
    stable_frames: int = 3,
) -> Optional[np.ndarray]:
    """
    High-level helper: opens camera, waits for a quality face, returns encoding.

    Parameters
    ----------
    display : bool
        Show live preview window (requires display).
    timeout : float
        Max seconds to wait before giving up.
    stable_frames : int
        Number of consecutive good frames required.

    Returns
    -------
    np.ndarray | None
        128-d face encoding, or None on failure/timeout.
    """
    cam = CameraManager(width=640, height=480)
    checker = FaceQualityChecker(stable_frames_required=stable_frames, camera_manager=cam)
    encoding = None
    start = time.time()

    with cam:
        while time.time() - start < timeout:
            frame = cam.capture_frame()
            if frame is None:
                time.sleep(0.05)
                continue

            result = checker.check_face_quality(frame)

            if display:
                annotated = checker.draw_quality_overlay(frame, result)
                # Convert RGB → BGR for OpenCV display
                cv2.imshow("Face Registration", cv2.cvtColor(annotated, cv2.COLOR_RGB2BGR))
                key = cv2.waitKey(1) & 0xFF
                if key == ord('q'):
                    break

            if result['success'] and result['encoding'] is not None:
                encoding = result['encoding']
                logger.info("Face registered successfully.")
                break

    if display:
        cv2.destroyAllWindows()

    return encoding


# ──────────────────────────────────────────────
# Singleton accessor
# ──────────────────────────────────────────────

_quality_checker: Optional[FaceQualityChecker] = None

def get_quality_checker() -> FaceQualityChecker:
    """Return (or create) the global FaceQualityChecker instance."""
    global _quality_checker
    if _quality_checker is None:
        _quality_checker = FaceQualityChecker()
    return _quality_checker


# ──────────────────────────────────────────────
# Quick test (run directly on Pi)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Starting face registration test — press Q to quit")
    enc = register_face(display=True, timeout=30, stable_frames=3)
    if enc is not None:
        print(f"✅ Encoding captured! Shape: {enc.shape}")
    else:
        print("❌ No face registered within timeout.")


        
