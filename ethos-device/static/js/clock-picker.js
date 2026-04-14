/**
 * 24-Hour Circular Clock Picker
 * Auto-initializes on <input data-clock-picker>
 */
(function () {
  "use strict";

  var OUTER_R = 105;   // px from center for outer ring
  var INNER_R = 70;    // px from center for inner ring
  var FACE_SIZE = 260; // matches CSS .cp-face width/height

  var overlay, face, handWrap, hhEl, mmEl;
  var activeInput = null;
  var mode = "hour";   // "hour" | "minute"
  var selHour = 0;
  var selMin = 0;
  var dragging = false;

  // ── Build DOM once ──
  function build() {
    overlay = document.createElement("div");
    overlay.className = "cp-overlay";
    overlay.innerHTML =
      '<div class="cp-container">' +
        '<div class="cp-header">' +
          '<span class="cp-hh cp-active" id="cp-hh">00</span>' +
          '<span class="cp-sep">:</span>' +
          '<span class="cp-mm" id="cp-mm">00</span>' +
        '</div>' +
        '<div class="cp-face" id="cp-face">' +
          '<div class="cp-center-dot"></div>' +
          '<div class="cp-hand-wrap" id="cp-hand"><div class="cp-hand-line"></div><div class="cp-hand-tip"></div></div>' +
        '</div>' +
        '<div class="cp-actions">' +
          '<button class="cp-btn cp-btn-cancel" id="cp-cancel" type="button">Cancel</button>' +
          '<button class="cp-btn cp-btn-ok" id="cp-ok" type="button">OK</button>' +
        '</div>' +
      '</div>';
    document.body.appendChild(overlay);

    face    = document.getElementById("cp-face");
    handWrap = document.getElementById("cp-hand");
    hhEl    = document.getElementById("cp-hh");
    mmEl    = document.getElementById("cp-mm");

    // Header taps to switch mode
    hhEl.addEventListener("click", function () { switchMode("hour"); });
    mmEl.addEventListener("click", function () { switchMode("minute"); });

    // Buttons
    document.getElementById("cp-cancel").addEventListener("click", function () { close(false); });
    document.getElementById("cp-ok").addEventListener("click", function () { close(true); });

    // Overlay click = cancel
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) close(false);
    });

    // Touch / mouse on clock face
    face.addEventListener("mousedown", onFaceDown);
    face.addEventListener("touchstart", onFaceDown, { passive: false });
    document.addEventListener("mousemove", onFaceMove);
    document.addEventListener("touchmove", onFaceMove, { passive: false });
    document.addEventListener("mouseup", onFaceUp);
    document.addEventListener("touchend", onFaceUp);
  }

  // ── Polar math ──
  function polarFromCenter(clientX, clientY) {
    var rect = face.getBoundingClientRect();
    var cx = rect.left + rect.width / 2;
    var cy = rect.top + rect.height / 2;
    var dx = clientX - cx;
    var dy = clientY - cy;
    var angle = Math.atan2(dx, -dy) * (180 / Math.PI);
    if (angle < 0) angle += 360;
    var dist = Math.sqrt(dx * dx + dy * dy);
    return { angle: angle, dist: dist };
  }

  function clientXY(e) {
    if (e.touches && e.touches.length) return { x: e.touches[0].clientX, y: e.touches[0].clientY };
    return { x: e.clientX, y: e.clientY };
  }

  // ── Value from angle/dist ──
  function hourFromPolar(angle, dist) {
    var sector = Math.round(angle / 30) % 12;
    // Threshold: midpoint between inner and outer radii
    var threshold = (OUTER_R + INNER_R) / 2;
    if (dist < threshold) {
      // Inner ring: 0-11
      return sector;
    } else {
      // Outer ring: 12-23. sector 0 = 12
      return sector === 0 ? 12 : sector + 12;
    }
  }

  function minuteFromAngle(angle) {
    var raw = Math.round(angle / 6) % 60;
    // Snap to nearest 5
    return Math.round(raw / 5) * 5 % 60;
  }

  // ── Face interaction ──
  function onFaceDown(e) {
    e.preventDefault();
    dragging = true;
    pickFromEvent(e);
  }
  function onFaceMove(e) {
    if (!dragging) return;
    e.preventDefault();
    pickFromEvent(e);
  }
  function onFaceUp() {
    if (!dragging) return;
    dragging = false;
    // Auto-advance from hour to minute
    if (mode === "hour") {
      setTimeout(function () { switchMode("minute"); }, 200);
    }
  }

  function pickFromEvent(e) {
    var pt = clientXY(e);
    var p = polarFromCenter(pt.x, pt.y);
    if (mode === "hour") {
      selHour = hourFromPolar(p.angle, p.dist);
      hhEl.textContent = pad(selHour);
    } else {
      selMin = minuteFromAngle(p.angle);
      mmEl.textContent = pad(selMin);
    }
    renderNums();
    updateHand();
  }

  // ── Render numbers ──
  function renderNums() {
    // Remove old numbers
    var old = face.querySelectorAll(".cp-num");
    for (var i = 0; i < old.length; i++) face.removeChild(old[i]);

    if (mode === "hour") {
      renderHourNums();
    } else {
      renderMinuteNums();
    }
  }

  function makeNum(value, label, radius, isInner, isSelected) {
    var el = document.createElement("div");
    el.className = "cp-num " + (isInner ? "cp-num-inner" : "cp-num-outer") + (isSelected ? " cp-selected" : "");
    el.textContent = label;

    var idx;
    if (mode === "hour") {
      idx = value % 12; // position index 0-11
    } else {
      idx = value / 5;  // position index 0-11
    }
    var angleDeg = idx * 30;
    var angleRad = angleDeg * Math.PI / 180;
    var cx = FACE_SIZE / 2 + radius * Math.sin(angleRad);
    var cy = FACE_SIZE / 2 - radius * Math.cos(angleRad);
    el.style.left = cx + "px";
    el.style.top = cy + "px";

    el.addEventListener("click", function (e) {
      e.stopPropagation();
      if (mode === "hour") {
        selHour = value;
        hhEl.textContent = pad(selHour);
        renderNums();
        updateHand();
        setTimeout(function () { switchMode("minute"); }, 200);
      } else {
        selMin = value;
        mmEl.textContent = pad(selMin);
        renderNums();
        updateHand();
      }
    });

    face.appendChild(el);
  }

  function renderHourNums() {
    // Outer ring: 12-23 (12 at top)
    for (var h = 12; h < 24; h++) {
      makeNum(h, String(h), OUTER_R, false, selHour === h);
    }
    // Inner ring: 0-11 (0 at top)
    for (var h2 = 0; h2 < 12; h2++) {
      makeNum(h2, pad(h2), INNER_R, true, selHour === h2);
    }
  }

  function renderMinuteNums() {
    for (var m = 0; m < 60; m += 5) {
      makeNum(m, pad(m), OUTER_R, false, selMin === m);
    }
  }

  // ── Hand ──
  function updateHand() {
    var angleDeg, radius;
    if (mode === "hour") {
      angleDeg = (selHour % 12) * 30;
      radius = selHour >= 0 && selHour < 12 ? INNER_R : OUTER_R;
    } else {
      angleDeg = (selMin / 5) * 30;
      radius = OUTER_R;
    }
    // Hand length = distance from center minus some padding for the number circle
    var len = radius - 14;
    handWrap.style.height = len + "px";
    handWrap.style.transform = "translateX(-50%) rotate(" + angleDeg + "deg)";
  }

  // ── Mode switch ──
  function switchMode(m) {
    mode = m;
    if (m === "hour") {
      hhEl.classList.add("cp-active");
      mmEl.classList.remove("cp-active");
    } else {
      mmEl.classList.add("cp-active");
      hhEl.classList.remove("cp-active");
    }
    renderNums();
    updateHand();
  }

  // ── Open / Close ──
  function open(inputEl) {
    activeInput = inputEl;
    // Parse existing value
    var val = (inputEl.value || "").trim();
    if (/^\d{1,2}:\d{2}$/.test(val)) {
      var parts = val.split(":");
      selHour = parseInt(parts[0], 10) || 0;
      selMin = parseInt(parts[1], 10) || 0;
      // Snap minutes to nearest 5
      selMin = Math.round(selMin / 5) * 5 % 60;
    } else {
      selHour = 0;
      selMin = 0;
    }
    hhEl.textContent = pad(selHour);
    mmEl.textContent = pad(selMin);
    switchMode("hour");
    overlay.classList.add("cp-open");
  }

  function close(apply) {
    if (apply && activeInput) {
      activeInput.value = pad(selHour) + ":" + pad(selMin);
      // Dispatch events so any listeners pick up the change
      activeInput.dispatchEvent(new Event("input", { bubbles: true }));
      activeInput.dispatchEvent(new Event("change", { bubbles: true }));
    }
    overlay.classList.remove("cp-open");
    activeInput = null;
  }

  // ── Helpers ──
  function pad(n) { return (n < 10 ? "0" : "") + n; }

  // ── Init ──
  function init() {
    build();
    var inputs = document.querySelectorAll("input[data-clock-picker]");
    for (var i = 0; i < inputs.length; i++) {
      (function (inp) {
        inp.readOnly = true;
        inp.style.cursor = "pointer";
        // Prevent focus (and OSK) — open picker instead
        inp.addEventListener("mousedown", function (e) {
          e.preventDefault();
          open(inp);
        });
        inp.addEventListener("touchstart", function (e) {
          e.preventDefault();
          open(inp);
        }, { passive: false });
      })(inputs[i]);
    }
  }

  // Expose for programmatic use
  window.ClockPicker = {
    open: function (el) { open(el); },
    setValue: function (el, v) { el.value = v; },
    getValue: function (el) { return el.value; }
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
