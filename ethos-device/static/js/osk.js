(function () {
  const keyboardEl = document.getElementById("osk");
  if (!window.SimpleKeyboard || !keyboardEl) return;

  let currentInput = null;
  let hideTimer    = null;
  let isCapsLock   = false;

  const baseLayout = {
    default: [
      "` 1 2 3 4 5 6 7 8 9 0 - = {bksp}",
      "{tab} q w e r t y u i o p [ ] \\",
      "{lock} a s d f g h j k l ; ' {enter}",
      "{shift} z x c v b n m , . / {shift}",
      "{space} {hide}"
    ],
    shift: [
      "~ ! @ # $ % ^ & * ( ) _ + {bksp}",
      "{tab} Q W E R T Y U I O P { } |",
      "{lock} A S D F G H J K L : \" {enter}",
      "{shift} Z X C V B N M < > ? {shift}",
      "{space} {hide}"
    ]
  };

  const keyboard = new window.SimpleKeyboard.default({
    keyboardDOM: keyboardEl,
    layout: baseLayout,
    layoutName: "default",
    mergeDisplay: true,
    preventMouseDownDefault: true,
    display: {
      "{bksp}":  "DEL",
      "{enter}": "Enter",
      "{tab}":   "Tab",
      "{lock}":  "Caps",
      "{shift}": "Shift",
      "{space}": "Space",
      "{hide}":  "Hide"
    },
    onChange: input => {
      if (currentInput) {
        currentInput.value = input;
        currentInput.dispatchEvent(new Event("input", { bubbles: true }));
      }
    },
    onKeyPress: button => handleKey(button)
  });

  window.simpleKeyboardInstance = keyboard;

  function handleKey(button) {
    switch (button) {
      case "{enter}":
        if (currentInput && currentInput.form) {
          currentInput.form.dispatchEvent(new Event("submit", { cancelable: true, bubbles: true }));
        }
        if (currentInput) currentInput.blur();
        hideKeyboard(true);
        return;

      case "{tab}":
        focusNextInput(currentInput);
        return;

      case "{shift}":
        toggleShift();
        return;

      case "{lock}":
        toggleCaps();
        return;

      case "{hide}":
        if (currentInput) currentInput.blur();
        hideKeyboard(true);
        return;

      default:
        if (currentInput) keyboard.setInput(currentInput.value || "");
        return;
    }
  }

  function toggleShift() {
    const next = keyboard.options.layoutName === "default" ? "shift" : "default";
    keyboard.setOptions({ layoutName: next });
  }

  function toggleCaps() {
    isCapsLock = !isCapsLock;
    const layout = keyboard.options.layoutName;
    if (isCapsLock && layout === "default") {
      keyboard.setOptions({ layoutName: "shift" });
    } else if (!isCapsLock && layout === "shift") {
      keyboard.setOptions({ layoutName: "default" });
    }
  }

  function showKeyboardFor(el) {
    currentInput = el;
    keyboard.setInput(el.value || "");
    keyboardEl.style.display = "block";
    document.body.classList.add("osk-open");
    clearTimeout(hideTimer);
  }

  function hideKeyboard(force = false) {
    clearTimeout(hideTimer);
    const doHide = () => {
      currentInput = null;
      keyboardEl.style.display = "none";
      keyboard.clearInput();
      document.body.classList.remove("osk-open");
    };
    if (force) return doHide();
    hideTimer = setTimeout(doHide, 80);
  }

  function isTextInput(el) {
    if (!el) return false;
    const t = (el.type || "").toLowerCase();
    return (
      (el.tagName === "INPUT" &&
        ["text", "search", "email", "password", "number", "tel", "url"].includes(t)) ||
      el.tagName === "TEXTAREA"
    );
  }

  /* Keep keyboard open when tapping keys */
  keyboardEl.addEventListener("mousedown", e => {
    clearTimeout(hideTimer);
    e.preventDefault();
  });

  /* Show on focus */
  document.addEventListener("focusin", e => {
    if (isTextInput(e.target)) showKeyboardFor(e.target);
  });

  /* Hide on blur (unless focus moves to another input or inside keyboard) */
  document.addEventListener("focusout", e => {
    const to = e.relatedTarget;
    if (isTextInput(to)) return;
    if (keyboardEl.contains(to)) return;
    hideKeyboard(false);
  });

  /* Sync caret when user clicks back into a field */
  document.addEventListener("click", e => {
    if (isTextInput(e.target)) keyboard.setInput(e.target.value || "");
  });

  function focusNextInput(el) {
    const inputs = Array.from(document.querySelectorAll("input, textarea"))
      .filter(isTextInput)
      .filter(i => !i.disabled && i.offsetParent !== null);
    const idx = inputs.indexOf(el);
    if (idx >= 0 && idx < inputs.length - 1) inputs[idx + 1].focus();
  }
})();
