(function () {
  const TOAST_CONTAINER_ID = "erpGlobalToastContainer";
  const DEFAULT_DURATION = 3000;
  const MAX_TOASTS = 4;
  const DEFAULT_POSITION = "top-right";
  const ICON_MAP = {
    success: "fa-check",
    error: "fa-xmark",
    info: "fa-circle-info"
  };
  const TITLE_MAP = {
    success: "Success",
    error: "Error",
    info: "Information"
  };

  function getSafeType(type) {
    return ["success", "error", "info"].includes(type) ? type : "info";
  }

  function getToastPosition() {
    const configured = window.ERP_TOAST_POSITION || DEFAULT_POSITION;
    return ["top-right", "top-left", "bottom-right", "bottom-left"].includes(configured)
      ? configured
      : DEFAULT_POSITION;
  }

  function applyContainerPosition(container) {
    container.className = "erp-toast-container";
    container.classList.add(getToastPosition());
  }

  function playSuccessSound() {
    const AudioContextRef = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextRef) return;

    try {
      if (!window.__erpToastAudioContext) {
        window.__erpToastAudioContext = new AudioContextRef();
      }

      const ctx = window.__erpToastAudioContext;
      if (ctx.state === "suspended") {
        ctx.resume().catch(() => {});
      }

      const now = ctx.currentTime;
      const oscillator = ctx.createOscillator();
      const gain = ctx.createGain();

      oscillator.type = "sine";
      oscillator.frequency.setValueAtTime(740, now);
      oscillator.frequency.exponentialRampToValueAtTime(880, now + 0.08);

      gain.gain.setValueAtTime(0.0001, now);
      gain.gain.exponentialRampToValueAtTime(0.018, now + 0.01);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.18);

      oscillator.connect(gain);
      gain.connect(ctx.destination);

      oscillator.start(now);
      oscillator.stop(now + 0.18);
    } catch (_) {
      // Silent fallback if audio is blocked.
    }
  }

  function ensureContainer() {
    let container = document.getElementById(TOAST_CONTAINER_ID);
    if (container) {
      applyContainerPosition(container);
      return container;
    }

    container = document.createElement("div");
    container.id = TOAST_CONTAINER_ID;
    applyContainerPosition(container);

    if (document.body) {
      document.body.appendChild(container);
    } else {
      document.addEventListener("DOMContentLoaded", () => {
        if (!document.getElementById(TOAST_CONTAINER_ID)) {
          document.body.appendChild(container);
        }
      }, { once: true });
    }

    return container;
  }

  function showToast(message, type = "success", title = "", duration = DEFAULT_DURATION) {
    const safeType = getSafeType(type);
    const container = ensureContainer();
    if (!container) return;
    const resolvedDuration =
      Number.isFinite(Number(duration)) && Number(duration) > 0 ? Number(duration) : DEFAULT_DURATION;

    while (container.children.length >= MAX_TOASTS) {
      container.firstElementChild?.remove();
    }

    const toast = document.createElement("div");
    toast.className = `erp-toast ${safeType}`;
    toast.innerHTML = `
      <div class="erp-toast-icon"><i class="fas ${ICON_MAP[safeType]}"></i></div>
      <div class="erp-toast-content">
        <div class="erp-toast-title"></div>
        <div class="erp-toast-message"></div>
      </div>
      <button type="button" class="erp-toast-close" aria-label="Close notification">
        <i class="fas fa-xmark"></i>
      </button>
      <div class="erp-toast-progress"><div class="erp-toast-progress-bar"></div></div>
    `;

    toast.querySelector(".erp-toast-title").textContent = title || TITLE_MAP[safeType];
    toast.querySelector(".erp-toast-message").textContent = String(message || "");
    container.appendChild(toast);

    if (safeType === "success") {
      playSuccessSound();
    }

    let timeoutId = null;
    let remaining = resolvedDuration;
    let startedAt = 0;
    let closing = false;

    const closeToast = () => {
      if (closing) return;
      closing = true;
      clearTimeout(timeoutId);
      toast.classList.remove("toast-paused");
      toast.classList.add("is-closing");
      toast.addEventListener("animationend", () => toast.remove(), { once: true });
    };

    const startTimer = () => {
      startedAt = performance.now();
      timeoutId = setTimeout(closeToast, remaining);
    };

    const pauseTimer = () => {
      if (closing) return;
      clearTimeout(timeoutId);
      remaining = Math.max(0, remaining - (performance.now() - startedAt));
      toast.classList.add("toast-paused");
    };

    const resumeTimer = () => {
      if (closing) return;
      toast.classList.remove("toast-paused");
      startTimer();
    };

    toast.addEventListener("mouseenter", pauseTimer);
    toast.addEventListener("mouseleave", resumeTimer);
    toast.querySelector(".erp-toast-close").addEventListener("click", closeToast);

    startTimer();
  }

  window.showToast = showToast;
})();
