(function () {
  const INSTALL_BUTTON_ID = "erpInstallAppButton";
  const INSTALL_STYLE_ID = "erpInstallAppStyle";
  let deferredPrompt = null;

  function isStandaloneMode() {
    return window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
  }

  function ensureInstallStyles() {
    if (document.getElementById(INSTALL_STYLE_ID)) return;

    const style = document.createElement("style");
    style.id = INSTALL_STYLE_ID;
    style.textContent = `
      .erp-install-btn {
        position: fixed;
        right: 18px;
        bottom: 18px;
        z-index: 9999;
        display: none;
        align-items: center;
        gap: 10px;
        min-height: 48px;
        padding: 12px 16px;
        border: none;
        border-radius: 999px;
        background: linear-gradient(180deg, #1e293b, #0f172a);
        color: #fff;
        font-size: 14px;
        font-weight: 800;
        box-shadow: 0 14px 28px rgba(15, 23, 42, 0.28);
        cursor: pointer;
      }

      .erp-install-btn.show {
        display: inline-flex;
      }

      .erp-install-btn .erp-install-badge {
        color: #f8d38f;
        font-size: 16px;
        line-height: 1;
      }

      @media (max-width: 768px) {
        .erp-install-btn {
          left: 16px;
          right: 16px;
          bottom: 16px;
          justify-content: center;
        }
      }
    `;
    document.head.appendChild(style);
  }

  function ensureInstallButton() {
    let button = document.getElementById(INSTALL_BUTTON_ID);
    if (button) return button;

    ensureInstallStyles();
    button = document.createElement("button");
    button.type = "button";
    button.id = INSTALL_BUTTON_ID;
    button.className = "erp-install-btn";
    button.innerHTML = '<span class="erp-install-badge" aria-hidden="true">+</span><span>Install App</span>';
    button.addEventListener("click", async function () {
      if (!deferredPrompt) return;

      deferredPrompt.prompt();
      try {
        await deferredPrompt.userChoice;
      } catch (_) {
      }
      deferredPrompt = null;
      button.classList.remove("show");
    });

    document.body.appendChild(button);
    return button;
  }

  function updateInstallButton() {
    const button = ensureInstallButton();
    const canShow = !!deferredPrompt && !isStandaloneMode();
    button.classList.toggle("show", canShow);
  }

  window.addEventListener("beforeinstallprompt", (event) => {
    event.preventDefault();
    deferredPrompt = event;
    updateInstallButton();
  });

  window.addEventListener("appinstalled", () => {
    deferredPrompt = null;
    updateInstallButton();
  });

  window.addEventListener("load", () => {
    ensureInstallButton();
    updateInstallButton();

    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/service-worker.js").catch((error) => {
        console.log("Service worker registration failed:", error);
      });
    }
  });
})();
