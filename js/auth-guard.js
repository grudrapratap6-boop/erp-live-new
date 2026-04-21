(function () {
  const LOGIN_PAGE = "login.html";
  const AUTH_COOKIE_NAME = "erp_user_id";

  function getStoredUser() {
    try {
      return JSON.parse(localStorage.getItem("erpLoggedInUser")) || null;
    } catch (_) {
      return null;
    }
  }

  function setAuthCookie(user) {
    const rawUserId = user?.id ?? user?.user_id ?? user?.userId ?? "";
    const userId = String(rawUserId || "").trim();
    if (!userId) return;

    document.cookie = `${AUTH_COOKIE_NAME}=${encodeURIComponent(userId)}; path=/; SameSite=Lax`;
  }

  function clearAuthCookie() {
    document.cookie = `${AUTH_COOKIE_NAME}=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT; SameSite=Lax`;
  }

  const user = getStoredUser();
  if (!user) {
    clearAuthCookie();
    window.location.replace(LOGIN_PAGE);
    return;
  }

  setAuthCookie(user);

  if (!window.__erpFetchAuthPatched && typeof window.fetch === "function") {
    const originalFetch = window.fetch.bind(window);
    window.fetch = function (input, init = {}) {
      const latestUser = getStoredUser();
      const latestUserId = latestUser?.id ?? latestUser?.user_id ?? latestUser?.userId ?? "";
      const headers = new Headers(
        init.headers || (input instanceof Request ? input.headers : undefined) || {}
      );

      if (latestUserId && !headers.has("x-user")) {
        headers.set("x-user", String(latestUserId));
      }

      return originalFetch(input, {
        ...init,
        headers
      });
    };

    window.__erpFetchAuthPatched = true;
  }
})();
