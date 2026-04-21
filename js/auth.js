// =============================
// ERP AUTH SYSTEM
// =============================

const ERP_PAGE_PERMISSION_MAP = {
  dashboard: ["SuperAdmin", "Admin", "Billing", "Invoice", "Stock", "Sticker", "Process", "Expense", "Transaction"],
  "admin-approval": ["SuperAdmin", "Admin"],
  sticker: ["SuperAdmin", "Admin", "Sticker"],
  stock: ["SuperAdmin", "Admin", "Stock", "Sticker", "Billing", "Invoice"],
  "material-stock": ["SuperAdmin", "Admin", "Stock", "Process"],
  "daily-report": ["SuperAdmin", "Admin", "Billing", "Invoice", "Stock", "Expense", "Transaction", "Process"],
  "sales-history": ["SuperAdmin", "Admin", "Billing", "Invoice", "Stock"],
  invoice: ["SuperAdmin", "Admin", "Invoice"],
  billing: ["SuperAdmin", "Admin", "Billing"],
  return: ["SuperAdmin", "Admin", "Billing", "Invoice", "Stock"],
  process: ["SuperAdmin", "Admin", "Process"],
  "staff-management": ["SuperAdmin", "Admin"],
  "expense-manager": ["SuperAdmin", "Admin", "Expense"],
  transaction: ["SuperAdmin", "Admin", "Transaction"],
  "transaction-reports": ["SuperAdmin", "Admin", "Transaction"],
  settings: ["SuperAdmin", "Admin"]
};

const ERP_MENU_PAGE_BY_HREF = {
  "dashboard.html": "dashboard",
  "admin-approval.html": "admin-approval",
  "sticker.html": "sticker",
  "stock.html": "stock",
  "material-stock.html": "material-stock",
  "daily-report.html": "daily-report",
  "sales-history.html": "sales-history",
  "invoice.html": "invoice",
  "billing.html": "billing",
  "return.html": "return",
  "process.html": "process",
  "staff-management.html": "staff-management",
  "expense-manager.html": "expense-manager",
  "transaction.html": "transaction",
  "transaction-reports.html": "transaction-reports",
  "settings.html": "settings"
};

const ERP_AUTH_COOKIE_NAME = "erp_user_id";

function getLoggedInUser() {
  try {
    return JSON.parse(localStorage.getItem("erpLoggedInUser")) || null;
  } catch (_) {
    return null;
  }
}

function setAuthCookie(user = null) {
  const targetUser = user || getLoggedInUser();
  const rawUserId = targetUser?.id ?? targetUser?.user_id ?? targetUser?.userId ?? "";
  const userId = String(rawUserId || "").trim();

  if (!userId) return;

  document.cookie = `${ERP_AUTH_COOKIE_NAME}=${encodeURIComponent(userId)}; path=/; SameSite=Lax`;
}

function clearAuthCookie() {
  document.cookie = `${ERP_AUTH_COOKIE_NAME}=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT; SameSite=Lax`;
}

function syncAuthCookie() {
  const user = getLoggedInUser();
  if (user) {
    setAuthCookie(user);
  } else {
    clearAuthCookie();
  }
}

function clearAuthSession() {
  localStorage.removeItem("erpLoggedInUser");
  clearAuthCookie();
}

function getCurrentCompanyId() {
  const user = getLoggedInUser();
  const raw = user?.company_id ?? user?.companyId ?? null;

  if (raw === null || raw === undefined || raw === "") return null;

  const parsed = Number(raw);
  return Number.isNaN(parsed) ? null : parsed;
}

function getCurrentUserId() {
  const user = getLoggedInUser();
  const raw = user?.id ?? user?.user_id ?? user?.userId ?? null;

  if (raw === null || raw === undefined || raw === "") return null;

  const parsed = Number(raw);
  return Number.isNaN(parsed) ? null : parsed;
}

function getNormalizedRole(user = null) {
  const targetUser = user || getLoggedInUser();
  return String(targetUser?.role || "").trim().toLowerCase();
}

function normalizeAllowedRoles(roles = []) {
  return roles.map((role) => String(role || "").trim().toLowerCase()).filter(Boolean);
}

function isSuperAdmin(user = null) {
  const targetUser = user || getLoggedInUser();
  const role = getNormalizedRole(targetUser);
  const email = String(targetUser?.email || "").trim().toLowerCase();

  return role === "superadmin" || email === "grudrapratap0@gmail.com";
}

function isAdminUser(user = null) {
  const targetUser = user || getLoggedInUser();
  return getNormalizedRole(targetUser) === "admin";
}

function buildProtectedQueryString({ includeCompany = false, companyId = null } = {}) {
  const params = new URLSearchParams();
  const userId = getCurrentUserId();
  const resolvedCompanyId =
    companyId === null || companyId === undefined ? getCurrentCompanyId() : Number(companyId);

  if (userId !== null && !Number.isNaN(userId)) {
    params.set("actingUserId", String(userId));
  }

  if (includeCompany && resolvedCompanyId !== null && !Number.isNaN(resolvedCompanyId)) {
    params.set("companyId", String(resolvedCompanyId));
  }

  return params.toString();
}

function showAccessMessage(message) {
  if (typeof window.showToast === "function") {
    window.showToast(message, "error");
  } else {
    alert(message);
  }
}

function requireLogin() {
  const user = getLoggedInUser();

  if (!user) {
    clearAuthCookie();
    showAccessMessage("Login required");
    window.location.href = "login.html";
    return false;
  }

  syncAuthCookie();

  return true;
}

function requireRole(allowedRoles = []) {
  const user = getLoggedInUser();

  if (!user) {
    window.location.href = "login.html";
    return false;
  }

  if (isSuperAdmin(user)) {
    return true;
  }

  const normalizedAllowedRoles = normalizeAllowedRoles(allowedRoles);
  const userRole = getNormalizedRole(user);

  if (!normalizedAllowedRoles.includes(userRole)) {
    showAccessMessage("Access Denied");
    window.location.href = "dashboard.html";
    return false;
  }

  return true;
}

function getCurrentPageKey() {
  const pathname = String(window.location.pathname || "").split("/").pop().toLowerCase();
  return ERP_MENU_PAGE_BY_HREF[pathname] || null;
}

function canAccessPage(pageKey, user = null) {
  const targetUser = user || getLoggedInUser();
  if (!targetUser) return false;
  if (isSuperAdmin(targetUser)) return true;

  const allowedRoles = ERP_PAGE_PERMISSION_MAP[pageKey];
  if (!Array.isArray(allowedRoles) || !allowedRoles.length) return true;

  return normalizeAllowedRoles(allowedRoles).includes(getNormalizedRole(targetUser));
}

function requirePageAccess(pageKey) {
  if (!requireLogin()) return false;

  if (canAccessPage(pageKey)) {
    return true;
  }

  showAccessMessage("Access Denied");
  window.location.href = "dashboard.html";
  return false;
}

function filterSidebarMenuByRole() {
  const user = getLoggedInUser();
  const menuLinks = document.querySelectorAll(".menu a");
  const roleAwareElements = document.querySelectorAll("[data-page-key]");

  if (menuLinks.length) {
    menuLinks.forEach((link) => {
      const href = String(link.getAttribute("href") || "").trim().toLowerCase();
      const pageKey = ERP_MENU_PAGE_BY_HREF[href];
      const listItem = link.closest("li");

      if (!listItem || !pageKey) return;

      listItem.style.display = !user || canAccessPage(pageKey, user) ? "" : "none";
    });
  }

  roleAwareElements.forEach((element) => {
    if (element.closest(".menu")) return;

    const pageKey = String(element.dataset.pageKey || "").trim();
    if (!pageKey) return;

    element.style.display = !user || canAccessPage(pageKey, user) ? "" : "none";
  });
}

function patchFetchWithAuthHeader() {
  if (window.__erpFetchAuthPatched) return;
  const originalFetch = window.fetch?.bind(window);
  if (typeof originalFetch !== "function") return;

  window.fetch = function (input, init = {}) {
    const user = getLoggedInUser();
    const userId = user?.id ?? user?.user_id ?? user?.userId ?? "";
    const headers = new Headers(init.headers || (input instanceof Request ? input.headers : undefined) || {});

    if (userId && !headers.has("x-user")) {
      headers.set("x-user", String(userId));
    }

    return originalFetch(input, {
      ...init,
      headers
    });
  };

  window.__erpFetchAuthPatched = true;
}

syncAuthCookie();
patchFetchWithAuthHeader();
