(() => {
  if (window.shared) {
    return;
  }

  const state = {
    token: localStorage.getItem("sessionToken") || "",
    role: localStorage.getItem("sessionRole") || "",
    username: localStorage.getItem("sessionUser") || "",
  };

  const byId = (id) => document.getElementById(id);

  function authHeaders() {
    return state.token ? { "X-Session-Token": state.token } : {};
  }

  function isAdmin() {
    return state.role === "admin" || state.role === "CDS Manager";
  }

  function isCdsStaff() {
    return isAdmin() || state.role === "CDS Team";
  }

  function isRecruiter() {
    return state.role === "Recruiter";
  }

  function isAlumni() {
    return state.role === "Alumni";
  }

  function isStudent() {
    return state.role === "Student";
  }

  function canManageCompaniesJobs() {
    return isAdmin() || isRecruiter();
  }

  function canViewCompaniesJobs() {
    return canManageCompaniesJobs() || isCdsStaff();
  }

  function canViewGroups() {
    return isAdmin() || isCdsStaff();
  }

  function canViewMembers() {
    return isAdmin() || isCdsStaff() || isAlumni();
  }

  function canViewAudit() {
    return isAdmin() || isCdsStaff();
  }

  function canViewPortfolio() {
    return !isRecruiter();
  }

  async function api(path, options = {}) {
    const response = await fetch(path, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(),
        ...(options.headers || {}),
      },
    });

    const text = await response.text();
    let body = null;
    if (text) {
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
    }

    if (!response.ok) {
      const detail = body && body.detail ? body.detail : response.statusText;
      throw new Error(`${response.status} ${detail}`);
    }

    return body;
  }

  function saveSession(token, username, role) {
    state.token = token;
    state.username = username;
    state.role = role;
    localStorage.setItem("sessionToken", token);
    localStorage.setItem("sessionUser", username);
    localStorage.setItem("sessionRole", role);
    renderSessionState();
  }

  function clearSession() {
    state.token = "";
    state.username = "";
    state.role = "";
    localStorage.removeItem("sessionToken");
    localStorage.removeItem("sessionUser");
    localStorage.removeItem("sessionRole");
    renderSessionState();
  }

  function renderSessionState() {
    const sessionState = byId("sessionState");
    if (sessionState) {
      sessionState.textContent = state.token
        ? `Logged in as ${state.username} (${state.role})`
        : "Not logged in";
    }

    document.querySelectorAll("[data-admin-only='1']").forEach((el) => {
      el.style.display = isAdmin() ? "" : "none";
    });

    document.querySelectorAll("[data-companies-access='1']").forEach((el) => {
      el.style.display = canManageCompaniesJobs() ? "" : "none";
    });

    document.querySelectorAll("[data-companies-view='1']").forEach((el) => {
      el.style.display = canViewCompaniesJobs() ? "" : "none";
    });

    document.querySelectorAll("[data-groups-view='1']").forEach((el) => {
      el.style.display = canViewGroups() ? "" : "none";
    });

    document.querySelectorAll("[data-members-access='1']").forEach((el) => {
      el.style.display = canViewMembers() ? "" : "none";
    });

    document.querySelectorAll("[data-audit-access='1']").forEach((el) => {
      el.style.display = canViewAudit() ? "" : "none";
    });

    document.querySelectorAll("[data-portfolio-access='1']").forEach((el) => {
      el.style.display = canViewPortfolio() ? "" : "none";
    });
  }

  function safeJson(targetId, data) {
    const target = byId(targetId);
    if (target) target.textContent = JSON.stringify(data, null, 2);
  }

  function parseCsvInts(value) {
    if (!value || !value.trim()) return [];
    return value
      .split(",")
      .map((v) => Number(v.trim()))
      .filter((n) => Number.isFinite(n));
  }

  function initShell() {
    renderSessionState();
    const logoutBtn = byId("logoutBtn");
    if (logoutBtn) {
      logoutBtn.addEventListener("click", () => {
        clearSession();
        window.location.href = "/";
      });
    }
  }

  async function checkAuthOrRedirect() {
    if (!state.token) {
      window.location.href = "/";
      return null;
    }
    try {
      return await api("/isAuth");
    } catch {
      clearSession();
      window.location.href = "/";
      return null;
    }
  }

  window.shared = {
    state,
    byId,
    api,
    isAdmin,
    isCdsStaff,
    isRecruiter,
    isAlumni,
    isStudent,
    canManageCompaniesJobs,
    canViewCompaniesJobs,
    canViewGroups,
    canViewMembers,
    canViewAudit,
    canViewPortfolio,
    saveSession,
    clearSession,
    safeJson,
    parseCsvInts,
    initShell,
    checkAuthOrRedirect,
  };
})();
