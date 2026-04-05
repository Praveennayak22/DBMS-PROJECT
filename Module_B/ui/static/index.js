(() => {
  if (window.__moduleBIndexInitialized) {
    return;
  }
  window.__moduleBIndexInitialized = true;

  if (!window.shared) {
    return;
  }

  const {
    byId,
    api,
    saveSession,
    clearSession,
    safeJson,
    initShell,
  } = window.shared;

  async function login() {
    try {
      const data = await api("/login", {
        method: "POST",
        body: JSON.stringify({
          username: byId("username").value.trim(),
          password: byId("password").value,
        }),
      });
      saveSession(data.session_token, data.username, data.role);
      safeJson("authInfo", data);
    } catch (err) {
      byId("authInfo").textContent = String(err.message || err);
    }
  }

  async function checkIsAuth() {
    try {
      const data = await api("/isAuth");
      safeJson("authInfo", data);
    } catch (err) {
      byId("authInfo").textContent = String(err.message || err);
    }
  }

  function init() {
    initShell();

    byId("loginBtn").addEventListener("click", login);
    byId("checkAuthBtn").addEventListener("click", checkIsAuth);
    byId("logoutBtn").addEventListener("click", clearSession);
  }

  init();
})();
