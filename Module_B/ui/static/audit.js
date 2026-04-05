const {
  byId,
  api,
  initShell,
  checkAuthOrRedirect,
  canViewAudit,
} = window.shared;

function renderAudit(items) {
  const root = byId("auditList");
  root.innerHTML = "";
  items.forEach((log) => {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${log.log_id} ${log.action} ${log.table_name}</strong>
        <small>${log.logged_at} | actor=${log.actor_user_id} | ${log.status}</small>
      </div>
      <code>${log.request_path || "-"}</code>
    `;
    root.appendChild(row);
  });
}

async function loadAuditLogs() {
  try {
    const limit = Number(byId("auditLimit").value) || 50;
    const data = await api(`/audit-logs?limit=${limit}`);
    renderAudit(data);
  } catch (err) {
    byId("auditList").textContent = String(err.message || err);
  }
}

async function init() {
  initShell();
  const auth = await checkAuthOrRedirect();
  if (!auth) return;
  if (!canViewAudit()) {
    window.location.href = "/";
    return;
  }

  byId("refreshAuditBtn").addEventListener("click", loadAuditLogs);
  loadAuditLogs();
}

init();
