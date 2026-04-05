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

function setSessionInfo() {
  byId("sessionState").textContent = state.token
    ? `Logged in as ${state.username} (${state.role})`
    : "Not logged in";

  const adminVisible = isAdmin();
  byId("memberCard").style.display = adminVisible ? "block" : "none";
  byId("groupsCard").style.display = adminVisible ? "block" : "none";
  byId("companyCard").style.display = adminVisible ? "block" : "none";
  byId("jobsCard").style.display = adminVisible ? "block" : "none";
}

function saveSession(token, username, role) {
  state.token = token;
  state.username = username;
  state.role = role;
  localStorage.setItem("sessionToken", token);
  localStorage.setItem("sessionUser", username);
  localStorage.setItem("sessionRole", role);
  setSessionInfo();
}

function clearSession() {
  state.token = "";
  state.username = "";
  state.role = "";
  localStorage.removeItem("sessionToken");
  localStorage.removeItem("sessionUser");
  localStorage.removeItem("sessionRole");
  setSessionInfo();
}

function safeJson(targetId, data) {
  byId(targetId).textContent = JSON.stringify(data, null, 2);
}

function parseCsvInts(value) {
  if (!value.trim()) return [];
  return value
    .split(",")
    .map((v) => Number(v.trim()))
    .filter((n) => Number.isFinite(n));
}

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
    await refreshAll();
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

async function loadPortfolio() {
  try {
    const memberId = Number(byId("memberId").value);
    const data = await api(`/portfolio/${memberId}`);
    byId("bio").value = data.bio || "";
    byId("skills").value = data.skills || "";
    byId("visibility").value = data.portfolio_visibility || "private";
    safeJson("portfolioOut", data);
  } catch (err) {
    byId("portfolioOut").textContent = String(err.message || err);
  }
}

async function updatePortfolio() {
  try {
    const memberId = Number(byId("memberId").value);
    const payload = {
      bio: byId("bio").value.trim() || null,
      skills: byId("skills").value.trim() || null,
      portfolio_visibility: byId("visibility").value,
    };
    const data = await api(`/portfolio/${memberId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    safeJson("portfolioOut", data);
  } catch (err) {
    byId("portfolioOut").textContent = String(err.message || err);
  }
}

async function createMember() {
  try {
    const payload = {
      username: byId("memberUsername").value.trim(),
      email: byId("memberEmail").value.trim(),
      password: byId("memberPassword").value.trim() || "student123",
      full_name: byId("memberFullName").value.trim(),
      role_name: "Student",
      latest_cpi: byId("memberCpi").value ? Number(byId("memberCpi").value) : null,
      program: byId("memberProgram").value.trim() || null,
      discipline: byId("memberDiscipline").value.trim() || null,
      graduating_year: byId("memberGradYear").value ? Number(byId("memberGradYear").value) : null,
      active_backlogs: byId("memberBacklogs").value ? Number(byId("memberBacklogs").value) : 0,
      bio: byId("memberBio").value.trim() || null,
      skills: byId("memberSkills").value.trim() || null,
      portfolio_visibility: byId("memberVisibility").value,
      group_ids: parseCsvInts(byId("memberGroups").value),
    };
    const data = await api("/members", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    safeJson("memberOut", data);
    await refreshGroups();
  } catch (err) {
    byId("memberOut").textContent = String(err.message || err);
  }
}

async function deleteMember() {
  try {
    const memberId = Number(byId("deleteMemberId").value);
    const data = await api(`/members/${memberId}`, { method: "DELETE" });
    safeJson("memberOut", data);
    await refreshGroups();
  } catch (err) {
    byId("memberOut").textContent = String(err.message || err);
  }
}

function renderGroups(items) {
  const root = byId("groupsList");
  root.innerHTML = "";
  items.forEach((g) => {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${g.group_id} ${g.group_name}</strong>
        <small>Members: ${g.member_count}</small>
      </div>
      <code>group_id=${g.group_id}</code>
    `;
    root.appendChild(row);
  });
}

async function refreshGroups() {
  try {
    const data = await api("/groups");
    renderGroups(data);
    safeJson("groupsOut", data);
  } catch (err) {
    byId("groupsOut").textContent = String(err.message || err);
  }
}

async function createGroup() {
  try {
    const data = await api("/groups", {
      method: "POST",
      body: JSON.stringify({ group_name: byId("groupName").value.trim() }),
    });
    safeJson("groupsOut", data);
    byId("groupName").value = "";
    await refreshGroups();
  } catch (err) {
    byId("groupsOut").textContent = String(err.message || err);
  }
}

async function addGroupMember() {
  try {
    const groupId = Number(byId("mapGroupId").value);
    const memberId = Number(byId("mapMemberId").value);
    const data = await api(`/groups/${groupId}/members`, {
      method: "POST",
      body: JSON.stringify({ member_id: memberId }),
    });
    safeJson("groupsOut", data);
    await refreshGroups();
  } catch (err) {
    byId("groupsOut").textContent = String(err.message || err);
  }
}

async function removeGroupMember() {
  try {
    const groupId = Number(byId("mapGroupId").value);
    const memberId = Number(byId("mapMemberId").value);
    const data = await api(`/groups/${groupId}/members/${memberId}`, {
      method: "DELETE",
    });
    safeJson("groupsOut", data);
    await refreshGroups();
  } catch (err) {
    byId("groupsOut").textContent = String(err.message || err);
  }
}

function renderCompanies(items) {
  const root = byId("companiesList");
  root.innerHTML = "";
  items.forEach((c) => {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${c.company_id} ${c.company_name}</strong>
        <small>${c.domain || "-"}</small>
      </div>
      <div class="actions">
        <button data-action="edit" data-id="${c.company_id}" class="ghost">Edit</button>
        <button data-action="delete" data-id="${c.company_id}">Delete</button>
      </div>
    `;
    root.appendChild(row);
  });
}

async function loadCompanies() {
  if (!state.token) return;
  try {
    const data = await api("/companies");
    renderCompanies(data);
  } catch (err) {
    byId("companiesList").textContent = String(err.message || err);
  }
}

async function createCompany() {
  try {
    await api("/companies", {
      method: "POST",
      body: JSON.stringify({
        company_name: byId("companyName").value.trim(),
        domain: byId("companyDomain").value.trim() || null,
      }),
    });
    byId("companyName").value = "";
    byId("companyDomain").value = "";
    await loadCompanies();
  } catch (err) {
    byId("companiesList").textContent = String(err.message || err);
  }
}

async function editCompany(id) {
  const companyName = prompt("New company name:");
  const domain = prompt("New domain:");
  if (!companyName && !domain) return;
  await api(`/companies/${id}`, {
    method: "PATCH",
    body: JSON.stringify({
      company_name: companyName || null,
      domain: domain || null,
    }),
  });
  await loadCompanies();
}

async function deleteCompany(id) {
  if (!confirm(`Delete company ${id}?`)) return;
  await api(`/companies/${id}`, { method: "DELETE" });
  await loadCompanies();
}

function renderJobs(items) {
  const root = byId("jobsList");
  root.innerHTML = "";
  items.forEach((j) => {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${j.job_id} ${j.title}</strong>
        <small>${j.company_name} | CPI <= ${j.min_cpi ?? "-"} | ${j.deadline || "-"}</small>
      </div>
      <div class="actions">
        <button data-action="edit" data-id="${j.job_id}" class="ghost">Edit</button>
        <button data-action="delete" data-id="${j.job_id}">Delete</button>
      </div>
    `;
    root.appendChild(row);
  });
}

async function loadJobs() {
  if (!state.token) return;
  try {
    const data = await api("/jobs");
    renderJobs(data);
  } catch (err) {
    byId("jobsList").textContent = String(err.message || err);
  }
}

async function createJob() {
  try {
    await api("/jobs", {
      method: "POST",
      body: JSON.stringify({
        company_id: Number(byId("jobCompanyId").value),
        title: byId("jobTitle").value.trim(),
        location: byId("jobLocation").value.trim() || null,
        min_cpi: byId("jobMinCpi").value ? Number(byId("jobMinCpi").value) : null,
        deadline: byId("jobDeadline").value.trim() || null,
      }),
    });
    byId("jobTitle").value = "";
    await loadJobs();
  } catch (err) {
    byId("jobsList").textContent = String(err.message || err);
  }
}

async function editJob(id) {
  const title = prompt("New job title:");
  const minCpiRaw = prompt("New min CPI:");
  const payload = {};
  if (title) payload.title = title;
  if (minCpiRaw) payload.min_cpi = Number(minCpiRaw);
  if (Object.keys(payload).length === 0) return;
  await api(`/jobs/${id}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
  await loadJobs();
}

async function deleteJob(id) {
  if (!confirm(`Delete job ${id}?`)) return;
  await api(`/jobs/${id}`, { method: "DELETE" });
  await loadJobs();
}

function renderApplications(items) {
  const root = byId("applicationsList");
  root.innerHTML = "";
  items.forEach((a) => {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${a.application_id} ${a.title || "-"}</strong>
        <small>member=${a.member_id} | ${a.status} | ${a.applied_at || "-"}</small>
      </div>
      <code>${a.full_name || ""}</code>
    `;
    root.appendChild(row);
  });
}

async function loadApplications() {
  if (!state.token) return;
  try {
    const data = await api("/applications");
    renderApplications(data);
  } catch (err) {
    byId("applicationsList").textContent = String(err.message || err);
  }
}

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
  if (!state.token || !isAdmin()) return;
  try {
    const limit = Number(byId("auditLimit").value) || 50;
    const data = await api(`/audit-logs?limit=${limit}`);
    renderAudit(data);
  } catch (err) {
    byId("auditList").textContent = String(err.message || err);
  }
}

function bindListActions() {
  byId("companiesList").addEventListener("click", async (event) => {
    const btn = event.target.closest("button");
    if (!btn) return;
    const id = btn.dataset.id;
    try {
      if (btn.dataset.action === "edit") await editCompany(id);
      if (btn.dataset.action === "delete") await deleteCompany(id);
    } catch (err) {
      byId("companiesList").textContent = String(err.message || err);
    }
  });

  byId("jobsList").addEventListener("click", async (event) => {
    const btn = event.target.closest("button");
    if (!btn) return;
    const id = btn.dataset.id;
    try {
      if (btn.dataset.action === "edit") await editJob(id);
      if (btn.dataset.action === "delete") await deleteJob(id);
    } catch (err) {
      byId("jobsList").textContent = String(err.message || err);
    }
  });
}

async function refreshAll() {
  await Promise.all([
    loadPortfolio(),
    refreshGroups(),
    loadCompanies(),
    loadJobs(),
    loadApplications(),
    loadAuditLogs(),
  ]);
}

function init() {
  setSessionInfo();
  bindListActions();

  byId("loginBtn").addEventListener("click", login);
  byId("checkAuthBtn").addEventListener("click", checkIsAuth);
  byId("logoutBtn").addEventListener("click", clearSession);

  byId("loadPortfolioBtn").addEventListener("click", loadPortfolio);
  byId("updatePortfolioBtn").addEventListener("click", updatePortfolio);

  byId("createMemberBtn").addEventListener("click", createMember);
  byId("deleteMemberBtn").addEventListener("click", deleteMember);

  byId("createGroupBtn").addEventListener("click", createGroup);
  byId("refreshGroupsBtn").addEventListener("click", refreshGroups);
  byId("addGroupMemberBtn").addEventListener("click", addGroupMember);
  byId("removeGroupMemberBtn").addEventListener("click", removeGroupMember);

  byId("createCompanyBtn").addEventListener("click", createCompany);
  byId("refreshCompaniesBtn").addEventListener("click", loadCompanies);

  byId("createJobBtn").addEventListener("click", createJob);
  byId("refreshJobsBtn").addEventListener("click", loadJobs);

  byId("refreshApplicationsBtn").addEventListener("click", loadApplications);
  byId("refreshAuditBtn").addEventListener("click", loadAuditLogs);

  if (state.token) {
    checkIsAuth();
    refreshAll();
  }
}

init();
