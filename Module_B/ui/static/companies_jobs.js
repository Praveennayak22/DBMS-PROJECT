const {
  byId,
  api,
  initShell,
  checkAuthOrRedirect,
  isRecruiter,
  isAdmin,
  canManageCompaniesJobs,
  canViewCompaniesJobs,
} = window.shared;

function renderCompanies(items) {
  const root = byId("companiesList");
  root.innerHTML = "";
  items.forEach((c) => {
    const row = document.createElement("div");
    row.className = "item";
    const actions = canManageCompaniesJobs()
      ? `
      <div class="actions">
        <button data-action="edit" data-id="${c.company_id}" class="ghost">Edit</button>
        <button data-action="delete" data-id="${c.company_id}">Delete</button>
      </div>
    `
      : "";
    row.innerHTML = `
      <div>
        <strong>#${c.company_id} ${c.company_name}</strong>
        <small>${c.domain || "-"}</small>
      </div>
      ${actions}
    `;
    root.appendChild(row);
  });
}

function renderJobs(items) {
  const root = byId("jobsList");
  root.innerHTML = "";
  items.forEach((j) => {
    const row = document.createElement("div");
    row.className = "item";
    const actions = canManageCompaniesJobs()
      ? `
      <div class="actions">
        <button data-action="edit" data-id="${j.job_id}" class="ghost">Edit</button>
        <button data-action="delete" data-id="${j.job_id}">Delete</button>
      </div>
    `
      : "";
    row.innerHTML = `
      <div>
        <strong>#${j.job_id} ${j.title}</strong>
        <small>${j.company_name} | CPI >= ${j.min_cpi ?? "-"} | ${j.deadline || "-"}</small>
      </div>
      ${actions}
    `;
    root.appendChild(row);
  });
}

function renderRecruiters(items) {
  const root = byId("recruitersList");
  if (!root) return;
  root.innerHTML = "";
  items.forEach((r) => {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${r.user_id} ${r.username}</strong>
        <small>${r.full_name || "-"} | active=${r.is_active ? 1 : 0}</small>
      </div>
      <code>${r.email || "-"}</code>
    `;
    root.appendChild(row);
  });
}

async function loadCompanies() {
  try {
    const data = await api("/companies");
    renderCompanies(data);
  } catch (err) {
    byId("companiesList").textContent = String(err.message || err);
  }
}

async function loadMyCompanyInfo() {
  try {
    const data = await api("/companies/me");
    byId("myCompanyInfo").textContent = JSON.stringify(data, null, 2);
    byId("jobCompanyId").value = data.company_id;
  } catch (err) {
    byId("myCompanyInfo").textContent = String(err.message || err);
  }
}

async function loadJobs() {
  try {
    const data = await api("/jobs");
    renderJobs(data);
  } catch (err) {
    byId("jobsList").textContent = String(err.message || err);
  }
}

async function loadRecruiters() {
  if (!isAdmin()) return;
  try {
    const data = await api("/recruiters");
    renderRecruiters(data);
  } catch (err) {
    byId("recruitersList").textContent = String(err.message || err);
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

async function createRecruiter() {
  try {
    await api("/recruiters", {
      method: "POST",
      body: JSON.stringify({
        username: byId("recruiterUsername").value.trim(),
        email: byId("recruiterEmail").value.trim(),
        password: byId("recruiterPassword").value.trim() || "recruiter123",
        full_name: byId("recruiterFullName").value.trim(),
        role_name: "Recruiter",
      }),
    });
    byId("recruiterUsername").value = "";
    byId("recruiterEmail").value = "";
    byId("recruiterFullName").value = "";
    await loadRecruiters();
  } catch (err) {
    byId("recruitersList").textContent = String(err.message || err);
  }
}

async function deleteRecruiter() {
  try {
    const recruiterId = Number(byId("deleteRecruiterId").value);
    if (!Number.isFinite(recruiterId) || recruiterId <= 0) return;
    await api(`/recruiters/${recruiterId}`, { method: "DELETE" });
    await loadRecruiters();
  } catch (err) {
    byId("recruitersList").textContent = String(err.message || err);
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

async function init() {
  initShell();
  const auth = await checkAuthOrRedirect();
  if (!auth) return;
  if (!canViewCompaniesJobs()) {
    window.location.href = "/";
    return;
  }

  if (isRecruiter()) {
    await loadMyCompanyInfo();
  } else {
    byId("myCompanyCard").style.display = "none";
  }

  if (!canManageCompaniesJobs()) {
    byId("companyCrudCard").querySelectorAll("input,button").forEach((el) => {
      if (el.id !== "refreshCompaniesBtn") el.disabled = true;
    });
    byId("jobsCrudCard").querySelectorAll("input,button").forEach((el) => {
      if (el.id !== "refreshJobsBtn") el.disabled = true;
    });
  }

  if (!isAdmin()) {
    byId("adminRecruitersCard").style.display = "none";
  }

  bindListActions();
  byId("createCompanyBtn")?.addEventListener("click", createCompany);
  byId("refreshCompaniesBtn").addEventListener("click", loadCompanies);
  byId("createJobBtn")?.addEventListener("click", createJob);
  byId("refreshJobsBtn").addEventListener("click", loadJobs);
  byId("createRecruiterBtn")?.addEventListener("click", createRecruiter);
  byId("deleteRecruiterBtn")?.addEventListener("click", deleteRecruiter);
  byId("refreshRecruitersBtn")?.addEventListener("click", loadRecruiters);

  await Promise.all([loadCompanies(), loadJobs()]);
  if (isAdmin()) {
    await loadRecruiters();
  }
}

init();
