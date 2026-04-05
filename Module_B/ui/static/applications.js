const {
  byId,
  api,
  initShell,
  checkAuthOrRedirect,
  isStudent,
  isRecruiter,
  isAdmin,
} = window.shared;

let applicationRows = [];

function renderJobs(items) {
  const root = byId("jobsList");
  if (!root) return;

  const appliedJobIds = new Set(applicationRows.map((a) => Number(a.job_id)));
  root.innerHTML = "";
  items.forEach((j) => {
    const alreadyApplied = appliedJobIds.has(Number(j.job_id));
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${j.job_id} ${j.title}</strong>
        <small>${j.company_name} | CPI >= ${j.min_cpi ?? "-"} | ${j.deadline || "-"}</small>
      </div>
      <div class="actions">
        <button data-action="apply" data-job-id="${j.job_id}" ${alreadyApplied ? "disabled" : ""}>${alreadyApplied ? "Applied" : "Apply"}</button>
      </div>
    `;
    root.appendChild(row);
  });
}

function renderApplications(items) {
  const root = byId("applicationsList");
  root.innerHTML = "";
  items.forEach((a) => {
    const canUpdateStatus = isAdmin() || isRecruiter();
    const statusActions = canUpdateStatus
      ? `
      <div class="actions">
        <select data-app-status="${a.application_id}">
          <option value="Applied" ${a.status === "Applied" ? "selected" : ""}>Applied</option>
          <option value="Shortlisted" ${a.status === "Shortlisted" ? "selected" : ""}>Shortlisted</option>
          <option value="Rejected" ${a.status === "Rejected" ? "selected" : ""}>Rejected</option>
          <option value="Offered" ${a.status === "Offered" ? "selected" : ""}>Offered</option>
        </select>
        <button data-action="update-status" data-app-id="${a.application_id}">Update</button>
      </div>
    `
      : "";

    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${a.application_id} ${a.title || "-"} (${a.company_name || "-"})</strong>
        <small>job=${a.job_id} | member=${a.member_id} | ${a.status} | ${a.applied_at || "-"}</small>
      </div>
      <code>${a.full_name || ""}</code>
      ${statusActions}
    `;
    root.appendChild(row);
  });
}

async function loadApplications() {
  try {
    const data = await api("/applications");
    applicationRows = data;
    renderApplications(data);

    if (isStudent()) {
      renderJobs(await api("/jobs"));
    }
  } catch (err) {
    byId("applicationsList").textContent = String(err.message || err);
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

async function applyForJob(jobId) {
  await api("/applications", {
    method: "POST",
    body: JSON.stringify({
      job_id: Number(jobId),
      status: "Applied",
    }),
  });
  await loadApplications();
}

async function updateApplicationStatus(applicationId) {
  const select = document.querySelector(`select[data-app-status="${applicationId}"]`);
  if (!select) return;
  await api(`/applications/${applicationId}`, {
    method: "PATCH",
    body: JSON.stringify({ status: select.value }),
  });
  await loadApplications();
}

function bindJobActions() {
  const jobsList = byId("jobsList");
  if (!jobsList) return;

  jobsList.addEventListener("click", async (event) => {
    const btn = event.target.closest("button");
    if (!btn || btn.dataset.action !== "apply") return;

    const jobId = btn.dataset.jobId;
    if (!jobId) return;

    try {
      await applyForJob(jobId);
      await loadJobs();
    } catch (err) {
      jobsList.textContent = String(err.message || err);
    }
  });
}

function bindApplicationActions() {
  byId("applicationsList").addEventListener("click", async (event) => {
    const btn = event.target.closest("button");
    if (!btn || btn.dataset.action !== "update-status") return;

    const appId = btn.dataset.appId;
    if (!appId) return;

    try {
      await updateApplicationStatus(appId);
    } catch (err) {
      byId("applicationsList").textContent = String(err.message || err);
    }
  });
}

async function init() {
  initShell();
  const auth = await checkAuthOrRedirect();
  if (!auth) return;

  const jobsCard = byId("availableJobsCard");
  if (jobsCard && !isStudent()) {
    jobsCard.style.display = "none";
  }

  bindJobActions();
  bindApplicationActions();
  byId("refreshApplicationsBtn").addEventListener("click", loadApplications);
  byId("refreshJobsBtn")?.addEventListener("click", loadJobs);

  if (isStudent()) {
    await loadJobs();
  }
  await loadApplications();
}

init();
