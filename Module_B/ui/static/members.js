const {
  byId,
  api,
  safeJson,
  parseCsvInts,
  initShell,
  checkAuthOrRedirect,
  canViewMembers,
  isAdmin,
} = window.shared;

function renderMembers(items) {
  const root = byId("membersList");
  root.innerHTML = "";
  items.forEach((m) => {
    const row = document.createElement("div");
    row.className = "item";
    row.innerHTML = `
      <div>
        <strong>#${m.member_id} ${m.full_name}</strong>
        <small>${m.program || "-"} | ${m.discipline || "-"} | CPI ${m.latest_cpi ?? "-"} | visibility=${m.portfolio_visibility}</small>
      </div>
      <code>${m.email || "-"}</code>
    `;
    root.appendChild(row);
  });
}

async function loadMembers() {
  try {
    const data = await api("/members");
    renderMembers(data);
  } catch (err) {
    byId("membersList").textContent = String(err.message || err);
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
  } catch (err) {
    byId("memberOut").textContent = String(err.message || err);
  }
}

async function deleteMember() {
  try {
    const memberId = Number(byId("deleteMemberId").value);
    const data = await api(`/members/${memberId}`, { method: "DELETE" });
    safeJson("memberOut", data);
  } catch (err) {
    byId("memberOut").textContent = String(err.message || err);
  }
}

async function init() {
  initShell();
  const auth = await checkAuthOrRedirect();
  if (!auth) return;
  if (!canViewMembers()) {
    window.location.href = "/";
    return;
  }

  if (!isAdmin()) {
    byId("adminCreateMemberCard").style.display = "none";
    byId("adminDeleteMemberCard").style.display = "none";
  }

  const createBtn = byId("createMemberBtn");
  const deleteBtn = byId("deleteMemberBtn");
  if (createBtn) createBtn.addEventListener("click", createMember);
  if (deleteBtn) deleteBtn.addEventListener("click", deleteMember);
  byId("refreshMembersBtn").addEventListener("click", loadMembers);
  await loadMembers();
}

init();
