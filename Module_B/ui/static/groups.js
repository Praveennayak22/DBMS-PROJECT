const {
  byId,
  api,
  safeJson,
  initShell,
  checkAuthOrRedirect,
  isAdmin,
  isCdsStaff,
} = window.shared;

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

async function init() {
  initShell();
  const auth = await checkAuthOrRedirect();
  if (!auth) return;
  if (!isAdmin() && !isCdsStaff()) {
    window.location.href = "/";
    return;
  }

  if (!isAdmin()) {
    byId("groupCreateControls")?.querySelectorAll("input,button").forEach((el) => {
      if (el.id !== "refreshGroupsBtn") el.disabled = true;
    });
    byId("groupMapControls")?.querySelectorAll("input,button").forEach((el) => {
      el.disabled = true;
    });
  }

  byId("createGroupBtn")?.addEventListener("click", createGroup);
  byId("refreshGroupsBtn")?.addEventListener("click", refreshGroups);
  byId("addGroupMemberBtn")?.addEventListener("click", addGroupMember);
  byId("removeGroupMemberBtn")?.addEventListener("click", removeGroupMember);
  refreshGroups();
}

init();
