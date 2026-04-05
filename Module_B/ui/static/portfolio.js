const {
  byId,
  api,
  canViewPortfolio,
  safeJson,
  initShell,
  checkAuthOrRedirect,
  isStudent,
} = window.shared;

async function hydrateOwnMemberId() {
  if (!isStudent()) return;
  try {
    const data = await api("/me/student");
    byId("memberId").value = data.member_id;
    byId("memberId").disabled = true;
  } catch {
    // Ignore when student profile isn't present yet.
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

async function init() {
  initShell();
  const auth = await checkAuthOrRedirect();
  if (!auth) return;

  if (!canViewPortfolio()) {
    window.location.href = "/ui/companies-jobs";
    return;
  }

  await hydrateOwnMemberId();

  byId("loadPortfolioBtn").addEventListener("click", loadPortfolio);
  byId("updatePortfolioBtn").addEventListener("click", updatePortfolio);
  loadPortfolio();
}

init();
