import os
from fastapi.testclient import TestClient

os.environ.setdefault("MODULE_B_DB_DSN", "postgresql://postgres:Dinesh%40123@localhost:5432/module_b")

from Module_B.app.main import app


def check(name, ok, detail=""):
    status = "OK" if ok else "FAIL"
    suffix = f" - {detail}" if detail else ""
    print(f"[{status}] {name}{suffix}")


with TestClient(app) as client:
    def login(username, password):
        response = client.post("/login", json={"username": username, "password": password})
        return response.status_code, response.json() if response.headers.get("content-type", "").startswith("application/json") else {}

    code, admin = login("admin", "admin123")
    check("Admin login", code == 200, str(code))
    admin_h = {"X-Session-Token": admin.get("session_token", "")}

    members = client.get("/members", headers=admin_h)
    check("Admin view members", members.status_code == 200, str(members.status_code))

    recruiters = client.get("/recruiters", headers=admin_h)
    check("Admin view recruiters", recruiters.status_code == 200, str(recruiters.status_code))

    rc = client.post(
        "/recruiters",
        headers=admin_h,
        json={
            "username": "recruiter_tmp_smoke",
            "email": "recruiter_tmp_smoke@local.dev",
            "password": "recruiter123",
            "full_name": "Recruiter Smoke",
            "role_name": "Recruiter",
        },
    )
    recruiter_id = rc.json().get("recruiter_user_id") if rc.status_code == 200 else None
    check("Admin create recruiter", rc.status_code == 200, str(rc.status_code))

    if recruiter_id:
        rd = client.delete(f"/recruiters/{recruiter_id}", headers=admin_h)
        check("Admin delete recruiter", rd.status_code == 200, str(rd.status_code))

    code, student = login("student4", "hash4")
    check("Student login", code == 200, str(code))
    student_h = {"X-Session-Token": student.get("session_token", "")}

    me = client.get("/me/student", headers=student_h)
    check("Student fetch own member id", me.status_code == 200, str(me.status_code))

    ineligible = client.post("/applications", headers=student_h, json={"job_id": 15, "status": "Applied"})
    check("Student blocked by eligibility", ineligible.status_code == 403, str(ineligible.status_code))

    student_apps = client.get("/applications", headers=student_h)
    check("Student view own applications", student_apps.status_code == 200, str(student_apps.status_code))

    code, recruiter = login("recruiter1", "hash41")
    check("Recruiter login", code == 200, str(code))
    recruiter_h = {"X-Session-Token": recruiter.get("session_token", "")}

    recruiter_apps = client.get("/applications", headers=recruiter_h)
    check("Recruiter view candidates", recruiter_apps.status_code == 200, str(recruiter_apps.status_code))

    if recruiter_apps.status_code == 200 and recruiter_apps.json():
        app_id = recruiter_apps.json()[0]["application_id"]
        update = client.patch(f"/applications/{app_id}", headers=recruiter_h, json={"status": "Shortlisted"})
        check("Recruiter update candidate status", update.status_code == 200, str(update.status_code))

    code, cds = login("cds1", "hash56")
    check("CDS login", code == 200, str(code))
    cds_h = {"X-Session-Token": cds.get("session_token", "")}

    cds_members = client.get("/members", headers=cds_h)
    check("CDS view members", cds_members.status_code == 200, str(cds_members.status_code))

    cds_groups = client.get("/groups", headers=cds_h)
    check("CDS view groups", cds_groups.status_code == 200, str(cds_groups.status_code))

    cds_create_job = client.post("/jobs", headers=cds_h, json={"company_id": 1, "title": "X", "min_cpi": 6.0})
    check("CDS blocked from editing", cds_create_job.status_code == 403, str(cds_create_job.status_code))

print("Smoke RBAC checks complete.")
