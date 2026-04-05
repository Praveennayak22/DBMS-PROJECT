import sqlite3
import statistics
import time
import os
import sys
import gc
import math
from pathlib import Path

from fastapi.testclient import TestClient

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "benchmark_module_b.db"
SCHEMA_PATH = BASE_DIR / "sql" / "init_schema.sql"
INDEX_PATH = BASE_DIR / "sql" / "indexes.sql"
OUTPUT_PATH = BASE_DIR / "benchmark_results.txt"

os.environ["MODULE_B_DB_PATH"] = str(DB_PATH)
os.environ["MODULE_B_APPLY_INDEXES"] = "0"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.main import app


def reset_db(apply_indexes: bool):
    if DB_PATH.exists():
        _safe_unlink(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    if apply_indexes:
        conn.executescript(INDEX_PATH.read_text(encoding="utf-8"))

    # Ensure baseline FK parents exist for synthetic benchmark data.
    conn.execute(
        "INSERT OR IGNORE INTO roles(role_id, role_name, description) VALUES (200, 'Benchmark Student', 'Benchmark role')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO users(user_id, username, email, password_hash, role_id, full_name, is_active) VALUES (1, 'company_seed', 'company_seed@local.dev', 'pw', 200, 'Company Seed', 1)"
    )
    conn.execute(
        "INSERT OR IGNORE INTO companies(company_id, user_id, company_name) VALUES (1, 1, 'Benchmark Co')"
    )

    # Seed larger volume for meaningful benchmark.
    users = [(f"user{i}", f"user{i}@local.dev", "pw", f"User {i}") for i in range(3, 5003)]
    conn.executemany(
        "INSERT OR IGNORE INTO users(username, email, password_hash, role_id, full_name, is_active) VALUES (?, ?, ?, 200, ?, 1)",
        users,
    )

    benchmark_user_ids = [
        row[0]
        for row in conn.execute(
            "SELECT user_id FROM users WHERE username LIKE 'user%' ORDER BY user_id"
        ).fetchall()
    ]
    students = [
        (uid, 7.0 + (uid % 10) * 0.1, "B.Tech", "Computer Science", 2026, 0, "bio", "python,sql")
        for uid in benchmark_user_ids
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO students(user_id, latest_cpi, program, discipline, graduating_year, active_backlogs, bio, skills, portfolio_visibility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'group')",
        students,
    )

    jobs = [(f"Role {i}", 6.0 + (i % 4) * 0.5) for i in range(3, 1003)]
    conn.executemany(
        "INSERT INTO job_postings(company_id, designation, location, deadline, posted_date) VALUES (1, ?, 'Remote', '2026-12-31', '2026-01-01')",
        [(title,) for title, _ in jobs],
    )

    created_jobs = conn.execute(
        "SELECT job_id FROM job_postings WHERE designation LIKE 'Role %' ORDER BY job_id LIMIT ?",
        (len(jobs),),
    ).fetchall()
    criteria = [(row[0], jobs[idx][1], 0, "CSE,IT", 2026, "") for idx, row in enumerate(created_jobs)]
    conn.executemany(
        """
        INSERT INTO eligibility_criteria(job_id, min_cpi, allowed_backlogs, eligible_programs, eligible_year, additional_requirements)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        criteria,
    )

    # Applications: 20k rows
    student_ids = [r[0] for r in conn.execute("SELECT student_id FROM students LIMIT 2000").fetchall()]
    job_ids = [r[0] for r in conn.execute("SELECT job_id FROM job_postings LIMIT 500").fetchall()]
    count = 0
    for s in student_ids:
        for j in job_ids[:10]:
            conn.execute(
                "INSERT OR IGNORE INTO applications(job_id, student_id, status) VALUES (?, ?, ?)",
                (j, s, "applied" if (count % 3) else "shortlisted"),
            )
            count += 1
            if count >= 20000:
                break
        if count >= 20000:
            break

    conn.commit()
    conn.close()


def _safe_unlink(path: Path, retries: int = 10, delay_s: float = 0.15):
    for attempt in range(retries):
        try:
            path.unlink()
            return
        except PermissionError:
            gc.collect()
            time.sleep(delay_s)
    # Final attempt should raise useful error if still locked.
    path.unlink()


def timed(conn: sqlite3.Connection, query: str, params=(), runs: int = 10):
    samples = []
    for _ in range(runs):
        start = time.perf_counter()
        conn.execute(query, params).fetchall()
        samples.append(time.perf_counter() - start)
    return statistics.mean(samples)


def explain(conn: sqlite3.Connection, query: str, params=()):
    return conn.execute("EXPLAIN QUERY PLAN " + query, params).fetchall()


def timed_api(client: TestClient, method: str, path: str, runs: int = 10, headers=None, params=None, json=None):
    samples = []
    for _ in range(runs):
        start = time.perf_counter()
        response = client.request(method, path, headers=headers, params=params, json=json)
        samples.append(time.perf_counter() - start)
        if response.status_code >= 400:
            raise RuntimeError(f"API {method} {path} failed: {response.status_code} {response.text}")
    return statistics.mean(samples)


def benchmark_api_endpoints():
    with TestClient(app) as client:
        login = client.post("/login", json={"username": "admin", "password": "admin123"})
        if login.status_code >= 400:
            raise RuntimeError(f"Login failed during API benchmark: {login.status_code} {login.text}")

        token = login.json()["session_token"]
        headers = {"X-Session-Token": token}

        jobs_avg = timed_api(client, "GET", "/jobs", headers=headers, params={"min_cpi": 8.0})
        applications_avg = timed_api(client, "GET", "/applications", headers=headers)

        return {
            "api_jobs_avg_s": jobs_avg,
            "api_applications_avg_s": applications_avg,
        }


def _fmt_timing(value: float) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "skipped"
    return f"{value:.6f}"


def run_case(apply_indexes: bool):
    reset_db(apply_indexes=apply_indexes)
    conn = sqlite3.connect(DB_PATH)

    q1 = """
    SELECT a.application_id, a.status, s.student_id, u.full_name, j.designation
    FROM applications a
    JOIN students s ON a.student_id = s.student_id
    JOIN users u ON s.user_id = u.user_id
    JOIN job_postings j ON a.job_id = j.job_id
    WHERE a.status = ?
    ORDER BY a.applied_at DESC
    LIMIT 500
    """
    q2 = """
    SELECT j.job_id, j.designation, c.company_name
    FROM job_postings j
    JOIN companies c ON j.company_id = c.company_id
    JOIN eligibility_criteria e ON e.job_id = j.job_id
    WHERE e.min_cpi <= ?
    ORDER BY j.deadline ASC
    """

    if os.getenv("MODULE_B_BENCHMARK_API", "0") == "1":
        api_result = benchmark_api_endpoints()
    else:
        api_result = {
            "api_jobs_avg_s": float("nan"),
            "api_applications_avg_s": float("nan"),
        }

    result = {
        "indexed": apply_indexes,
        "q1_avg_s": timed(conn, q1, ("applied",)),
        "q2_avg_s": timed(conn, q2, (8.0,)),
        "q1_explain": explain(conn, q1, ("applied",)),
        "q2_explain": explain(conn, q2, (8.0,)),
        "api_jobs_avg_s": api_result["api_jobs_avg_s"],
        "api_applications_avg_s": api_result["api_applications_avg_s"],
    }
    conn.close()
    return result


def main():
    print("Running benchmark with indexes...")
    before = run_case(apply_indexes=False)
    print("Running benchmark without indexes...")
    after = run_case(apply_indexes=True)

    lines = []
    lines.append("Module B Index Benchmark\n")
    lines.append("API timings (seconds):")
    lines.append(f"GET /jobs?min_cpi=8.0 after  index avg (s): {_fmt_timing(before['api_jobs_avg_s'])}")
    lines.append(f"GET /jobs?min_cpi=8.0 before index avg (s): {_fmt_timing(after['api_jobs_avg_s'])}")
    lines.append(f"GET /applications after  index avg (s): {_fmt_timing(before['api_applications_avg_s'])}")
    lines.append(f"GET /applications before index avg (s): {_fmt_timing(after['api_applications_avg_s'])}\n")

    lines.append("SQL timings (seconds):")
    lines.append(f"Q1 after  index avg (s): {before['q1_avg_s']:.6f}")
    lines.append(f"Q1 before index avg (s): {after['q1_avg_s']:.6f}")
    lines.append(f"Q2 after  index avg (s): {before['q2_avg_s']:.6f}")
    lines.append(f"Q2 before index avg (s): {after['q2_avg_s']:.6f}\n")

    lines.append("Q1 EXPLAIN after:")
    for row in before["q1_explain"]:
        lines.append(str(row))

    lines.append("\nQ1 EXPLAIN before:")
    for row in after["q1_explain"]:
        lines.append(str(row))

    lines.append("\nQ2 EXPLAIN after:")
    for row in before["q2_explain"]:
        lines.append(str(row))

    lines.append("\nQ2 EXPLAIN before:")
    for row in after["q2_explain"]:
        lines.append(str(row))

    OUTPUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print("Saved:", OUTPUT_PATH)
    print("Benchmark DB:", DB_PATH)


if __name__ == "__main__":
    main()
