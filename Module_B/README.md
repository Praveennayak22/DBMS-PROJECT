# Module B - Local API, RBAC, and SQL Optimization

## Features Implemented
- Local DB with Assignment 1-aligned core tables: `roles`, `users`, `sessions`, `groups`, `user_groups`, and `audit_logs`.
- Project tables aligned to Assignment 1: `students`, `companies`, `job_postings`, `applications`.
- Session-validated API endpoints with role enforcement (`CDS Manager` as admin-equivalent and `Student` as regular user).
- Student portfolio access control (`private`, `group`, `public`).
- CRUD APIs for companies and jobs.
- Audit logging for every write operation to both `logs/audit.log` and `audit_logs` table, including denied admin-only access attempts.
- SQL index strategy in `sql/indexes.sql` targeting query predicates and joins.
- Performance benchmark script and EXPLAIN plan capture in `scripts/benchmark_indexing.py`.

## Run locally
```bash
pip install -r requirements.txt
$env:MODULE_B_DB_DSN="postgresql://postgres:postgres@localhost:5432/module_b"
python -m uvicorn app.main:app --reload --port 8001 --app-dir Module_B
```

If you run from inside `Module_B`, you can use:
```bash
$env:MODULE_B_DB_DSN="postgresql://postgres:postgres@localhost:5432/module_b"
python -m uvicorn app.main:app --reload --port 8001
```

For Command Prompt (`cmd.exe`), use:
```bash
set MODULE_B_DB_DSN=postgresql://postgres:postgres@localhost:5432/module_b
```

### PostgreSQL notes
- Module B now uses PostgreSQL as its runtime database.
- Create the database once before first run, for example:
```bash
psql -U postgres -c "CREATE DATABASE module_b;"
```
- Override credentials/host by setting `MODULE_B_DB_DSN`.

## Web UI
- Open `http://127.0.0.1:8001/` for the local frontend (login, portfolio, company/job CRUD views).
- Swagger remains available at `http://127.0.0.1:8001/docs`.

## Seed credentials
- Admin-equivalent (`CDS Manager`): `admin` / `admin123`
- Students: `student1` to `student30` with passwords `hash1` to `hash30`
- Alumni: `alumni1` to `alumni10` with passwords `hash31` to `hash40`
- Recruiters: `recruiter1` to `recruiter15` with passwords `hash41` to `hash55`
- CDS Team: `cds1` to `cds5` with passwords `hash56` to `hash60`
- CDS Manager: `cdsmanager1` / `hash61`

## Key endpoints
- `POST /login`
- `GET /isAuth`
- `GET /portfolio/{member_id}`
- `PATCH /portfolio/{member_id}`
- `POST/GET/PATCH/DELETE /companies`
- `POST/GET/PATCH/DELETE /jobs`
- `POST/GET/PATCH/DELETE /applications`
- `GET /audit-logs` (CDS Manager / admin-equivalent only)

## Benchmark
```bash
python scripts/benchmark_indexing.py
```
This writes `benchmark_results.txt` with:
- API response times before/after indexing
- SQL query times before/after indexing
- EXPLAIN QUERY PLAN snapshots before/after indexing
