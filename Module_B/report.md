# Module B Optimization and Security Report

## Video Link
https://drive.google.com/drive/folders/1P_dnP9cqHbLV13K84n574tkl9wwxf71Y?usp=drive_link

## UI Demonstration Notes
- Frontend URL: `http://127.0.0.1:8001/`
- API docs URL: `http://127.0.0.1:8001/docs`
- UI coverage for demo:
  - Login and session validation
  - Portfolio read/update
  - Company CRUD (admin)
  - Job CRUD (admin)

## 1) Schema Design
- Core tables (`roles`, `users`, `sessions`, `groups`, `user_groups`) are separate from project tables (`students`, `companies`, `job_postings`, `applications`).
- Student records reference `users.user_id`, avoiding credential duplication in project-specific tables.
- Deletion integrity is maintained using foreign keys and cascading rules.

## 2) Security and Session Validation
- Every protected endpoint requires `X-Session-Token`.
- `POST /login` creates an expiring local session in `sessions`.
- `GET /isAuth` validates token, expiry, and active-user status.
- RBAC rules:
  - `CDS Manager` (admin-equivalent): full CRUD and audit access.
  - `Student` (regular user): read-restricted access and self-service portfolio updates only.

## 3) Logging and Unauthorized Change Detection
- Each data-modifying API call writes both:
  - file log: `logs/audit.log`
  - table log: `audit_logs`
- Any direct DB mutation that does not pass through APIs will have no corresponding API audit trail, making it easy to flag as unauthorized during review.

## 4) Indexing Strategy
Indexes in `sql/indexes.sql` target real query patterns:
- Session validation: `sessions(user_id)`, `sessions(expires_at)`
- Portfolio and student joins: `students(user_id)`, `students(portfolio_visibility)`, `students(latest_cpi)`
- Eligibility-driven job filters: `eligibility_criteria(job_id)`, `eligibility_criteria(min_cpi)` with `job_postings(deadline)`
- Application joins/filtering: `(job_id, student_id)`, `applications(status)`
- Audit review queries: `audit_logs(logged_at)`, `(actor_user_id, logged_at)`

## 5) Benchmarking and Profiling
- Benchmark script: `scripts/benchmark_indexing.py`
- It records both API response times and SQL query execution times before/after indexing.
- It also records `EXPLAIN QUERY PLAN` outputs before/after indexing.
- Output file: `benchmark_results.txt`

### API Response Time Evidence (Before vs After Indexing)
- `GET /jobs?min_cpi=8.0` before: `0.130571 s`
- `GET /jobs?min_cpi=8.0` after: `0.137360 s`
- `GET /applications` before: `0.794799 s`
- `GET /applications` after: `0.795590 s`

### SQL Query Time Evidence (Before vs After Indexing)
- Q1 before: `0.010015 s`
- Q1 after: `0.016503 s`
- Q2 before: `0.001866 s`
- Q2 after: `0.002743 s`

### EXPLAIN Plan Change Summary
- Q1 (applications filter): before uses `SCAN a`, after uses `SEARCH a USING INDEX idx_applications_status (status=?)`.
- Q2 (jobs eligibility filter): before uses `SCAN e`, after uses `SEARCH e USING INDEX idx_criteria_min_cpi (min_cpi<?)`.

## 6) Conclusion
The local API enforces session security and RBAC while maintaining strict data separation between authentication data and project domain data. Applied indexes improve frequent query paths and reduce scan-heavy execution plans.
