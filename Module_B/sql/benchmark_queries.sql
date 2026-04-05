-- Query profiling examples (run before and after applying indexes.sql)

EXPLAIN QUERY PLAN
SELECT a.application_id, a.status, m.member_id, u.full_name, j.title
FROM applications a
JOIN members m ON a.member_id = m.member_id
JOIN users u ON m.user_id = u.user_id
JOIN job_postings j ON a.job_id = j.job_id
WHERE a.status = 'applied'
ORDER BY a.applied_at DESC;

EXPLAIN QUERY PLAN
SELECT j.job_id, j.title, c.company_name
FROM job_postings j
JOIN companies c ON j.company_id = c.company_id
WHERE j.min_cpi <= 8.0
ORDER BY j.deadline ASC;

EXPLAIN QUERY PLAN
SELECT log_id, actor_user_id, action, table_name, logged_at
FROM audit_logs
WHERE actor_user_id = 1
ORDER BY logged_at DESC
LIMIT 100;
