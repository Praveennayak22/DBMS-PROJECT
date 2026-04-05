-- Targeting frequent WHERE, JOIN, ORDER BY patterns in API queries.

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

CREATE INDEX IF NOT EXISTS idx_students_user_id ON students(user_id);
CREATE INDEX IF NOT EXISTS idx_students_visibility ON students(portfolio_visibility);
CREATE INDEX IF NOT EXISTS idx_students_cpi ON students(latest_cpi);

CREATE INDEX IF NOT EXISTS idx_companies_user_id ON companies(user_id);

CREATE INDEX IF NOT EXISTS idx_jobs_company_id ON job_postings(company_id);
CREATE INDEX IF NOT EXISTS idx_jobs_deadline ON job_postings(deadline);
CREATE INDEX IF NOT EXISTS idx_jobs_designation ON job_postings(designation);

CREATE INDEX IF NOT EXISTS idx_criteria_job_id ON eligibility_criteria(job_id);
CREATE INDEX IF NOT EXISTS idx_criteria_min_cpi ON eligibility_criteria(min_cpi);

CREATE INDEX IF NOT EXISTS idx_applications_job_student ON applications(job_id, student_id);
CREATE INDEX IF NOT EXISTS idx_applications_status ON applications(status);

CREATE INDEX IF NOT EXISTS idx_audit_logged_at ON audit_logs(logged_at);
CREATE INDEX IF NOT EXISTS idx_audit_actor_time ON audit_logs(actor_user_id, logged_at);
