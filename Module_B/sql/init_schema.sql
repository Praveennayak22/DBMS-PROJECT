PRAGMA foreign_keys = ON;

-- Core auth and RBAC tables
CREATE TABLE IF NOT EXISTS roles (
    role_id INTEGER PRIMARY KEY,
    role_name TEXT UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role_id INTEGER NOT NULL,
    is_verified INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    full_name TEXT NOT NULL,
    contact_number TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','INACTIVE','SUSPENDED')),
    is_active INTEGER NOT NULL DEFAULT 1,
    FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

CREATE TABLE IF NOT EXISTS user_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    ip_address TEXT,
    start_time TEXT NOT NULL,
    end_time TEXT,
    device_info TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    CHECK (end_time IS NULL OR end_time > start_time)
);

CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS user_groups (
    user_id INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    PRIMARY KEY (user_id, group_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES groups(group_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sessions (
    session_token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- Project domain tables aligned to Task 1 shape
CREATE TABLE IF NOT EXISTS alumni_user (
    alumni_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER UNIQUE NOT NULL,
    grad_year INTEGER NOT NULL,
    current_company TEXT,
    placement_history TEXT,
    designation TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    CHECK (grad_year BETWEEN 1900 AND 2100)
);

CREATE TABLE IF NOT EXISTS students (
    student_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER UNIQUE NOT NULL,
    latest_cpi REAL,
    program TEXT,
    discipline TEXT,
    graduating_year INTEGER,
    active_backlogs INTEGER NOT NULL DEFAULT 0,
    gender TEXT CHECK(gender IN ('Male','Female','Other')),
    tenth_percent REAL,
    tenth_passout_year INTEGER,
    twelfth_percent REAL,
    twelfth_passout_year INTEGER,
    bio TEXT,
    skills TEXT,
    portfolio_visibility TEXT NOT NULL DEFAULT 'private' CHECK(portfolio_visibility IN ('private','group','public')),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    CHECK (latest_cpi IS NULL OR (latest_cpi >= 0 AND latest_cpi <= 10)),
    CHECK (active_backlogs >= 0),
    CHECK (tenth_percent IS NULL OR (tenth_percent >= 0 AND tenth_percent <= 100)),
    CHECK (twelfth_percent IS NULL OR (twelfth_percent >= 0 AND twelfth_percent <= 100))
);

CREATE TABLE IF NOT EXISTS resumes (
    resume_id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER NOT NULL,
    resume_label TEXT NOT NULL,
    file_url TEXT NOT NULL,
    ats_score REAL,
    is_verified INTEGER NOT NULL DEFAULT 0,
    uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    CHECK (ats_score IS NULL OR (ats_score >= 0 AND ats_score <= 100))
);

CREATE TABLE IF NOT EXISTS companies (
    company_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER UNIQUE NOT NULL,
    company_name TEXT NOT NULL,
    industry_sector TEXT,
    type_of_organization TEXT,
    hiring_history TEXT,
    company_description TEXT,
    website_url TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    CHECK (company_name <> '')
);

CREATE TABLE IF NOT EXISTS job_postings (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    designation TEXT,
    description TEXT,
    location TEXT,
    stipend REAL,
    job_type TEXT,
    deadline TEXT,
    posted_date TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE CASCADE,
    CHECK (stipend IS NULL OR stipend >= 0)
);

CREATE TABLE IF NOT EXISTS eligibility_criteria (
    criteria_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER UNIQUE NOT NULL,
    min_cpi REAL,
    allowed_backlogs INTEGER,
    eligible_programs TEXT,
    eligible_year INTEGER,
    additional_requirements TEXT,
    FOREIGN KEY (job_id) REFERENCES job_postings(job_id) ON DELETE CASCADE,
    CHECK (min_cpi IS NULL OR (min_cpi >= 0 AND min_cpi <= 10)),
    CHECK (allowed_backlogs IS NULL OR allowed_backlogs >= 0)
);

CREATE TABLE IF NOT EXISTS applications (
    application_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    student_id INTEGER NOT NULL,
    applied_at TEXT,
    status TEXT,
    FOREIGN KEY (job_id) REFERENCES job_postings(job_id) ON DELETE CASCADE,
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
    UNIQUE(job_id, student_id)
);

CREATE TABLE IF NOT EXISTS job_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    event_name TEXT,
    event_type TEXT CHECK(event_type IN ('PPT','OA','GD','Interview')),
    event_date TEXT,
    event_time TEXT,
    start_time TEXT,
    end_time TEXT,
    mode TEXT CHECK(mode IN ('ONLINE','OFFLINE','HYBRID')),
    venue TEXT,
    description TEXT,
    FOREIGN KEY (job_id) REFERENCES job_postings(job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS venue_booking (
    booking_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    room_number TEXT,
    equipment_needed TEXT,
    academic_block TEXT,
    FOREIGN KEY (event_id) REFERENCES job_events(event_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS interviews (
    interview_id INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    meeting_link TEXT,
    platform TEXT CHECK(platform IN ('Zoom','Microsoft Teams','Google Meet')),
    FOREIGN KEY (application_id) REFERENCES applications(application_id) ON DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES job_events(event_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS question_bank (
    q_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER,
    question_text TEXT NOT NULL,
    type TEXT CHECK(type IN ('MCQ','Coding','Subjective')),
    difficulty TEXT CHECK(difficulty IN ('Easy','Medium','Hard')),
    alumni_id INTEGER,
    FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE SET NULL,
    FOREIGN KEY (alumni_id) REFERENCES alumni_user(alumni_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS prep_pages (
    page_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER,
    process_details TEXT,
    senior_feedback TEXT,
    FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS placement_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch INTEGER,
    placed_count INTEGER,
    avg_package REAL,
    highest_package REAL,
    generated_at TEXT,
    CHECK (placed_count IS NULL OR placed_count >= 0),
    CHECK (avg_package IS NULL OR avg_package >= 0),
    CHECK (highest_package IS NULL OR avg_package IS NULL OR highest_package >= avg_package)
);

CREATE TABLE IF NOT EXISTS penalties (
    penalty_id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER NOT NULL,
    reason TEXT NOT NULL,
    penalty_type TEXT CHECK(penalty_type IN ('Warning','Debarment')),
    issued_at TEXT,
    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cds_training_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    description TEXT,
    start_time TEXT,
    end_time TEXT,
    mode TEXT CHECK(mode IN ('ONLINE','OFFLINE','HYBRID')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (end_time IS NULL OR start_time IS NULL OR end_time > start_time)
);

CREATE TABLE IF NOT EXISTS alumni_training_map (
    alumni_id INTEGER,
    session_id INTEGER,
    PRIMARY KEY (alumni_id, session_id),
    FOREIGN KEY (alumni_id) REFERENCES alumni_user(alumni_id) ON DELETE CASCADE,
    FOREIGN KEY (session_id) REFERENCES cds_training_sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS audit_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_user_id INTEGER,
    action TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_id TEXT,
    request_path TEXT,
    status TEXT NOT NULL,
    logged_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (actor_user_id) REFERENCES users(user_id)
);

-- Seed RBAC roles for Module B API auth flow
INSERT OR IGNORE INTO roles (role_id, role_name, description)
VALUES
    (1, 'Student', 'Current student eligible for placements'),
    (2, 'Recruiter', 'Company representative posting jobs and hiring students'),
    (3, 'CDS Team', 'Placement cell coordinators managing drives'),
    (4, 'CDS Manager', 'Head of Career Development and Placement Services'),
    (5, 'Alumni', 'Graduated student providing referrals and guidance');

INSERT OR IGNORE INTO users (
    user_id, username, email, password_hash, role_id, is_verified, full_name, contact_number, status, is_active
)
VALUES
    (1,'student1','aarav.patel@gmail.com','hash1',1,true,'Aarav Patel','+919876540001','ACTIVE',true),
    (2,'student2','vivaan.shah@gmail.com','hash2',1,true,'Vivaan Shah','+919876540002','ACTIVE',true),
    (3,'student3','aditya.mehta@gmail.com','hash3',1,true,'Aditya Mehta','+919876540003','ACTIVE',true),
    (4,'student4','krishna.patel@gmail.com','hash4',1,true,'Krishna Patel','+919876540004','ACTIVE',true),
    (5,'student5','ishaan.shah@gmail.com','hash5',1,true,'Ishaan Shah','+919876540005','ACTIVE',true),
    (6,'student6','rohan.desai@gmail.com','hash6',1,true,'Rohan Desai','+919876540006','ACTIVE',true),
    (7,'student7','arjun.joshi@gmail.com','hash7',1,true,'Arjun Joshi','+919876540007','ACTIVE',true),
    (8,'student8','dhruv.patel@gmail.com','hash8',1,true,'Dhruv Patel','+919876540008','ACTIVE',true),
    (9,'student9','kabir.shah@gmail.com','hash9',1,true,'Kabir Shah','+919876540009','ACTIVE',true),
    (10,'student10','dev.patel@gmail.com','hash10',1,true,'Dev Patel','+919876540010','ACTIVE',true),
    (11,'student11','jay.shah@gmail.com','hash11',1,true,'Jay Shah','+919876540011','ACTIVE',true),
    (12,'student12','neel.mehta@gmail.com','hash12',1,true,'Neel Mehta','+919876540012','ACTIVE',true),
    (13,'student13','om.patel@gmail.com','hash13',1,true,'Om Patel','+919876540013','ACTIVE',true),
    (14,'student14','raj.shah@gmail.com','hash14',1,true,'Raj Shah','+919876540014','ACTIVE',true),
    (15,'student15','karan.patel@gmail.com','hash15',1,true,'Karan Patel','+919876540015','ACTIVE',true),
    (16,'student16','aryan.shah@gmail.com','hash16',1,true,'Aryan Shah','+919876540016','ACTIVE',true),
    (17,'student17','harsh.patel@gmail.com','hash17',1,true,'Harsh Patel','+919876540017','ACTIVE',true),
    (18,'student18','yash.shah@gmail.com','hash18',1,true,'Yash Shah','+919876540018','ACTIVE',true),
    (19,'student19','meet.patel@gmail.com','hash19',1,true,'Meet Patel','+919876540019','ACTIVE',true),
    (20,'student20','parth.shah@gmail.com','hash20',1,true,'Parth Shah','+919876540020','ACTIVE',true),
    (21,'student21','nirav.patel@gmail.com','hash21',1,true,'Nirav Patel','+919876540021','ACTIVE',true),
    (22,'student22','vraj.shah@gmail.com','hash22',1,true,'Vraj Shah','+919876540022','ACTIVE',true),
    (23,'student23','sahil.patel@gmail.com','hash23',1,true,'Sahil Patel','+919876540023','ACTIVE',true),
    (24,'student24','manav.shah@gmail.com','hash24',1,true,'Manav Shah','+919876540024','ACTIVE',true),
    (25,'student25','tirth.patel@gmail.com','hash25',1,true,'Tirth Patel','+919876540025','ACTIVE',true),
    (26,'student26','kunal.shah@gmail.com','hash26',1,true,'Kunal Shah','+919876540026','ACTIVE',true),
    (27,'student27','jatin.patel@gmail.com','hash27',1,true,'Jatin Patel','+919876540027','ACTIVE',true),
    (28,'student28','deep.shah@gmail.com','hash28',1,true,'Deep Shah','+919876540028','ACTIVE',true),
    (29,'student29','hiren.patel@gmail.com','hash29',1,true,'Hiren Patel','+919876540029','ACTIVE',true),
    (30,'student30','rahul.shah@gmail.com','hash30',1,true,'Rahul Shah','+919876540030','ACTIVE',true),
    (31,'alumni1','amit.shah@gmail.com','hash31',5,true,'Amit Shah','+919876540031','ACTIVE',true),
    (32,'alumni2','rahul.mehta@gmail.com','hash32',5,true,'Rahul Mehta','+919876540032','ACTIVE',true),
    (33,'alumni3','suresh.patel@gmail.com','hash33',5,true,'Suresh Patel','+919876540033','ACTIVE',true),
    (34,'alumni4','mahesh.shah@gmail.com','hash34',5,true,'Mahesh Shah','+919876540034','ACTIVE',true),
    (35,'alumni5','rajesh.patel@gmail.com','hash35',5,true,'Rajesh Patel','+919876540035','ACTIVE',true),
    (36,'alumni6','anil.shah@gmail.com','hash36',5,true,'Anil Shah','+919876540036','ACTIVE',true),
    (37,'alumni7','sunil.patel@gmail.com','hash37',5,true,'Sunil Patel','+919876540037','ACTIVE',true),
    (38,'alumni8','rakesh.shah@gmail.com','hash38',5,true,'Rakesh Shah','+919876540038','ACTIVE',true),
    (39,'alumni9','mukesh.patel@gmail.com','hash39',5,true,'Mukesh Patel','+919876540039','ACTIVE',true),
    (40,'alumni10','naresh.shah@gmail.com','hash40',5,true,'Naresh Shah','+919876540040','ACTIVE',true),
    (41,'recruiter1','hr.tcs@gmail.com','hash41',2,true,'Priya Sharma','+919876540041','ACTIVE',true),
    (42,'recruiter2','hr.infosys@gmail.com','hash42',2,true,'Anjali Verma','+919876540042','ACTIVE',true),
    (43,'recruiter3','hr.google@gmail.com','hash43',2,true,'Sneha Iyer','+919876540043','ACTIVE',true),
    (44,'recruiter4','hr.microsoft@gmail.com','hash44',2,true,'Riya Kapoor','+919876540044','ACTIVE',true),
    (45,'recruiter5','hr.amazon@gmail.com','hash45',2,true,'Pooja Singh','+919876540045','ACTIVE',true),
    (46,'recruiter6','hr.meta@gmail.com','hash46',2,true,'Neha Agarwal','+919876540046','ACTIVE',true),
    (47,'recruiter7','hr.ibm@gmail.com','hash47',2,true,'Kavya Nair','+919876540047','ACTIVE',true),
    (48,'recruiter8','hr.oracle@gmail.com','hash48',2,true,'Isha Menon','+919876540048','ACTIVE',true),
    (49,'recruiter9','hr.intel@gmail.com','hash49',2,true,'Megha Pillai','+919876540049','ACTIVE',true),
    (50,'recruiter10','hr.nvidia@gmail.com','hash50',2,true,'Nisha Reddy','+919876540050','ACTIVE',true),
    (51,'recruiter11','hr.adobe@gmail.com','hash51',2,true,'Ritu Sharma','+919876540051','ACTIVE',true),
    (52,'recruiter12','hr.salesforce@gmail.com','hash52',2,true,'Simran Kaur','+919876540052','ACTIVE',true),
    (53,'recruiter13','hr.uber@gmail.com','hash53',2,true,'Divya Jain','+919876540053','ACTIVE',true),
    (54,'recruiter14','hr.netflix@gmail.com','hash54',2,true,'Payal Gupta','+919876540054','ACTIVE',true),
    (55,'recruiter15','hr.accenture@gmail.com','hash55',2,true,'Komal Shah','+919876540055','ACTIVE',true),
    (56,'cds1','cds1@college.edu','hash56',3,true,'Placement Coordinator 1','+919876540056','ACTIVE',true),
    (57,'cds2','cds2@college.edu','hash57',3,true,'Placement Coordinator 2','+919876540057','ACTIVE',true),
    (58,'cds3','cds3@college.edu','hash58',3,true,'Placement Coordinator 3','+919876540058','ACTIVE',true),
    (59,'cds4','cds4@college.edu','hash59',3,true,'Placement Coordinator 4','+919876540059','ACTIVE',true),
    (60,'cds5','cds5@college.edu','hash60',3,true,'Placement Coordinator 5','+919876540060','ACTIVE',true),
    (61,'cdsmanager1','manager@college.edu','hash61',4,true,'Placement Manager','+919876540061','ACTIVE',true),
    (1000,'admin','admin@local.dev','admin123',4,true,'System Admin',NULL,'ACTIVE',true);

INSERT OR IGNORE INTO groups (group_id, group_name)
VALUES
    (1, 'placement-cell'),
    (2, 'student-group');

INSERT OR IGNORE INTO user_groups (user_id, group_id)
VALUES
    (1, 1),
    (2, 2),
    (3, 2),
    (56, 1),
    (57, 1),
    (58, 1),
    (59, 1),
    (60, 1),
    (61, 1);

INSERT OR IGNORE INTO students (
    student_id, user_id, latest_cpi, program, discipline, graduating_year,
    active_backlogs, gender, tenth_percent, tenth_passout_year,
    twelfth_percent, twelfth_passout_year, bio, skills, portfolio_visibility
)
VALUES
    (230001,1,9.12,'B.Tech','Computer Science',2026,0,'Male',95.2,2020,93.4,2022,'Interested in backend systems','python,sql,api','group'),
    (230002,2,8.75,'B.Tech','Computer Science',2026,0,'Male',92.3,2020,91.2,2022,'Data and systems enthusiast','python,sql,ml','private'),
    (230003,3,8.21,'B.Tech','Electronics',2026,0,'Male',90.4,2020,88.6,2022,'Embedded and IoT projects','c,sql,hardware','private'),
    (230004,4,7.95,'B.Tech','Mechanical',2026,0,'Male',88.3,2020,85.1,2022,'CAD and manufacturing learner','cad,python','private'),
    (230005,5,9.30,'B.Tech','Computer Science',2026,0,'Male',96.1,2020,94.2,2022,'Competitive programmer','cpp,dsa,sql','public'),
    (230006,6,8.66,'B.Tech','Civil',2026,0,'Male',91.4,2020,89.3,2022,'Civil analytics and planning','excel,python','private'),
    (230007,7,7.88,'B.Tech','Chemical',2026,2,'Male',87.2,2020,84.5,2022,'Process optimization projects','matlab,python','private'),
    (230008,8,8.91,'B.Tech','Electrical',2026,0,'Male',93.6,2020,90.2,2022,'Power systems and automation','python,simulink','group'),
    (230009,9,9.41,'B.Tech','Computer Science',2026,0,'Male',97.2,2020,95.8,2022,'Full-stack and ML projects','node,react,ml','public'),
    (230010,10,8.03,'B.Tech','Mathematics',2026,0,'Male',89.5,2020,87.3,2022,'Applied math and analytics','python,stats,sql','private'),
    (230011,11,7.76,'B.Tech','Physics',2026,0,'Male',85.3,2020,83.7,2022,'Simulation projects','python,numpy','private'),
    (230012,12,8.88,'B.Tech','Biotechnology',2026,0,'Male',92.7,2020,91.5,2022,'Bioinformatics learner','python,r,sql','private'),
    (230013,13,9.55,'B.Tech','Aerospace',2026,0,'Male',98.1,2020,96.2,2022,'Control systems projects','matlab,python','group'),
    (230014,14,8.46,'B.Tech','Production',2026,0,'Male',90.8,2020,88.4,2022,'Operations and productivity focus','excel,sql','private'),
    (230015,15,8.11,'B.Tech','Metallurgy',2026,0,'Male',88.9,2020,86.3,2022,'Materials analysis projects','python,lab','private'),
    (230016,16,7.64,'B.Tech','Artificial Intelligence',2026,2,'Male',84.3,2020,82.1,2022,'AI beginner portfolio','python,pytorch','private'),
    (230017,17,9.67,'B.Tech','Data Science',2026,0,'Male',97.9,2020,96.4,2022,'Advanced analytics and ML','python,ml,sql','public'),
    (230018,18,8.34,'B.Tech','Robotics',2026,0,'Male',91.2,2020,89.5,2022,'Robotics and automation','ros,python,cpp','group'),
    (230019,19,8.97,'B.Tech','Computer Science',2026,0,'Male',94.3,2020,92.6,2022,'Backend and distributed systems','go,sql,redis','group'),
    (230020,20,7.83,'B.Tech','Electronics',2026,0,'Male',86.8,2020,84.9,2022,'VLSI and embedded interests','verilog,c','private'),
    (230021,21,8.42,'B.Tech','Mechanical',2026,0,'Male',90.3,2020,88.2,2022,'Design and thermal systems','cad,ansys','private'),
    (230022,22,9.02,'B.Tech','Civil',2026,0,'Male',95.1,2020,93.7,2022,'Structural analysis projects','staad,excel','private'),
    (230023,23,8.16,'B.Tech','Chemical',2026,0,'Male',89.7,2020,87.4,2022,'Chemical process research','matlab,python','private'),
    (230024,24,8.69,'B.Tech','Electrical',2026,0,'Male',92.5,2020,90.1,2022,'Control and embedded work','c,python','group'),
    (230025,25,7.92,'B.Tech','Mathematics',2026,0,'Male',87.1,2020,85.3,2022,'Quant and analytics profile','python,r,sql','private'),
    (230026,26,8.81,'B.Tech','Physics',2026,0,'Male',93.8,2020,91.7,2022,'Computational physics','python,julia','private'),
    (230027,27,9.28,'B.Tech','Biotechnology',2026,0,'Male',96.4,2020,94.8,2022,'Genomics and analytics','python,bioinfo','group'),
    (230028,28,8.93,'B.Tech','Aerospace',2026,0,'Male',94.7,2020,92.9,2022,'Flight mechanics projects','matlab,cpp','private'),
    (230029,29,8.54,'B.Tech','Production',2026,0,'Male',91.6,2020,89.8,2022,'Manufacturing analytics','excel,sql,python','private'),
    (230030,30,9.74,'B.Tech','Artificial Intelligence',2026,0,'Male',98.6,2020,97.1,2022,'Research-oriented AI portfolio','python,ml,dl','public');

INSERT OR IGNORE INTO user_logs (log_id, user_id, action, ip_address, start_time, end_time, device_info)
VALUES
    (1,1,'Login','192.168.1.10','2026-01-10 09:00:00','2026-01-10 11:15:00','Chrome Windows 11'),
    (2,2,'Applied to Job','192.168.1.11','2026-01-11 10:05:00','2026-01-11 10:45:00','Firefox Windows 10'),
    (3,3,'Logout','192.168.1.12','2026-01-11 12:00:00','2026-01-11 12:10:00','Chrome Android'),
    (4,4,'Updated Resume','192.168.1.13','2026-01-12 14:30:00','2026-01-12 15:00:00','Edge Windows 11'),
    (5,5,'Login','192.168.1.14','2026-01-12 16:20:00','2026-01-12 18:45:00','Chrome MacOS'),
    (6,6,'Applied to Job','192.168.1.15','2026-01-13 09:10:00','2026-01-13 09:50:00','Safari iPhone'),
    (7,7,'Viewed Job Posting','192.168.1.16','2026-01-13 11:00:00','2026-01-13 11:25:00','Chrome Windows 11'),
    (8,8,'Login','192.168.1.17','2026-01-14 08:45:00','2026-01-14 10:00:00','Firefox Linux'),
    (9,9,'Logout','192.168.1.18','2026-01-14 13:30:00','2026-01-14 13:40:00','Chrome Android'),
    (10,41,'Posted Job','192.168.1.19','2026-01-15 10:15:00','2026-01-15 11:30:00','Chrome Windows 10'),
    (11,42,'Updated Job Posting','192.168.1.20','2026-01-15 12:00:00','2026-01-15 13:20:00','Edge Windows'),
    (12,56,'Scheduled Interview','192.168.1.21','2026-01-16 09:30:00','2026-01-16 10:45:00','Chrome Windows 11'),
    (13,57,'Booked Venue','192.168.1.22','2026-01-16 11:10:00','2026-01-16 11:50:00','Firefox Windows'),
    (14,61,'Generated Placement Report','192.168.1.23','2026-01-17 14:00:00','2026-01-17 16:30:00','Chrome MacOS'),
    (15,31,'Added Interview Question','192.168.1.24','2026-01-18 15:20:00','2026-01-18 16:00:00','Safari MacBook');

INSERT OR IGNORE INTO alumni_user (alumni_id, user_id, grad_year, current_company, placement_history, designation)
VALUES
    (1,31,2022,'Google','Placed at Google via campus placement','Software Engineer'),
    (2,32,2021,'Microsoft','Placed at Microsoft after internship conversion','Software Engineer'),
    (3,33,2020,'Amazon','Campus placement at Amazon','SDE II'),
    (4,34,2022,'Meta','Campus placement at Meta','Backend Developer'),
    (5,35,2019,'Adobe','Campus placement at Adobe','Software Engineer'),
    (6,36,2021,'Oracle','Campus placement at Oracle','Database Engineer'),
    (7,37,2020,'IBM','Campus placement at IBM','Systems Engineer'),
    (8,38,2019,'Intel','Campus placement at Intel','Hardware Engineer'),
    (9,39,2022,'Nvidia','Campus placement at Nvidia','GPU Software Engineer'),
    (10,40,2021,'Uber','Campus placement at Uber','Software Developer');

INSERT OR IGNORE INTO resumes (resume_id, student_id, resume_label, file_url, ats_score, is_verified, uploaded_at)
VALUES
    (1,230001,'Software Resume','https://drive.google.com/resume230001',92,1,'2026-01-05 10:00:00'),
    (2,230002,'Software Resume','https://drive.google.com/resume230002',88,1,'2026-01-05 10:15:00'),
    (3,230003,'Electronics Resume','https://drive.google.com/resume230003',81,1,'2026-01-06 09:30:00'),
    (4,230004,'Mechanical Resume','https://drive.google.com/resume230004',75,0,'2026-01-06 11:20:00'),
    (5,230005,'Software Resume','https://drive.google.com/resume230005',95,1,'2026-01-07 08:40:00'),
    (6,230006,'Civil Resume','https://drive.google.com/resume230006',82,1,'2026-01-07 09:10:00'),
    (7,230007,'Chemical Resume','https://drive.google.com/resume230007',72,0,'2026-01-07 12:00:00'),
    (8,230008,'Electrical Resume','https://drive.google.com/resume230008',87,1,'2026-01-08 10:30:00'),
    (9,230009,'Software Resume','https://drive.google.com/resume230009',96,1,'2026-01-08 14:00:00'),
    (10,230010,'Data Science Resume','https://drive.google.com/resume230010',84,1,'2026-01-09 09:00:00'),
    (11,230011,'Physics Resume','https://drive.google.com/resume230011',74,0,'2026-01-09 11:45:00'),
    (12,230012,'Biotech Resume','https://drive.google.com/resume230012',85,1,'2026-01-10 08:15:00'),
    (13,230013,'Aerospace Resume','https://drive.google.com/resume230013',91,1,'2026-01-10 10:50:00'),
    (14,230014,'Production Resume','https://drive.google.com/resume230014',80,1,'2026-01-11 09:35:00'),
    (15,230015,'Metallurgy Resume','https://drive.google.com/resume230015',78,0,'2026-01-11 13:10:00'),
    (16,230016,'AI Resume','https://drive.google.com/resume230016',86,1,'2026-01-12 08:55:00'),
    (17,230017,'Data Science Resume','https://drive.google.com/resume230017',94,1,'2026-01-12 10:30:00'),
    (18,230018,'Robotics Resume','https://drive.google.com/resume230018',83,1,'2026-01-13 09:20:00'),
    (19,230019,'Software Resume','https://drive.google.com/resume230019',89,1,'2026-01-13 11:40:00'),
    (20,230020,'Electronics Resume','https://drive.google.com/resume230020',77,0,'2026-01-14 10:00:00'),
    (21,230021,'Mechanical Resume','https://drive.google.com/resume230021',81,1,'2026-01-14 12:15:00'),
    (22,230022,'Civil Resume','https://drive.google.com/resume230022',90,1,'2026-01-15 08:45:00'),
    (23,230023,'Chemical Resume','https://drive.google.com/resume230023',79,1,'2026-01-15 11:00:00'),
    (24,230024,'Electrical Resume','https://drive.google.com/resume230024',88,1,'2026-01-16 09:10:00'),
    (25,230025,'Math Resume','https://drive.google.com/resume230025',76,0,'2026-01-16 12:30:00'),
    (26,230026,'Physics Resume','https://drive.google.com/resume230026',85,1,'2026-01-17 10:20:00'),
    (27,230027,'Biotech Resume','https://drive.google.com/resume230027',93,1,'2026-01-17 13:50:00'),
    (28,230028,'Aerospace Resume','https://drive.google.com/resume230028',87,1,'2026-01-18 08:35:00'),
    (29,230029,'Production Resume','https://drive.google.com/resume230029',82,1,'2026-01-18 11:25:00'),
    (30,230030,'AI Resume','https://drive.google.com/resume230030',96,1,'2026-01-19 09:40:00');

INSERT OR IGNORE INTO companies (
    company_id, user_id, company_name, industry_sector, type_of_organization,
    hiring_history, company_description, website_url
)
VALUES
    (1,41,'Tata Consultancy Services','IT Services','Service-Based','Hires 50+ students annually from campus','Leading Indian multinational IT services and consulting company','https://www.tcs.com'),
    (2,42,'Infosys','IT Services','Service-Based','Hires 40+ students annually','Global leader in next-generation digital services and consulting','https://www.infosys.com'),
    (3,43,'Google','Software','Product-Based','Hires 5-10 students annually','Global technology leader specializing in internet-related services','https://careers.google.com'),
    (4,44,'Microsoft','Software','Product-Based','Hires 8-12 students annually','Multinational technology corporation producing software and services','https://careers.microsoft.com'),
    (5,45,'Amazon','E-Commerce','Product-Based','Hires 10-15 students annually','World''s largest online retailer and cloud provider','https://www.amazon.jobs'),
    (6,46,'Meta','Social Media','Product-Based','Hires 5-8 students annually','Technology company focusing on social media and virtual reality','https://www.metacareers.com'),
    (7,47,'IBM','IT Services','Service-Based','Hires 20+ students annually','Global technology and consulting company','https://www.ibm.com/careers'),
    (8,48,'Oracle','Database Software','Product-Based','Hires 10+ students annually','Specializes in database software and enterprise solutions','https://www.oracle.com/careers'),
    (9,49,'Intel','Semiconductors','Product-Based','Hires 5-10 students annually','Designs and manufactures semiconductor chips','https://jobs.intel.com'),
    (10,50,'NVIDIA','Semiconductors','Product-Based','Hires 5+ students annually','Leader in GPU computing and AI technology','https://www.nvidia.com/careers'),
    (11,51,'Adobe','Software','Product-Based','Hires 5-7 students annually','Known for creative and multimedia software products','https://careers.adobe.com'),
    (12,52,'Salesforce','Cloud Computing','Product-Based','Hires 5-10 students annually','Cloud-based customer relationship management company','https://careers.salesforce.com'),
    (13,53,'Uber','Technology','Product-Based','Hires 3-6 students annually','Ride-hailing and technology platform company','https://www.uber.com/careers'),
    (14,54,'Netflix','Entertainment Technology','Product-Based','Hires 2-5 students annually','Streaming entertainment and production company','https://jobs.netflix.com'),
    (15,55,'Accenture','IT Services','Service-Based','Hires 30+ students annually','Global professional services company','https://www.accenture.com/careers');

INSERT OR IGNORE INTO job_postings (
    job_id, company_id, designation, description, location, stipend, job_type, deadline, posted_date
)
VALUES
    (1, 1, 'Software Engineer', 'Develop and maintain web applications using modern frameworks', 'Bangalore', 1200000, 'FULL_TIME', '2025-02-01', '2025-01-01'),
    (2, 2, 'Data Analyst', 'Analyze business data and generate insights', 'Mumbai', 800000, 'FULL_TIME', '2025-02-05', '2025-01-05'),
    (3, 3, 'Backend Developer', 'Build scalable backend APIs', 'Hyderabad', 1100000, 'FULL_TIME', '2025-02-10', '2025-01-10'),
    (4, 4, 'Frontend Developer', 'Create responsive user interfaces', 'Pune', 900000, 'FULL_TIME', '2025-02-12', '2025-01-12'),
    (5, 5, 'DevOps Engineer', 'Manage CI/CD pipelines and deployment', 'Chennai', 1300000, 'FULL_TIME', '2025-02-15', '2025-01-15'),
    (6, 6, 'AI Engineer', 'Develop machine learning models', 'Bangalore', 1500000, 'FULL_TIME', '2025-02-18', '2025-01-18'),
    (7, 7, 'Cloud Engineer', 'Manage cloud infrastructure', 'Hyderabad', 1400000, 'FULL_TIME', '2025-02-20', '2025-01-20'),
    (8, 8, 'Cyber Security Analyst', 'Ensure system security', 'Mumbai', 1000000, 'FULL_TIME', '2025-02-22', '2025-01-22'),
    (9, 9, 'Full Stack Developer', 'Work on frontend and backend', 'Pune', 1250000, 'FULL_TIME', '2025-02-25', '2025-01-25'),
    (10, 10, 'Mobile App Developer', 'Develop Android applications', 'Chennai', 950000, 'FULL_TIME', '2025-02-28', '2025-01-28'),
    (11, 11, 'QA Engineer', 'Test applications and ensure quality', 'Noida', 700000, 'FULL_TIME', '2025-03-01', '2025-02-01'),
    (12, 12, 'Business Analyst', 'Gather requirements and analyze', 'Gurgaon', 850000, 'FULL_TIME', '2025-03-05', '2025-02-05'),
    (13, 13, 'System Engineer', 'Maintain IT systems', 'Ahmedabad', 750000, 'FULL_TIME', '2025-03-08', '2025-02-08'),
    (14, 14, 'Database Administrator', 'Manage database systems', 'Bangalore', 1150000, 'FULL_TIME', '2025-03-10', '2025-02-10'),
    (15, 15, 'Machine Learning Engineer', 'Build ML pipelines', 'Hyderabad', 1600000, 'FULL_TIME', '2025-03-12', '2025-02-12');

INSERT OR IGNORE INTO eligibility_criteria (
    criteria_id, job_id, min_cpi, allowed_backlogs, eligible_programs, eligible_year, additional_requirements
)
VALUES
    (1, 1, 7.0, 0, 'CSE, IT', 2025, 'Strong programming skills required'),
    (2, 2, 6.5, 1, 'CSE, IT, ECE', 2025, 'Good analytical skills'),
    (3, 3, 7.5, 0, 'CSE, IT', 2025, 'Knowledge of backend frameworks'),
    (4, 4, 6.5, 0, 'CSE, IT, Design', 2025, 'HTML, CSS, JavaScript required'),
    (5, 5, 7.0, 0, 'CSE, IT, ECE', 2025, 'Knowledge of cloud platforms'),
    (6, 6, 8.0, 0, 'CSE, IT, AI', 2025, 'Machine learning knowledge required'),
    (7, 7, 7.5, 0, 'CSE, IT', 2025, 'Cloud certification preferred'),
    (8, 8, 7.0, 0, 'CSE, IT, Cyber Security', 2025, 'Security fundamentals required'),
    (9, 9, 7.0, 1, 'CSE, IT', 2025, 'Full stack project experience'),
    (10, 10, 6.5, 2, 'CSE, IT', 2025, 'Android development experience'),
    (11, 11, 6.0, 2, 'CSE, IT, ECE', 2025, 'Basic testing knowledge'),
    (12, 12, 6.5, 1, 'CSE, IT, MBA', 2025, 'Communication skills required'),
    (13, 13, 6.0, 2, 'CSE, IT, ECE, EE', 2025, 'System administration knowledge'),
    (14, 14, 7.5, 0, 'CSE, IT', 2025, 'Database management knowledge'),
    (15, 15, 8.0, 0, 'CSE, IT, AI, DS', 2025, 'Strong ML and statistics background');

INSERT OR IGNORE INTO applications (application_id, job_id, student_id, applied_at, status)
VALUES
    (1, 1, 230001, '2025-01-05', 'Applied'),
    (2, 2, 230001, '2025-01-06', 'Shortlisted'),
    (3, 3, 230002, '2025-01-07', 'Applied'),
    (4, 4, 230002, '2025-01-08', 'Rejected'),
    (5, 5, 230003, '2025-01-09', 'Shortlisted'),
    (6, 6, 230003, '2025-01-10', 'Applied'),
    (7, 7, 230004, '2025-01-11', 'Applied'),
    (8, 8, 230004, '2025-01-12', 'Shortlisted'),
    (9, 9, 230005, '2025-01-13', 'Rejected'),
    (10, 10, 230005, '2025-01-14', 'Applied'),
    (11, 11, 230006, '2025-01-15', 'Shortlisted'),
    (12, 12, 230006, '2025-01-16', 'Applied'),
    (13, 13, 230007, '2025-01-17', 'Rejected'),
    (14, 14, 230007, '2025-01-18', 'Applied'),
    (15, 15, 230008, '2025-01-19', 'Shortlisted'),
    (31, 1, 230006, '2025-02-05', 'Applied'),
    (32, 2, 230007, '2025-02-06', 'Applied'),
    (33, 3, 230008, '2025-02-07', 'Rejected'),
    (34, 4, 230009, '2025-02-08', 'Applied'),
    (35, 5, 230010, '2025-02-09', 'Shortlisted');

INSERT OR IGNORE INTO job_events (event_id, job_id, event_name, event_type, mode, venue, event_date, event_time)
VALUES
    (1, 1, 'Infosys Pre-Placement Talk', NULL, NULL, 'Auditorium A', '2025-01-03', '10:00'),
    (2, 2, 'TCS Online Test', NULL, NULL, 'Computer Lab 1', '2025-01-08', '09:00'),
    (3, 3, 'Wipro Technical Test', NULL, NULL, 'Computer Lab 2', '2025-01-12', '10:00'),
    (4, 4, 'Accenture PPT', NULL, NULL, 'Seminar Hall', '2025-01-15', '11:00'),
    (5, 5, 'Capgemini Coding Test', NULL, NULL, 'Lab 3', '2025-01-18', '09:30'),
    (6, 6, 'Google Technical Interview', NULL, NULL, 'Placement Office', '2025-01-20', '10:00'),
    (7, 7, 'Microsoft Interview', NULL, NULL, 'Placement Office', '2025-01-22', '11:00'),
    (8, 8, 'Amazon Assessment', NULL, NULL, 'Lab 4', '2025-01-24', '09:00'),
    (9, 9, 'IBM Interview', NULL, NULL, 'Placement Office', '2025-01-26', '10:30'),
    (10, 10, 'Cognizant PPT', NULL, NULL, 'Seminar Hall', '2025-01-28', '12:00'),
    (11, 11, 'Oracle Interview', NULL, NULL, 'Placement Office', '2025-02-02', '10:00'),
    (12, 12, 'Deloitte Test', NULL, NULL, 'Lab 5', '2025-02-04', '09:00'),
    (13, 13, 'HCL Interview', NULL, NULL, 'Placement Office', '2025-02-06', '11:30'),
    (14, 14, 'Tech Mahindra Test', NULL, NULL, 'Lab 2', '2025-02-08', '09:30'),
    (15, 15, 'Zoho Interview', NULL, NULL, 'Placement Office', '2025-02-10', '10:00');

INSERT OR IGNORE INTO venue_booking (booking_id, event_id, room_number, equipment_needed, academic_block)
VALUES
    (1, 1, 'AUD-A', 'Projector, Mic, Speaker System', 'Block 1'),
    (2, 2, 'CL-101', 'Desktop Systems, LAN, UPS', 'Block 2'),
    (3, 3, 'CL-102', 'Desktop Systems, Whiteboard', 'Block 2'),
    (4, 4, 'SEM-201', 'Projector, Mic', 'Block 3'),
    (5, 5, 'CL-103', 'Desktop Systems, LAN', 'Block 2'),
    (6, 6, 'INT-1', 'Interview Table, Chairs', 'Block 4'),
    (7, 7, 'INT-2', 'Interview Table, Projector', 'Block 4'),
    (8, 8, 'CL-104', 'Desktop Systems, LAN, UPS', 'Block 2'),
    (9, 9, 'INT-3', 'Interview Table, Whiteboard', 'Block 4'),
    (10, 10, 'SEM-202', 'Projector, Mic, Speaker', 'Block 3'),
    (11, 11, 'INT-4', 'Interview Table, Laptop Setup', 'Block 4'),
    (12, 12, 'CL-105', 'Desktop Systems, LAN', 'Block 2'),
    (13, 13, 'INT-5', 'Interview Table, Chairs', 'Block 4'),
    (14, 14, 'CL-106', 'Desktop Systems, UPS', 'Block 2'),
    (15, 15, 'INT-6', 'Interview Table, Projector', 'Block 4');

INSERT OR IGNORE INTO interviews (interview_id, application_id, event_id, meeting_link, platform)
VALUES
    (1, 1, 6, 'https://meet.google.com/inf-interview-001', 'Google Meet'),
    (2, 2, 7, 'https://teams.microsoft.com/interview-002', 'Microsoft Teams'),
    (3, 3, 9, 'https://zoom.us/interview-003', 'Zoom'),
    (4, 4, 11, 'https://meet.google.com/interview-004', 'Google Meet'),
    (5, 5, 13, 'https://teams.microsoft.com/interview-005', 'Microsoft Teams'),
    (6, 6, 15, 'https://zoom.us/interview-006', 'Zoom'),
    (7, 7, 6, 'https://meet.google.com/interview-007', 'Google Meet'),
    (8, 8, 7, 'https://teams.microsoft.com/interview-008', 'Microsoft Teams'),
    (9, 9, 9, 'https://zoom.us/interview-009', 'Zoom'),
    (10, 10, 11, 'https://meet.google.com/interview-010', 'Google Meet'),
    (11, 11, 13, 'https://teams.microsoft.com/interview-011', 'Microsoft Teams'),
    (12, 12, 15, 'https://zoom.us/interview-012', 'Zoom'),
    (13, 13, 6, 'https://meet.google.com/interview-013', 'Google Meet'),
    (14, 14, 7, 'https://teams.microsoft.com/interview-014', 'Microsoft Teams'),
    (15, 15, 9, 'https://zoom.us/interview-015', 'Zoom');

INSERT OR IGNORE INTO question_bank (q_id, company_id, question_text, type, difficulty, alumni_id)
VALUES
    (1, 1, 'Tell me about yourself and your projects.', 'Subjective', 'Easy', 1),
    (2, 2, 'Write a program to reverse a linked list.', 'Coding', 'Medium', 2),
    (3, 3, 'Explain OOPS concepts with examples.', 'Subjective', 'Easy', 3),
    (4, 4, 'Find the time complexity of binary search.', 'MCQ', 'Easy', 4),
    (5, 5, 'Write SQL query to find second highest salary.', 'Coding', 'Medium', 5),
    (6, 6, 'Design a scalable URL shortener.', 'Subjective', 'Hard', 6),
    (7, 7, 'Write a program to detect cycle in graph.', 'Coding', 'Hard', 7),
    (8, 8, 'Difference between process and thread.', 'Subjective', 'Easy', 8),
    (9, 9, 'Find duplicate elements in array.', 'Coding', 'Medium', 9),
    (10, 10, 'Explain normalization in DBMS.', 'Subjective', 'Medium', 10),
    (11, 11, 'Write query to join three tables.', 'Coding', 'Hard', 1),
    (12, 12, 'Explain operating system scheduling.', 'Subjective', 'Medium', 2),
    (13, 13, 'Find factorial using recursion.', 'Coding', 'Easy', 3),
    (14, 14, 'Explain deadlock and prevention.', 'Subjective', 'Hard', 4),
    (15, 15, 'Find largest element in array.', 'Coding', 'Easy', 5);

INSERT OR IGNORE INTO prep_pages (page_id, company_id, process_details, senior_feedback)
VALUES
    (1, 1, 'Infosys process includes aptitude test, technical interview and HR round.', 'Focus on aptitude and DBMS concepts. HR is easy if confident.'),
    (2, 2, 'TCS process includes online test, technical and managerial interview.', 'Coding basics and communication skills are important.'),
    (3, 3, 'Wipro has aptitude, technical interview and HR.', 'Prepare OOPS and basic programming questions.'),
    (4, 4, 'Accenture process includes aptitude, coding and interview.', 'Practice coding on arrays and strings.'),
    (5, 5, 'Capgemini focuses on aptitude and coding test.', 'SQL and logical reasoning questions are common.'),
    (6, 6, 'Google process includes coding rounds and system design.', 'Practice DSA and system design extensively.'),
    (7, 7, 'Microsoft includes online coding test and interviews.', 'Focus on problem solving and data structures.'),
    (8, 8, 'Amazon process includes OA and technical interviews.', 'Practice Leetcode medium and hard questions.'),
    (9, 9, 'IBM process includes aptitude and technical interview.', 'Focus on DBMS and basic programming.'),
    (10, 10, 'Cognizant includes aptitude and technical interview.', 'Coding and aptitude both are important.'),
    (11, 11, 'Oracle includes coding and system design interview.', 'SQL and DBMS questions are frequently asked.'),
    (12, 12, 'Deloitte includes aptitude and technical rounds.', 'Prepare aptitude and communication skills.'),
    (13, 13, 'HCL includes aptitude and technical interview.', 'Focus on core subjects and confidence.'),
    (14, 14, 'Tech Mahindra includes aptitude and HR round.', 'Communication skills matter most.'),
    (15, 15, 'Zoho includes coding rounds and interviews.', 'Strong DSA preparation is required.');

INSERT OR IGNORE INTO placement_stats (stat_id, batch, placed_count, avg_package, highest_package, generated_at)
VALUES
    (1, 2010, 320, 4.5, 12.0, '2011-06-01'),
    (2, 2011, 340, 4.8, 14.0, '2012-06-01'),
    (3, 2012, 360, 5.2, 16.5, '2013-06-01'),
    (4, 2013, 380, 5.5, 18.0, '2014-06-01'),
    (5, 2014, 400, 6.0, 20.0, '2015-06-01'),
    (6, 2015, 420, 6.3, 22.5, '2016-06-01'),
    (7, 2016, 450, 6.8, 25.0, '2017-06-01'),
    (8, 2017, 470, 7.2, 28.0, '2018-06-01'),
    (9, 2018, 490, 7.8, 32.0, '2019-06-01'),
    (10, 2019, 510, 8.5, 36.0, '2020-06-01'),
    (11, 2020, 530, 9.2, 40.0, '2021-06-01'),
    (12, 2021, 550, 10.0, 45.0, '2022-06-01'),
    (13, 2022, 580, 11.5, 52.0, '2023-06-01'),
    (14, 2023, 600, 12.8, 60.0, '2024-06-01'),
    (15, 2024, 620, 14.2, 68.0, '2025-06-01');

INSERT OR IGNORE INTO penalties (penalty_id, student_id, reason, penalty_type, issued_at)
VALUES
    (1, 230001, 'Absent during Infosys interview without notice', 'Warning', '2025-01-05'),
    (2, 230002, 'Late arrival in TCS online test', 'Warning', '2025-01-08'),
    (3, 230003, 'No show for Wipro interview', 'Debarment', '2025-01-12'),
    (4, 230004, 'Missed Accenture interview slot', 'Warning', '2025-01-15'),
    (5, 230005, 'Did not attend Capgemini test after registration', 'Debarment', '2025-01-18'),
    (6, 230006, 'Misconduct during Google interview', 'Debarment', '2025-01-20'),
    (7, 230007, 'Late submission of placement documents', 'Warning', '2025-01-22'),
    (8, 230008, 'Absent in Amazon interview', 'Debarment', '2025-01-24'),
    (9, 230009, 'Violation of placement rules', 'Warning', '2025-01-26'),
    (10, 230010, 'No show in Cognizant interview', 'Debarment', '2025-01-28'),
    (11, 230011, 'Improper behavior during interview', 'Warning', '2025-02-02'),
    (12, 230012, 'Missed Deloitte interview', 'Debarment', '2025-02-04'),
    (13, 230013, 'Absent in HCL recruitment process', 'Warning', '2025-02-06'),
    (14, 230014, 'Failed to attend Tech Mahindra interview', 'Debarment', '2025-02-08'),
    (15, 230015, 'No show in Zoho interview', 'Debarment', '2025-02-10');

INSERT OR IGNORE INTO cds_training_sessions (session_id, title, description, start_time, end_time, mode, created_at)
VALUES
    (1, 'Resume Building Workshop', 'Session on creating ATS friendly resumes', '2025-01-02 10:00:00', '2025-01-02 12:00:00', 'OFFLINE', '2024-12-20 09:00:00'),
    (2, 'Aptitude Preparation Session', 'Training on quantitative aptitude and logical reasoning', '2025-01-05 14:00:00', '2025-01-05 16:00:00', 'ONLINE', '2024-12-22 10:00:00'),
    (3, 'Technical Interview Preparation', 'Guidance on clearing technical interviews', '2025-01-08 10:00:00', '2025-01-08 12:30:00', 'OFFLINE', '2024-12-24 11:00:00'),
    (4, 'Mock Interview Session', 'Practice interviews with alumni', '2025-01-10 09:00:00', '2025-01-10 13:00:00', 'OFFLINE', '2024-12-26 09:30:00'),
    (5, 'Group Discussion Training', 'Training on GD techniques', '2025-01-12 14:00:00', '2025-01-12 16:00:00', 'ONLINE', '2024-12-28 10:30:00'),
    (6, 'Coding Interview Preparation', 'DSA and coding interview guidance', '2025-01-15 10:00:00', '2025-01-15 13:00:00', 'ONLINE', '2024-12-30 12:00:00'),
    (7, 'HR Interview Preparation', 'Training for HR interview questions', '2025-01-18 11:00:00', '2025-01-18 13:00:00', 'OFFLINE', '2025-01-01 10:00:00'),
    (8, 'System Design Basics', 'Introduction to system design concepts', '2025-01-20 14:00:00', '2025-01-20 17:00:00', 'ONLINE', '2025-01-03 11:00:00'),
    (9, 'LinkedIn Profile Optimization', 'Improve LinkedIn profile for recruiters', '2025-01-22 10:00:00', '2025-01-22 12:00:00', 'ONLINE', '2025-01-05 09:00:00'),
    (10, 'Communication Skills Workshop', 'Improve verbal communication skills', '2025-01-24 09:00:00', '2025-01-24 12:00:00', 'OFFLINE', '2025-01-07 10:00:00'),
    (11, 'Advanced Coding Practice', 'Practice competitive programming questions', '2025-01-26 14:00:00', '2025-01-26 17:00:00', 'ONLINE', '2025-01-09 11:30:00'),
    (12, 'Interview Experience Sharing', 'Alumni sharing interview experiences', '2025-01-28 10:00:00', '2025-01-28 13:00:00', 'OFFLINE', '2025-01-11 09:00:00'),
    (13, 'Placement Strategy Session', 'Planning placement preparation strategy', '2025-01-30 11:00:00', '2025-01-30 13:00:00', 'ONLINE', '2025-01-13 10:00:00'),
    (14, 'Technical Quiz Session', 'Test your technical knowledge', '2025-02-02 10:00:00', '2025-02-02 12:00:00', 'OFFLINE', '2025-01-15 09:30:00'),
    (15, 'Final Placement Preparation', 'Last minute placement preparation tips', '2025-02-05 14:00:00', '2025-02-05 16:30:00', 'ONLINE', '2025-01-17 11:00:00');

INSERT OR IGNORE INTO alumni_training_map (alumni_id, session_id)
VALUES
    (1, 1), (2, 1), (1, 2), (3, 3), (2, 3), (4, 4), (5, 4), (3, 5), (6, 6), (7, 6),
    (4, 7), (8, 8), (5, 8), (9, 9), (10, 10), (6, 11), (7, 12), (8, 12), (9, 13), (10, 14);
