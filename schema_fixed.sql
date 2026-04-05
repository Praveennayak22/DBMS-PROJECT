-- User Status
CREATE TYPE user_status_enum AS ENUM ('ACTIVE','INACTIVE','SUSPENDED');

-- Job Type
CREATE TYPE job_type_enum AS ENUM ('FULL_TIME','INTERNSHIP','PPO');

-- Event Type
CREATE TYPE event_type_enum AS ENUM ('PPT','OA','GD','Interview');

-- Mode
CREATE TYPE mode_enum AS ENUM ('ONLINE','OFFLINE','HYBRID');

-- Application Status
CREATE TYPE application_status_enum AS ENUM 
('Applied','Shortlisted','Rejected','Offered');

-- Penalty Type
CREATE TYPE penalty_type_enum AS ENUM ('Warning','Debarment');

-- Difficulty
CREATE TYPE difficulty_enum AS ENUM ('Easy','Medium','Hard');

-- Question Type
CREATE TYPE question_type_enum AS ENUM ('MCQ','Coding','Subjective');

-- Platform
CREATE TYPE platform_enum AS ENUM ('Zoom','Microsoft Teams','Google Meet');


CREATE TABLE Roles (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE Users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role_id INT NOT NULL,
    is_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    full_name VARCHAR(150) NOT NULL,
    contact_number VARCHAR(20),
    status user_status_enum DEFAULT 'ACTIVE',

    CONSTRAINT fk_role
        FOREIGN KEY (role_id)
        REFERENCES Roles(role_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CHECK (created_at <= CURRENT_TIMESTAMP),
    CHECK (email ~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'),
    CHECK (contact_number ~ '^\+?[1-9]\d{1,14}$')
);


CREATE TABLE User_Logs (
    log_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    action TEXT NOT NULL,
    ip_address VARCHAR(45),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    device_info TEXT,

    FOREIGN KEY (user_id)
        REFERENCES Users(user_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CHECK (end_time IS NULL OR end_time > start_time),
    CHECK (start_time <= CURRENT_TIMESTAMP),
    CHECK (action <> '')
);


CREATE TABLE Alumni_User (
    alumni_id SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL,
    grad_year INT NOT NULL,
    current_company VARCHAR(150),
    placement_history TEXT,
    designation VARCHAR(150),

    FOREIGN KEY (user_id)
        REFERENCES Users(user_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CHECK (grad_year BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE) + 1)
);


CREATE TABLE Students (
    student_id BIGSERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL,
    latest_cpi NUMERIC(3,2),
    program VARCHAR(100),
    discipline VARCHAR(100),
    graduating_year INT,
    active_backlogs INT DEFAULT 0,
    gender VARCHAR(10),
    tenth_percent NUMERIC(5,2),
    tenth_passout_year INT,
    twelfth_percent NUMERIC(5,2),
    twelfth_passout_year INT,

    FOREIGN KEY (user_id)
        REFERENCES Users(user_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CHECK (latest_cpi BETWEEN 0 AND 10),
    CHECK (active_backlogs >= 0),
    CHECK (tenth_percent BETWEEN 0 AND 100),
    CHECK (twelfth_percent BETWEEN 0 AND 100),
    CHECK (gender IN ('Male','Female','Other')),
    CHECK (tenth_passout_year < twelfth_passout_year),
    CHECK (twelfth_passout_year <= graduating_year)
);


CREATE TABLE Resumes (
    resume_id SERIAL PRIMARY KEY,
    student_id BIGINT NOT NULL,
    resume_label VARCHAR(100) NOT NULL,
    file_url TEXT NOT NULL,
    ats_score NUMERIC(5,2),
    is_verified BOOLEAN DEFAULT FALSE,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (student_id)
        REFERENCES Students(student_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CHECK (ats_score BETWEEN 0 AND 100),
    CHECK (resume_label <> ''),
    CHECK (file_url ~ '^https?://')
);


CREATE TABLE Companies (
    company_id SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL,
    company_name VARCHAR(200) NOT NULL,
    industry_sector VARCHAR(150),
    type_of_organization VARCHAR(100),
    hiring_history TEXT,
    company_description TEXT,
    website_url TEXT,

    FOREIGN KEY (user_id)
        REFERENCES Users(user_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CHECK (company_name <> '')
);


CREATE TABLE Job_Postings (
    job_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL,
    designation VARCHAR(150),
    description TEXT,
    location VARCHAR(150),
    stipend NUMERIC(10,2),
    job_type VARCHAR(50),
    deadline DATE,
    posted_date DATE,

    FOREIGN KEY (company_id)
        REFERENCES Companies(company_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CHECK (stipend >= 0)
);


CREATE TABLE Eligibility_Criteria (
    criteria_id SERIAL PRIMARY KEY,
    job_id INT UNIQUE NOT NULL,
    min_cpi NUMERIC(3,2),
    allowed_backlogs INT,
    eligible_programs TEXT,
    eligible_year INT,
    additional_requirements TEXT,

    FOREIGN KEY (job_id)
        REFERENCES Job_Postings(job_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    CHECK (min_cpi BETWEEN 0 AND 10),
    CHECK (allowed_backlogs >= 0)
);


CREATE TABLE Applications (
    application_id SERIAL PRIMARY KEY,
    job_id INT NOT NULL,
    student_id BIGINT NOT NULL,
    applied_at DATE,
    status VARCHAR(50),

    FOREIGN KEY (job_id)
        REFERENCES Job_Postings(job_id)
        ON DELETE CASCADE,

    FOREIGN KEY (student_id)
        REFERENCES Students(student_id)
        ON DELETE CASCADE,
    
    UNIQUE(job_id, student_id)
);


CREATE TABLE Job_Events (
    event_id SERIAL PRIMARY KEY,
    job_id INT NOT NULL,
    event_name VARCHAR(200),
    event_date DATE,
    event_time TIME,
    venue VARCHAR(200),
    description TEXT,

    FOREIGN KEY (job_id)
        REFERENCES Job_Postings(job_id)
        ON DELETE CASCADE
);


CREATE TABLE Venue_Booking (
    booking_id SERIAL PRIMARY KEY,
    event_id INT NOT NULL,
    room_number VARCHAR(50),
    equipment_needed TEXT,
    academic_block VARCHAR(50),

    FOREIGN KEY (event_id)
        REFERENCES Job_Events(event_id)
        ON DELETE CASCADE
);


CREATE TABLE Interviews (
    interview_id SERIAL PRIMARY KEY,
    application_id INT NOT NULL,
    event_id INT NOT NULL,
    meeting_link TEXT,
    platform VARCHAR(50),

    FOREIGN KEY (application_id)
        REFERENCES Applications(application_id)
        ON DELETE CASCADE,

    FOREIGN KEY (event_id)
        REFERENCES Job_Events(event_id)
        ON DELETE CASCADE,

    CHECK (meeting_link ~ '^https?://')
);


CREATE TABLE Question_Bank (
    q_id SERIAL PRIMARY KEY,
    company_id INT,
    question_text TEXT NOT NULL,
    type question_type_enum,
    difficulty difficulty_enum,
    alumni_id INT,

    FOREIGN KEY (company_id)
        REFERENCES Companies(company_id)
        ON DELETE SET NULL,

    FOREIGN KEY (alumni_id)
        REFERENCES Alumni_User(alumni_id)
        ON DELETE SET NULL,

    CHECK (question_text <> '')
);


CREATE TABLE Prep_Pages (
    page_id SERIAL PRIMARY KEY,
    company_id INT,
    process_details TEXT,
    senior_feedback TEXT,

    FOREIGN KEY (company_id)
        REFERENCES Companies(company_id)
        ON DELETE CASCADE
);


CREATE TABLE Placement_Stats (
    stat_id SERIAL PRIMARY KEY,
    batch INT,
    placed_count INT,
    avg_package NUMERIC(12,2),
    highest_package NUMERIC(12,2),
    generated_at DATE,

    CHECK (placed_count >= 0),
    CHECK (avg_package >= 0),
    CHECK (highest_package >= avg_package)
);


CREATE TABLE Penalties (
    penalty_id SERIAL PRIMARY KEY,
    student_id BIGINT NOT NULL,
    reason TEXT NOT NULL,
    penalty_type penalty_type_enum,
    issued_at DATE,

    FOREIGN KEY (student_id)
        REFERENCES Students(student_id)
        ON DELETE CASCADE,

    CHECK (reason <> '')
);


CREATE TABLE CDS_Training_Sessions (
    session_id BIGSERIAL PRIMARY KEY,
    title VARCHAR(150),
    description TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    mode mode_enum,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CHECK (end_time > start_time)
);


CREATE TABLE Alumni_Training_Map (
    alumni_id INT,
    session_id BIGINT,

    PRIMARY KEY (alumni_id, session_id),

    FOREIGN KEY (alumni_id)
        REFERENCES Alumni_User(alumni_id)
        ON DELETE CASCADE,

    FOREIGN KEY (session_id)
        REFERENCES CDS_Training_Sessions(session_id)
        ON DELETE CASCADE
);


-- =====================================================
-- DATA INSERTION
-- =====================================================

INSERT INTO Roles (role_id, role_name, description) VALUES
(1, 'Student', 'Current student eligible for placements'),
(2, 'Recruiter', 'Company representative posting jobs and hiring students'),
(3, 'CDS Team', 'Placement cell coordinators managing drives'),
(4, 'CDS Manager', 'Head of Career Development and Placement Services'),
(5, 'Alumni', 'Graduated student providing referrals, guidance, and training');


INSERT INTO Users (username, email, password_hash, role_id, is_verified, full_name, contact_number, status) VALUES
-- ================= STUDENTS (30) =================
('student1','aarav.patel@gmail.com','hash1',1,TRUE,'Aarav Patel','+919876540001','ACTIVE'),
('student2','vivaan.shah@gmail.com','hash2',1,TRUE,'Vivaan Shah','+919876540002','ACTIVE'),
('student3','aditya.mehta@gmail.com','hash3',1,TRUE,'Aditya Mehta','+919876540003','ACTIVE'),
('student4','krishna.patel@gmail.com','hash4',1,TRUE,'Krishna Patel','+919876540004','ACTIVE'),
('student5','ishaan.shah@gmail.com','hash5',1,TRUE,'Ishaan Shah','+919876540005','ACTIVE'),
('student6','rohan.desai@gmail.com','hash6',1,TRUE,'Rohan Desai','+919876540006','ACTIVE'),
('student7','arjun.joshi@gmail.com','hash7',1,TRUE,'Arjun Joshi','+919876540007','ACTIVE'),
('student8','dhruv.patel@gmail.com','hash8',1,TRUE,'Dhruv Patel','+919876540008','ACTIVE'),
('student9','kabir.shah@gmail.com','hash9',1,TRUE,'Kabir Shah','+919876540009','ACTIVE'),
('student10','dev.patel@gmail.com','hash10',1,TRUE,'Dev Patel','+919876540010','ACTIVE'),
('student11','jay.shah@gmail.com','hash11',1,TRUE,'Jay Shah','+919876540011','ACTIVE'),
('student12','neel.mehta@gmail.com','hash12',1,TRUE,'Neel Mehta','+919876540012','ACTIVE'),
('student13','om.patel@gmail.com','hash13',1,TRUE,'Om Patel','+919876540013','ACTIVE'),
('student14','raj.shah@gmail.com','hash14',1,TRUE,'Raj Shah','+919876540014','ACTIVE'),
('student15','karan.patel@gmail.com','hash15',1,TRUE,'Karan Patel','+919876540015','ACTIVE'),
('student16','aryan.shah@gmail.com','hash16',1,TRUE,'Aryan Shah','+919876540016','ACTIVE'),
('student17','harsh.patel@gmail.com','hash17',1,TRUE,'Harsh Patel','+919876540017','ACTIVE'),
('student18','yash.shah@gmail.com','hash18',1,TRUE,'Yash Shah','+919876540018','ACTIVE'),
('student19','meet.patel@gmail.com','hash19',1,TRUE,'Meet Patel','+919876540019','ACTIVE'),
('student20','parth.shah@gmail.com','hash20',1,TRUE,'Parth Shah','+919876540020','ACTIVE'),
('student21','nirav.patel@gmail.com','hash21',1,TRUE,'Nirav Patel','+919876540021','ACTIVE'),
('student22','vraj.shah@gmail.com','hash22',1,TRUE,'Vraj Shah','+919876540022','ACTIVE'),
('student23','sahil.patel@gmail.com','hash23',1,TRUE,'Sahil Patel','+919876540023','ACTIVE'),
('student24','manav.shah@gmail.com','hash24',1,TRUE,'Manav Shah','+919876540024','ACTIVE'),
('student25','tirth.patel@gmail.com','hash25',1,TRUE,'Tirth Patel','+919876540025','ACTIVE'),
('student26','kunal.shah@gmail.com','hash26',1,TRUE,'Kunal Shah','+919876540026','ACTIVE'),
('student27','jatin.patel@gmail.com','hash27',1,TRUE,'Jatin Patel','+919876540027','ACTIVE'),
('student28','deep.shah@gmail.com','hash28',1,TRUE,'Deep Shah','+919876540028','ACTIVE'),
('student29','hiren.patel@gmail.com','hash29',1,TRUE,'Hiren Patel','+919876540029','ACTIVE'),
('student30','rahul.shah@gmail.com','hash30',1,TRUE,'Rahul Shah','+919876540030','ACTIVE'),

-- ================= ALUMNI (10) =================
('alumni1','amit.shah@gmail.com','hash31',5,TRUE,'Amit Shah','+919876540031','ACTIVE'),
('alumni2','rahul.mehta@gmail.com','hash32',5,TRUE,'Rahul Mehta','+919876540032','ACTIVE'),
('alumni3','suresh.patel@gmail.com','hash33',5,TRUE,'Suresh Patel','+919876540033','ACTIVE'),
('alumni4','mahesh.shah@gmail.com','hash34',5,TRUE,'Mahesh Shah','+919876540034','ACTIVE'),
('alumni5','rajesh.patel@gmail.com','hash35',5,TRUE,'Rajesh Patel','+919876540035','ACTIVE'),
('alumni6','anil.shah@gmail.com','hash36',5,TRUE,'Anil Shah','+919876540036','ACTIVE'),
('alumni7','sunil.patel@gmail.com','hash37',5,TRUE,'Sunil Patel','+919876540037','ACTIVE'),
('alumni8','rakesh.shah@gmail.com','hash38',5,TRUE,'Rakesh Shah','+919876540038','ACTIVE'),
('alumni9','mukesh.patel@gmail.com','hash39',5,TRUE,'Mukesh Patel','+919876540039','ACTIVE'),
('alumni10','naresh.shah@gmail.com','hash40',5,TRUE,'Naresh Shah','+919876540040','ACTIVE'),

-- ================= RECRUITERS (15) =================
('recruiter1','hr.tcs@gmail.com','hash41',2,TRUE,'Priya Sharma','+919876540041','ACTIVE'),
('recruiter2','hr.infosys@gmail.com','hash42',2,TRUE,'Anjali Verma','+919876540042','ACTIVE'),
('recruiter3','hr.google@gmail.com','hash43',2,TRUE,'Sneha Iyer','+919876540043','ACTIVE'),
('recruiter4','hr.microsoft@gmail.com','hash44',2,TRUE,'Riya Kapoor','+919876540044','ACTIVE'),
('recruiter5','hr.amazon@gmail.com','hash45',2,TRUE,'Pooja Singh','+919876540045','ACTIVE'),
('recruiter6','hr.meta@gmail.com','hash46',2,TRUE,'Neha Agarwal','+919876540046','ACTIVE'),
('recruiter7','hr.ibm@gmail.com','hash47',2,TRUE,'Kavya Nair','+919876540047','ACTIVE'),
('recruiter8','hr.oracle@gmail.com','hash48',2,TRUE,'Isha Menon','+919876540048','ACTIVE'),
('recruiter9','hr.intel@gmail.com','hash49',2,TRUE,'Megha Pillai','+919876540049','ACTIVE'),
('recruiter10','hr.nvidia@gmail.com','hash50',2,TRUE,'Nisha Reddy','+919876540050','ACTIVE'),
('recruiter11','hr.adobe@gmail.com','hash51',2,TRUE,'Ritu Sharma','+919876540051','ACTIVE'),
('recruiter12','hr.salesforce@gmail.com','hash52',2,TRUE,'Simran Kaur','+919876540052','ACTIVE'),
('recruiter13','hr.uber@gmail.com','hash53',2,TRUE,'Divya Jain','+919876540053','ACTIVE'),
('recruiter14','hr.netflix@gmail.com','hash54',2,TRUE,'Payal Gupta','+919876540054','ACTIVE'),
('recruiter15','hr.accenture@gmail.com','hash55',2,TRUE,'Komal Shah','+919876540055','ACTIVE'),

-- ================= CDS TEAM (5) =================
('cds1','cds1@college.edu','hash56',3,TRUE,'Placement Coordinator 1','+919876540056','ACTIVE'),
('cds2','cds2@college.edu','hash57',3,TRUE,'Placement Coordinator 2','+919876540057','ACTIVE'),
('cds3','cds3@college.edu','hash58',3,TRUE,'Placement Coordinator 3','+919876540058','ACTIVE'),
('cds4','cds4@college.edu','hash59',3,TRUE,'Placement Coordinator 4','+919876540059','ACTIVE'),
('cds5','cds5@college.edu','hash60',3,TRUE,'Placement Coordinator 5','+919876540060','ACTIVE'),

-- ================= CDS MANAGER (1) =================
('cdsmanager1','manager@college.edu','hash61',4,TRUE,'Placement Manager','+919876540061','ACTIVE');


INSERT INTO Students (student_id, user_id, latest_cpi, program, discipline, graduating_year, active_backlogs, gender, tenth_percent, tenth_passout_year, twelfth_percent, twelfth_passout_year) VALUES
(230001,1,9.12,'B.Tech','Computer Science',2026,0,'Male',95.2,2020,93.4,2022),
(230002,2,8.75,'B.Tech','Computer Science',2026,0,'Male',92.3,2020,91.2,2022),
(230003,3,8.21,'B.Tech','Electronics',2026,0,'Male',90.4,2020,88.6,2022),
(230004,4,7.95,'B.Tech','Mechanical',2026,1,'Male',88.3,2020,85.1,2022),
(230005,5,9.30,'B.Tech','Computer Science',2026,0,'Male',96.1,2020,94.2,2022),
(230006,6,8.66,'B.Tech','Civil',2026,0,'Male',91.4,2020,89.3,2022),
(230007,7,7.88,'B.Tech','Chemical',2026,2,'Male',87.2,2020,84.5,2022),
(230008,8,8.91,'B.Tech','Electrical',2026,0,'Male',93.6,2020,90.2,2022),
(230009,9,9.41,'B.Tech','Computer Science',2026,0,'Male',97.2,2020,95.8,2022),
(230010,10,8.03,'B.Tech','Mathematics',2026,0,'Male',89.5,2020,87.3,2022),
(230011,11,7.76,'B.Tech','Physics',2026,1,'Male',85.3,2020,83.7,2022),
(230012,12,8.88,'B.Tech','Biotechnology',2026,0,'Male',92.7,2020,91.5,2022),
(230013,13,9.55,'B.Tech','Aerospace',2026,0,'Male',98.1,2020,96.2,2022),
(230014,14,8.46,'B.Tech','Production',2026,0,'Male',90.8,2020,88.4,2022),
(230015,15,8.11,'B.Tech','Metallurgy',2026,0,'Male',88.9,2020,86.3,2022),
(230016,16,7.64,'B.Tech','Artificial Intelligence',2026,2,'Male',84.3,2020,82.1,2022),
(230017,17,9.67,'B.Tech','Data Science',2026,0,'Male',97.9,2020,96.4,2022),
(230018,18,8.34,'B.Tech','Robotics',2026,0,'Male',91.2,2020,89.5,2022),
(230019,19,8.97,'B.Tech','Computer Science',2026,0,'Male',94.3,2020,92.6,2022),
(230020,20,7.83,'B.Tech','Electronics',2026,1,'Male',86.8,2020,84.9,2022),
(230021,21,8.42,'B.Tech','Mechanical',2026,0,'Male',90.3,2020,88.2,2022),
(230022,22,9.02,'B.Tech','Civil',2026,0,'Male',95.1,2020,93.7,2022),
(230023,23,8.16,'B.Tech','Chemical',2026,0,'Male',89.7,2020,87.4,2022),
(230024,24,8.69,'B.Tech','Electrical',2026,0,'Male',92.5,2020,90.1,2022),
(230025,25,7.92,'B.Tech','Mathematics',2026,1,'Male',87.1,2020,85.3,2022),
(230026,26,8.81,'B.Tech','Physics',2026,0,'Male',93.8,2020,91.7,2022),
(230027,27,9.28,'B.Tech','Biotechnology',2026,0,'Male',96.4,2020,94.8,2022),
(230028,28,8.93,'B.Tech','Aerospace',2026,0,'Male',94.7,2020,92.9,2022),
(230029,29,8.54,'B.Tech','Production',2026,0,'Male',91.6,2020,89.8,2022),
(230030,30,9.74,'B.Tech','Artificial Intelligence',2026,0,'Male',98.6,2020,97.1,2022);


INSERT INTO User_Logs (log_id, user_id, action, ip_address, start_time, end_time, device_info) VALUES
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


INSERT INTO Alumni_User (alumni_id, user_id, grad_year, current_company, placement_history, designation) VALUES
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


INSERT INTO Resumes (resume_id, student_id, resume_label, file_url, ats_score, is_verified, uploaded_at) VALUES
(1,230001,'Software Resume','https://drive.google.com/resume230001',92,TRUE,'2026-01-05 10:00:00'),
(2,230002,'Software Resume','https://drive.google.com/resume230002',88,TRUE,'2026-01-05 10:15:00'),
(3,230003,'Electronics Resume','https://drive.google.com/resume230003',81,TRUE,'2026-01-06 09:30:00'),
(4,230004,'Mechanical Resume','https://drive.google.com/resume230004',75,FALSE,'2026-01-06 11:20:00'),
(5,230005,'Software Resume','https://drive.google.com/resume230005',95,TRUE,'2026-01-07 08:40:00'),
(6,230006,'Civil Resume','https://drive.google.com/resume230006',82,TRUE,'2026-01-07 09:10:00'),
(7,230007,'Chemical Resume','https://drive.google.com/resume230007',72,FALSE,'2026-01-07 12:00:00'),
(8,230008,'Electrical Resume','https://drive.google.com/resume230008',87,TRUE,'2026-01-08 10:30:00'),
(9,230009,'Software Resume','https://drive.google.com/resume230009',96,TRUE,'2026-01-08 14:00:00'),
(10,230010,'Data Science Resume','https://drive.google.com/resume230010',84,TRUE,'2026-01-09 09:00:00'),
(11,230011,'Physics Resume','https://drive.google.com/resume230011',74,FALSE,'2026-01-09 11:45:00'),
(12,230012,'Biotech Resume','https://drive.google.com/resume230012',85,TRUE,'2026-01-10 08:15:00'),
(13,230013,'Aerospace Resume','https://drive.google.com/resume230013',91,TRUE,'2026-01-10 10:50:00'),
(14,230014,'Production Resume','https://drive.google.com/resume230014',80,TRUE,'2026-01-11 09:35:00'),
(15,230015,'Metallurgy Resume','https://drive.google.com/resume230015',78,FALSE,'2026-01-11 13:10:00'),
(16,230016,'AI Resume','https://drive.google.com/resume230016',86,TRUE,'2026-01-12 08:55:00'),
(17,230017,'Data Science Resume','https://drive.google.com/resume230017',94,TRUE,'2026-01-12 10:30:00'),
(18,230018,'Robotics Resume','https://drive.google.com/resume230018',83,TRUE,'2026-01-13 09:20:00'),
(19,230019,'Software Resume','https://drive.google.com/resume230019',89,TRUE,'2026-01-13 11:40:00'),
(20,230020,'Electronics Resume','https://drive.google.com/resume230020',77,FALSE,'2026-01-14 10:00:00'),
(21,230021,'Mechanical Resume','https://drive.google.com/resume230021',81,TRUE,'2026-01-14 12:15:00'),
(22,230022,'Civil Resume','https://drive.google.com/resume230022',90,TRUE,'2026-01-15 08:45:00'),
(23,230023,'Chemical Resume','https://drive.google.com/resume230023',79,TRUE,'2026-01-15 11:00:00'),
(24,230024,'Electrical Resume','https://drive.google.com/resume230024',88,TRUE,'2026-01-16 09:10:00'),
(25,230025,'Math Resume','https://drive.google.com/resume230025',76,FALSE,'2026-01-16 12:30:00'),
(26,230026,'Physics Resume','https://drive.google.com/resume230026',85,TRUE,'2026-01-17 10:20:00'),
(27,230027,'Biotech Resume','https://drive.google.com/resume230027',93,TRUE,'2026-01-17 13:50:00'),
(28,230028,'Aerospace Resume','https://drive.google.com/resume230028',87,TRUE,'2026-01-18 08:35:00'),
(29,230029,'Production Resume','https://drive.google.com/resume230029',82,TRUE,'2026-01-18 11:25:00'),
(30,230030,'AI Resume','https://drive.google.com/resume230030',96,TRUE,'2026-01-19 09:40:00');


INSERT INTO Companies (company_id, user_id, company_name, industry_sector, type_of_organization, hiring_history, company_description, website_url) VALUES
(1,41,'Tata Consultancy Services','IT Services','Service-Based','Hires 50+ students annually from campus','Leading Indian multinational IT services and consulting company','https://www.tcs.com'),
(2,42,'Infosys','IT Services','Service-Based','Hires 40+ students annually','Global leader in next-generation digital services and consulting','https://www.infosys.com'),
(3,43,'Google','Software','Product-Based','Hires 5–10 students annually','Global technology leader specializing in internet-related services','https://careers.google.com'),
(4,44,'Microsoft','Software','Product-Based','Hires 8–12 students annually','Multinational technology corporation producing software and services','https://careers.microsoft.com'),
(5,45,'Amazon','E-Commerce','Product-Based','Hires 10–15 students annually','World''s largest online retailer and cloud provider','https://www.amazon.jobs'),
(6,46,'Meta','Social Media','Product-Based','Hires 5–8 students annually','Technology company focusing on social media and virtual reality','https://www.metacareers.com'),
(7,47,'IBM','IT Services','Service-Based','Hires 20+ students annually','Global technology and consulting company','https://www.ibm.com/careers'),
(8,48,'Oracle','Database Software','Product-Based','Hires 10+ students annually','Specializes in database software and enterprise solutions','https://www.oracle.com/careers'),
(9,49,'Intel','Semiconductors','Product-Based','Hires 5–10 students annually','Designs and manufactures semiconductor chips','https://jobs.intel.com'),
(10,50,'NVIDIA','Semiconductors','Product-Based','Hires 5+ students annually','Leader in GPU computing and AI technology','https://www.nvidia.com/careers'),
(11,51,'Adobe','Software','Product-Based','Hires 5–7 students annually','Known for creative and multimedia software products','https://careers.adobe.com'),
(12,52,'Salesforce','Cloud Computing','Product-Based','Hires 5–10 students annually','Cloud-based customer relationship management company','https://careers.salesforce.com'),
(13,53,'Uber','Technology','Product-Based','Hires 3–6 students annually','Ride-hailing and technology platform company','https://www.uber.com/careers'),
(14,54,'Netflix','Entertainment Technology','Product-Based','Hires 2–5 students annually','Streaming entertainment and production company','https://jobs.netflix.com'),
(15,55,'Accenture','IT Services','Service-Based','Hires 30+ students annually','Global professional services company','https://www.accenture.com/careers');


INSERT INTO Job_Postings (job_id, company_id, designation, description, location, stipend, job_type, deadline, posted_date) VALUES
(1, 1, 'Software Engineer', 'Develop and maintain web applications using modern frameworks', 'Bangalore', 1200000, 'Full-Time', '2025-02-01', '2025-01-01'),
(2, 2, 'Data Analyst', 'Analyze business data and generate insights', 'Mumbai', 800000, 'Full-Time', '2025-02-05', '2025-01-05'),
(3, 3, 'Backend Developer', 'Build scalable backend APIs', 'Hyderabad', 1100000, 'Full-Time', '2025-02-10', '2025-01-10'),
(4, 4, 'Frontend Developer', 'Create responsive user interfaces', 'Pune', 900000, 'Full-Time', '2025-02-12', '2025-01-12'),
(5, 5, 'DevOps Engineer', 'Manage CI/CD pipelines and deployment', 'Chennai', 1300000, 'Full-Time', '2025-02-15', '2025-01-15'),
(6, 6, 'AI Engineer', 'Develop machine learning models', 'Bangalore', 1500000, 'Full-Time', '2025-02-18', '2025-01-18'),
(7, 7, 'Cloud Engineer', 'Manage cloud infrastructure', 'Hyderabad', 1400000, 'Full-Time', '2025-02-20', '2025-01-20'),
(8, 8, 'Cyber Security Analyst', 'Ensure system security', 'Mumbai', 1000000, 'Full-Time', '2025-02-22', '2025-01-22'),
(9, 9, 'Full Stack Developer', 'Work on frontend and backend', 'Pune', 1250000, 'Full-Time', '2025-02-25', '2025-01-25'),
(10, 10, 'Mobile App Developer', 'Develop Android applications', 'Chennai', 950000, 'Full-Time', '2025-02-28', '2025-01-28'),
(11, 11, 'QA Engineer', 'Test applications and ensure quality', 'Noida', 700000, 'Full-Time', '2025-03-01', '2025-02-01'),
(12, 12, 'Business Analyst', 'Gather requirements and analyze', 'Gurgaon', 850000, 'Full-Time', '2025-03-05', '2025-02-05'),
(13, 13, 'System Engineer', 'Maintain IT systems', 'Ahmedabad', 750000, 'Full-Time', '2025-03-08', '2025-02-08'),
(14, 14, 'Database Administrator', 'Manage database systems', 'Bangalore', 1150000, 'Full-Time', '2025-03-10', '2025-02-10'),
(15, 15, 'Machine Learning Engineer', 'Build ML pipelines', 'Hyderabad', 1600000, 'Full-Time', '2025-03-12', '2025-02-12');


INSERT INTO Eligibility_Criteria (criteria_id, job_id, min_cpi, allowed_backlogs, eligible_programs, eligible_year, additional_requirements) VALUES
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


INSERT INTO Applications (application_id, job_id, student_id, applied_at, status) VALUES
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
(16, 16, 230008, '2025-01-20', 'Applied'),
(17, 17, 230009, '2025-01-21', 'Applied'),
(18, 18, 230009, '2025-01-22', 'Rejected'),
(19, 19, 230010, '2025-01-23', 'Shortlisted'),
(20, 20, 230010, '2025-01-24', 'Applied'),
(21, 21, 230011, '2025-01-25', 'Applied'),
(22, 22, 230011, '2025-01-26', 'Shortlisted'),
(23, 23, 230012, '2025-01-27', 'Applied'),
(24, 24, 230012, '2025-01-28', 'Rejected'),
(25, 25, 230013, '2025-01-29', 'Shortlisted'),
(26, 26, 230013, '2025-01-30', 'Applied'),
(27, 27, 230014, '2025-02-01', 'Applied'),
(28, 28, 230014, '2025-02-02', 'Shortlisted'),
(29, 29, 230015, '2025-02-03', 'Applied'),
(30, 30, 230015, '2025-02-04', 'Shortlisted'),
(31, 1, 230006, '2025-02-05', 'Applied'),
(32, 2, 230007, '2025-02-06', 'Applied'),
(33, 3, 230008, '2025-02-07', 'Rejected'),
(34, 4, 230009, '2025-02-08', 'Applied'),
(35, 5, 230010, '2025-02-09', 'Shortlisted');


INSERT INTO Job_Events (event_id, job_id, event_name, event_date, event_time, venue, description) VALUES
(1, 1, 'Infosys Pre-Placement Talk', '2025-01-03', '10:00', 'Auditorium A', 'Company presentation and hiring process explanation'),
(2, 2, 'TCS Online Test', '2025-01-08', '09:00', 'Computer Lab 1', 'Aptitude and technical online assessment'),
(3, 3, 'Wipro Technical Test', '2025-01-12', '10:00', 'Computer Lab 2', 'Technical written test'),
(4, 4, 'Accenture PPT', '2025-01-15', '11:00', 'Seminar Hall', 'Pre-placement talk'),
(5, 5, 'Capgemini Coding Test', '2025-01-18', '09:30', 'Lab 3', 'Online coding round'),
(6, 6, 'Google Technical Interview', '2025-01-20', '10:00', 'Placement Office', 'Technical interview round'),
(7, 7, 'Microsoft Interview', '2025-01-22', '11:00', 'Placement Office', 'Technical and HR interview'),
(8, 8, 'Amazon Assessment', '2025-01-24', '09:00', 'Lab 4', 'Online assessment'),
(9, 9, 'IBM Interview', '2025-01-26', '10:30', 'Placement Office', 'Technical interview'),
(10, 10, 'Cognizant PPT', '2025-01-28', '12:00', 'Seminar Hall', 'Company overview'),
(11, 11, 'Oracle Interview', '2025-02-02', '10:00', 'Placement Office', 'Interview round'),
(12, 12, 'Deloitte Test', '2025-02-04', '09:00', 'Lab 5', 'Online test'),
(13, 13, 'HCL Interview', '2025-02-06', '11:30', 'Placement Office', 'Technical interview'),
(14, 14, 'Tech Mahindra Test', '2025-02-08', '09:30', 'Lab 2', 'Online test'),
(15, 15, 'Zoho Interview', '2025-02-10', '10:00', 'Placement Office', 'Final interview');


INSERT INTO Venue_Booking (booking_id, event_id, room_number, equipment_needed, academic_block) VALUES
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


INSERT INTO Interviews (interview_id, application_id, event_id, meeting_link, platform) VALUES
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


INSERT INTO Question_Bank (q_id, company_id, question_text, type, difficulty, alumni_id) VALUES
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


INSERT INTO Prep_Pages (page_id, company_id, process_details, senior_feedback) VALUES
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


INSERT INTO Placement_Stats (stat_id, batch, placed_count, avg_package, highest_package, generated_at) VALUES
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


INSERT INTO Penalties (penalty_id, student_id, reason, penalty_type, issued_at) VALUES
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


INSERT INTO CDS_Training_Sessions (session_id, title, description, start_time, end_time, mode, created_at) VALUES
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


INSERT INTO Alumni_Training_Map (alumni_id, session_id) VALUES
(1, 1),
(2, 1),
(1, 2),
(3, 3),
(2, 3),
(4, 4),
(5, 4),
(3, 5),
(6, 6),
(7, 6),
(4, 7),
(8, 8),
(5, 8),
(9, 9),
(10, 10),
(6, 11),
(7, 12),
(8, 12),
(9, 13),
(10, 14);
