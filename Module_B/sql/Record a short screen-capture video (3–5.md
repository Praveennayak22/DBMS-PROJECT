Record a short screen-capture video (3–5 minutes) demonstrating your fully integrated local system.
You must include a clear audio voice-over/explanation that walks us through your implementation.
Your video and audio explanation must clearly cover:
• UI & API Functionality: Narrate your navigation of the web UI, viewing the Member Portfolio,
and successfully performing CRUD operations on your Task 1 tables.
• RBAC Enforcement: Explain how your roles are set up while logging in as an Admin (demonstrating
administrative access) and then as a Regular User (demonstrating restricted/read-only access).
• Security Logging Check: Briefly explain your logging mechanism while showing your local logs
capturing valid API operations and highlighting any flagged unauthorised database modifications.
Note: Host the video on a platform like Google Drive or YouTube (Unlisted) and include the link in your
report.


for this task, my script is like this- 
firstly, we need to start our backend by running the module B using this line-  & ".venv/Scripts/python.exe" -m uvicorn app.main:app --port 8001 --app-dir Module_B. 

once backend starts, we will now go follow this url and this is our website. 
here, lets first login as an admin. 
admin has power to do anything and everything. Admin can access portfolios of all students. Admin can add and delete existing students for example- (20 sec demo). Admin can add and deldete recruiters/ job postings(10 sec demo). Admin can view all applications that students have applied. 

next moving to what all a student can do, they can edit their profile and apply to jobs they are eligible and view their statuses. (20 sec demo)

next coming to recruiters, they can add their company ddetails and post new jobs and view the candidates applied for wach job. 

next we have cds team who have powers similar to admin but just viewing and no editing.(5 sec demo). 

I will be showing the live changes in module_b.db and the terminal outputs. so give what to say while checking the terminal outputs. 

Next, I will walk through the file struture of module B. Tell what should be my script for this part as well. 

have i missed anything asked in the question?? if yes do add that and see that the script takes no more than 5 minutes to complete. 







SELECT * FROM students s JOIN users u ON u.user_id = s.user_id ORDER BY s.student_id DESC LIMIT 3;

SELECT * FROM job_postings j JOIN companies c ON c.company_id = j.company_id LEFT JOIN eligibility_criteria e ON e.job_id = j.job_id ORDER BY j.job_id DESC LIMIT 5;

SELECT * FROM applications a JOIN students s ON s.student_id = a.student_id JOIN users u ON u.user_id = s.user_id JOIN job_postings j ON j.job_id = a.job_id ORDER BY a.application_id DESC LIMIT 5;