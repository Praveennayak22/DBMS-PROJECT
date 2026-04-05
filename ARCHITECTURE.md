# CareerTrack System Architecture

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                          │
│  (Web Browser, Mobile App, API Testing Tools like Postman)  │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ HTTPS/HTTP
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     FASTAPI APPLICATION                      │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              API Layer (app/api/)                     │  │
│  │  • Authentication Endpoints                           │  │
│  │  • Student Endpoints                                  │  │
│  │  • Company Endpoints                                  │  │
│  │  • Job Endpoints                                      │  │
│  │  • Application Endpoints                              │  │
│  │  • Interview Endpoints                                │  │
│  │  • Alumni Endpoints                                   │  │
│  └──────────────────────────────────────────────────────┘  │
│                            │                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           Security Layer (app/core/)                  │  │
│  │  • JWT Authentication                                 │  │
│  │  • Password Hashing (Bcrypt)                         │  │
│  │  • Role-Based Access Control                         │  │
│  └──────────────────────────────────────────────────────┘  │
│                            │                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Business Logic Layer (app/services/)          │  │
│  │  • Eligibility Checking                              │  │
│  │  • No-Show Tracking                                  │  │
│  │  • Application Workflow                              │  │
│  └──────────────────────────────────────────────────────┘  │
│                            │                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │            ORM Layer (app/models/)                    │  │
│  │  • SQLAlchemy Models                                 │  │
│  │  • Relationships                                     │  │
│  │  • Database Constraints                              │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ SQL Queries
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    POSTGRESQL DATABASE                       │
│                                                               │
│  • 17 Tables                                                 │
│  • Foreign Key Relationships                                 │
│  • Indexes for Performance                                   │
│  • ACID Compliance                                          │
└─────────────────────────────────────────────────────────────┘
```

## Database Schema Diagram

```
┌─────────────┐
│    users    │───┐
│ (auth)      │   │
└─────────────┘   │
                  │
     ┌────────────┼────────────┬────────────┬────────────┐
     │            │            │            │            │
     ▼            ▼            ▼            ▼            ▼
┌─────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌──────────┐
│students │  │recruiters│  │ cds_team │  │ alumni │  │ admin    │
└─────────┘  └──────────┘  └──────────┘  └────────┘  └──────────┘
     │             │                          │
     │             │                          │
     │        ┌────┴─────┐                    │
     │        │          │                    │
     │        ▼          │                    ▼
     │   ┌──────────┐   │              ┌───────────┐
     │   │companies │◄──┘              │ referrals │
     │   └──────────┘                  └───────────┘
     │        │                              │
     │        │                              │
     │        ▼                              ▼
     │   ┌──────┐                    ┌──────────────────┐
     │   │ jobs │                    │training_sessions │
     │   └──────┘                    └──────────────────┘
     │        │                              │
     │        │                              │
     └────────┼──────────────────────────────┘
              │
              ▼
     ┌──────────────────┐
     │ job_applications │
     └──────────────────┘
              │
              ▼
        ┌───────────┐
        │interviews │
        └───────────┘

Additional Tables:
• skills (many-to-many with students)
• certifications
• placement_drives
• question_bank
• notifications
• training_attendances
```

## User Role Hierarchy

```
                    ┌──────────┐
                    │  ADMIN   │ (Full Access)
                    └──────────┘
                         │
        ┌────────────────┼────────────────┐
        │                                 │
   ┌────────────┐                  ┌──────────────┐
   │CDS_MANAGER │                  │  CDS_TEAM    │
   │(Analytics) │                  │(Coordinator) │
   └────────────┘                  └──────────────┘
                                          │
        ┌─────────────────────────────────┼─────────────────┐
        │                                 │                 │
   ┌─────────┐                      ┌──────────┐      ┌────────┐
   │RECRUITER│                      │ STUDENT  │      │ ALUMNI │
   │(Hiring) │                      │(Applicant)│     │(Mentor)│
   └─────────┘                      └──────────┘      └────────┘
```

## Application Workflow

```
1. Student Registration
   ↓
2. Profile Setup
   ↓
3. Browse Eligible Jobs ←──────────┐
   ↓                                │
4. Submit Application               │
   ↓                                │
5. Eligibility Check (Auto)         │
   ↓                                │
6. Recruiter Review                 │
   ├─ Reject ─────────────────────→┤
   └─ Shortlist                     │
       ↓                            │
7. Schedule Interview               │
   ├─ No Show (penalty) ──────────→┤
   └─ Attended                      │
       ↓                            │
8. Feedback & Result                │
   ├─ Rejected ───────────────────→┤
   └─ Selected                      │
       ↓                            │
9. Offer Extended                   │
   ├─ Declined ───────────────────→┤
   └─ Accepted                      │
       ↓                            │
10. Placed Successfully            │
    └──────────────────────────────┘
```

## Request Flow

```
Client Request
     │
     ▼
FastAPI Endpoint
     │
     ▼
Authentication Middleware
     │
     ├─ Invalid Token → 401 Unauthorized
     ├─ No Permission → 403 Forbidden
     │
     ▼ Valid
Role-Based Access Control
     │
     ▼
Input Validation (Pydantic)
     │
     ├─ Invalid Data → 422 Validation Error
     │
     ▼ Valid
Business Logic Layer
     │
     ▼
Database Query (SQLAlchemy)
     │
     ├─ Not Found → 404 Not Found
     ├─ Conflict → 400 Bad Request
     │
     ▼ Success
Response Serialization
     │
     ▼
JSON Response
     │
     ▼
Client
```

## Security Layers

```
┌─────────────────────────────────────────┐
│ Layer 1: Input Validation (Pydantic)   │
│  • Type checking                        │
│  • Format validation                    │
│  • Required fields                      │
└─────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Layer 2: Authentication (JWT)          │
│  • Token verification                   │
│  • Expiration check                     │
│  • User existence                       │
└─────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Layer 3: Authorization (RBAC)          │
│  • Role verification                    │
│  • Permission checking                  │
│  • Resource ownership                   │
└─────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Layer 4: Data Access (ORM)             │
│  • SQL injection prevention             │
│  • Parameterized queries                │
│  • Transaction management               │
└─────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Layer 5: Database Security              │
│  • User permissions                     │
│  • Connection pooling                   │
│  • Encrypted connections                │
└─────────────────────────────────────────┘
```

## Component Interaction

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Student  │     │Recruiter │     │CDS Team  │
└─────┬────┘     └────┬─────┘     └────┬─────┘
      │               │                 │
      │ Login         │ Login           │ Login
      ├──────────────→│                 │
      │               ├────────────────→│
      │               │                 │
      │ View Jobs     │                 │
      ├──────────────→│                 │
      │               │                 │
      │ Apply         │                 │
      ├──────────────→│                 │
      │               │                 │
      │               │ Review Apps     │
      │               ├────────────────→│
      │               │                 │
      │               │ Schedule Int.   │
      │               ├────────────────→│
      │               │                 │
      │ Attend Int.   │                 │
      ├──────────────→│                 │
      │               │                 │
      │               │ Submit Feedback │
      │               ├────────────────→│
      │               │                 │
      │ Get Result    │                 │
      ←──────────────┤                 │
      │               │                 │
```

## Data Flow - Job Application

```
Student                    System                     Database
   │                         │                           │
   │ POST /applications/     │                           │
   ├────────────────────────→│                           │
   │                         │                           │
   │                         │ Validate Input            │
   │                         │ Check Authentication      │
   │                         │                           │
   │                         │ Get Student Profile       │
   │                         ├──────────────────────────→│
   │                         │←──────────────────────────┤
   │                         │                           │
   │                         │ Get Job Details           │
   │                         ├──────────────────────────→│
   │                         │←──────────────────────────┤
   │                         │                           │
   │                         │ Check Eligibility:        │
   │                         │  • GPA                    │
   │                         │  • Branch                 │
   │                         │  • Batch                  │
   │                         │  • Debarred Status        │
   │                         │                           │
   │                         │ Create Application        │
   │                         ├──────────────────────────→│
   │                         │←──────────────────────────┤
   │                         │                           │
   │←────────────────────────┤                           │
   │ 201 Created             │                           │
```

## Technology Stack Detail

```
┌─────────────────────────────────────────────┐
│              Frontend (Future)              │
│  • React/Vue.js/Angular                    │
│  • Mobile: Flutter/React Native            │
└─────────────────────────────────────────────┘
                    │ REST API
                    ▼
┌─────────────────────────────────────────────┐
│              Backend (Current)              │
│  FastAPI Framework                          │
│  ├─ Routing: APIRouter                     │
│  ├─ Validation: Pydantic                   │
│  ├─ Async: ASGI (Uvicorn)                 │
│  └─ Docs: OpenAPI (Swagger)               │
└─────────────────────────────────────────────┘
                    │ SQLAlchemy ORM
                    ▼
┌─────────────────────────────────────────────┐
│           Database (PostgreSQL)             │
│  • Version: 12+                            │
│  • Features: ACID, Relations, JSON         │
│  • Driver: psycopg2                        │
└─────────────────────────────────────────────┘
```

## Deployment Architecture (Future)

```
┌──────────────┐
│ Load         │
│ Balancer     │
└──────┬───────┘
       │
   ┌───┴────┬──────────┐
   │        │          │
   ▼        ▼          ▼
┌────┐  ┌────┐    ┌────┐
│App │  │App │    │App │  FastAPI
│ 1  │  │ 2  │... │ N  │  Instances
└─┬──┘  └─┬──┘    └─┬──┘
  │       │          │
  └───────┼──────────┘
          │
          ▼
   ┌────────────┐
   │ PostgreSQL │
   │   Master   │
   └──────┬─────┘
          │
     ┌────┴────┐
     │         │
     ▼         ▼
  ┌──────┐  ┌──────┐
  │Slave │  │Slave │
  │  1   │  │  2   │
  └──────┘  └──────┘
```

---

This architecture provides:
✅ Scalability
✅ Security
✅ Maintainability
✅ Performance
✅ Extensibility
