from datetime import datetime
from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class AuthResponse(BaseModel):
    message: str
    username: str
    role: str
    expiry: datetime


class CompanyCreate(BaseModel):
    company_name: str = Field(min_length=1)
    domain: str | None = None


class CompanyUpdate(BaseModel):
    company_name: str | None = None
    domain: str | None = None


class JobCreate(BaseModel):
    company_id: int
    title: str = Field(min_length=1)
    location: str | None = None
    min_cpi: float | None = None
    deadline: str | None = None


class JobUpdate(BaseModel):
    title: str | None = None
    location: str | None = None
    min_cpi: float | None = None
    deadline: str | None = None


class ApplicationCreate(BaseModel):
    job_id: int
    student_id: int | None = None
    status: str = "applied"


class ApplicationUpdate(BaseModel):
    status: str = Field(min_length=1)


class MemberUpdate(BaseModel):
    bio: str | None = None
    skills: str | None = None
    portfolio_visibility: str | None = None


class MemberCreate(BaseModel):
    username: str = Field(min_length=1)
    email: str = Field(min_length=3)
    password: str = Field(min_length=1)
    full_name: str = Field(min_length=1)
    role_name: str = "Student"
    is_active: bool = True
    latest_cpi: float | None = None
    program: str | None = None
    discipline: str | None = None
    graduating_year: int | None = None
    active_backlogs: int = 0
    bio: str | None = None
    skills: str | None = None
    portfolio_visibility: str = "private"
    group_ids: list[int] = []


class GroupCreate(BaseModel):
    group_name: str = Field(min_length=1)


class GroupMembershipRequest(BaseModel):
    member_id: int
