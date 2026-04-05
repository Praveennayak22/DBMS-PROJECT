import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import Header, HTTPException, status

from .db import execute, fetch_one

BASE_DIR = Path(__file__).resolve().parents[1]
LOG_PATH = BASE_DIR / "logs" / "audit.log"

SESSION_HOURS = 8
ADMIN_ROLES = {"admin", "CDS Manager"}
CDS_ROLES = ADMIN_ROLES | {"CDS Team"}
RECRUITER_ROLES = ADMIN_ROLES | {"Recruiter"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def generate_session_token() -> str:
    return secrets.token_urlsafe(32)


def log_audit(actor_user_id: Optional[int], action: str, table_name: str, record_id: Optional[str], request_path: str, status_value: str):
    line = f"{utc_now().isoformat()} actor={actor_user_id} action={action} table={table_name} record={record_id} path={request_path} status={status_value}"
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    execute(
        """
        INSERT INTO audit_logs(actor_user_id, action, table_name, record_id, request_path, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (actor_user_id, action, table_name, record_id, request_path, status_value),
    )


def authenticate_user(username: str, password: str):
    user = fetch_one(
        """
        SELECT u.user_id, u.username, u.password_hash, r.role_name AS role, u.is_active
        FROM users u
        JOIN roles r ON u.role_id = r.role_id
        WHERE u.username = ?
        """,
        (username,),
    )
    if not user:
        return None
    if not user["is_active"]:
        return None
    if password != user["password_hash"]:
        return None
    return user


def create_session(user_id: int) -> str:
    token = generate_session_token()
    expires_at = utc_now() + timedelta(hours=SESSION_HOURS)
    execute(
        "INSERT INTO sessions(session_token, user_id, expires_at) VALUES (?, ?, ?)",
        (token, user_id, expires_at.isoformat()),
    )
    return token


def get_session_user(session_token: str):
    session = fetch_one(
        """
        SELECT s.session_token, s.expires_at, u.user_id, u.username, r.role_name AS role, u.is_active
        FROM sessions s
        JOIN users u ON s.user_id = u.user_id
        JOIN roles r ON u.role_id = r.role_id
        WHERE s.session_token = ?
        """,
        (session_token,),
    )
    if not session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid session token")

    expiry = datetime.fromisoformat(session["expires_at"])
    if expiry < utc_now():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired")

    if not session["is_active"]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Inactive user")

    return session


def current_user_dependency(x_session_token: Optional[str] = Header(default=None)):
    if not x_session_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No session found")
    return get_session_user(x_session_token)


def require_admin(user_row, request_path: Optional[str] = None, table_name: str = "authorization"):
    if user_row["role"] not in ADMIN_ROLES:
        if request_path:
            actor_user_id = user_row["user_id"] if "user_id" in user_row.keys() else None
            log_audit(
                actor_user_id,
                "DENY",
                table_name,
                None,
                request_path,
                "forbidden",
            )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")


def is_admin_user(user_row) -> bool:
    return user_row["role"] in ADMIN_ROLES


def is_cds_user(user_row) -> bool:
    return user_row["role"] in CDS_ROLES


def is_recruiter_user(user_row) -> bool:
    return user_row["role"] in RECRUITER_ROLES


def require_cds_access(user_row, request_path: Optional[str] = None, table_name: str = "authorization"):
    if user_row["role"] not in CDS_ROLES:
        if request_path:
            actor_user_id = user_row["user_id"] if "user_id" in user_row.keys() else None
            log_audit(
                actor_user_id,
                "DENY",
                table_name,
                None,
                request_path,
                "forbidden",
            )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="CDS role required")


def require_recruiter_access(user_row, request_path: Optional[str] = None, table_name: str = "authorization"):
    if user_row["role"] not in RECRUITER_ROLES:
        if request_path:
            actor_user_id = user_row["user_id"] if "user_id" in user_row.keys() else None
            log_audit(
                actor_user_id,
                "DENY",
                table_name,
                None,
                request_path,
                "forbidden",
            )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Recruiter role required")
