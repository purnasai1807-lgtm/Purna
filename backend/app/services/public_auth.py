from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.security import hash_password, verify_password
from app.db.models import User


def ensure_public_user(db: Session) -> User:
    email = settings.public_access_email.strip().lower()
    full_name = settings.public_access_full_name.strip() or "Public Workspace"
    password = settings.public_access_password

    user = db.scalar(select(User).where(User.email == email))
    if user:
        changed = False

        if user.full_name != full_name:
            user.full_name = full_name
            changed = True

        if not verify_password(password, user.password_hash):
            user.password_hash = hash_password(password)
            changed = True

        if changed:
            db.add(user)
            db.commit()
            db.refresh(user)

        return user

    user = User(
        email=email,
        full_name=full_name,
        password_hash=hash_password(password),
    )
    db.add(user)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing_user = db.scalar(select(User).where(User.email == email))
        if existing_user:
            return existing_user
        raise

    db.refresh(user)
    return user
