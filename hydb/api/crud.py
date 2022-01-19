from typing import Optional

from hydb.db import DB
from hydb import db as models
from . import schemas


def get_user_by_pkid(db: DB, user_pk: int) -> Optional[models.User]:
    return db.Session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    ).one_or_none()


def get_user_by_tg_id(db: DB, tg_user_id: int) -> Optional[models.User]:
    return db.Session.query(
        models.User
    ).where(
        models.User.tg_user_id == tg_user_id
    ).one_or_none()


def create_user(db: DB, user: schemas.UserCreate) -> models.User:
    return models.User.get(db, user.tg_user_id, create=True)


def delete_user(db: DB, user_pk: int):
    u: models.User = db.Session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    ).one()

    u.delete(db)
