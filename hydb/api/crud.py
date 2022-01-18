from hydb.db import DB
from hydb import db as models
from . import schemas


def get_user_by_pkid(db: DB, user_pk: int) -> models.User:
    return db.Session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    )


def get_user_by_tg_id(db: DB, tg_user_id: int) -> models.User:
    return db.Session.query(
        models.User
    ).where(
        models.User.tg_user_id == tg_user_id
    )


def get_user_by_tg_at(db: DB, tg_user_at: str) -> models.User:
    return db.Session.query(
        models.User
    ).where(
        models.User.tg_user_at == tg_user_at
    )

