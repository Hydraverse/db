from typing import Optional

from hydb.db import DB
from hydb import db as models
from . import schemas


def user_get(db: DB, tg_user_id: int) -> Optional[models.User]:
    return db.Session.query(
        models.User
    ).where(
        models.User.tg_user_id == tg_user_id
    ).one_or_none()


def user_add(db: DB, user: schemas.UserCreate) -> models.User:
    return models.User.get(db, user.tg_user_id, create=True)


def user_del(db: DB, user_pk: int):
    u: models.User = db.Session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    ).one()

    u.delete(db)


def user_addr_add(db: DB, user: models.User, addr: schemas.UserAddrAdd) -> models.UserAddr:
    return user.addr_get(
        db=db,
        address=addr.address,
        create=True
    )


def user_addr_del(db: DB, user: models.User, addr: schemas.UserAddrDel) -> bool:
    return user.addr_del(
        db=db,
        addr_pk=addr.addr_pk
    )
