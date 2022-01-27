from typing import Optional

from hydb.db import DB
from hydb import db as models
from . import schemas


def server_info(db: DB) -> schemas.ServerInfo:
    return schemas.ServerInfo(mainnet=db.rpc.mainnet)


def user_get_by_tgid(db: DB, tg_user_id: int) -> Optional[models.User]:
    return db.Session.query(
        models.User
    ).where(
        models.User.tg_user_id == tg_user_id
    ).one_or_none()


def user_get_by_pkid(db: DB, user_pk: int) -> Optional[models.User]:
    return db.Session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    ).one_or_none()


def user_add(db: DB, user_create: schemas.UserCreate) -> models.User:
    return models.User.get(db, user_create.tg_user_id, create=True)


def user_del(db: DB, user_pk: int):
    u: models.User = db.Session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    ).one()

    u.delete(db)


def user_info_update(db: DB, user: models.User, update: schemas.UserInfoUpdate) -> schemas.UserInfoUpdate.Result:
    return user.update_info(db, update)


def user_addr_get(db: DB, user: models.User, address: str) -> Optional[models.UserAddr]:
    return user.addr_get(
        db=db,
        address=address,
        create=False
    )


def user_addr_add(db: DB, user: models.User, addr_add: schemas.UserAddrAdd) -> models.UserAddr:
    return user.addr_get(
        db=db,
        address=addr_add.address,
        create=True
    )


def user_addr_del(db: DB, user: models.User, user_addr_pk: int) -> schemas.DeleteResult:
    # noinspection PyArgumentList
    return schemas.DeleteResult(
        deleted=user.addr_del(
            db=db,
            user_addr_pk=user_addr_pk
        )
    )


def user_addr_token_add(db: DB, user_addr: models.UserAddr, addr_token_add: schemas.UserAddrTokenAdd) -> schemas.UserAddrTokenAdd.Result:
    return schemas.UserAddrTokenAdd.Result(
        **user_addr.token_addr_add(
            db=db,
            address=addr_token_add.address,
        )
    )


def user_addr_token_del(db: DB, user_addr: models.UserAddr, address: str) -> schemas.DeleteResult:
    # noinspection PyArgumentList
    return schemas.DeleteResult(
        deleted=user_addr.token_addr_del(
            db=db,
            address=address
        )
    )


def user_addr_hist_del(db: DB, user_addr: models.UserAddr, user_addr_hist_pk: int) -> schemas.DeleteResult:
    # noinspection PyArgumentList
    return schemas.DeleteResult(
        deleted=user_addr.addr_hist_del(
            db=db,
            user_addr_hist_pk=user_addr_hist_pk
        )
    )


def block_get(db: DB, block_pk: int) -> Optional[schemas.Block]:
    return db.Session.query(
        models.Block
    ).where(
        models.Block.pkid == block_pk
    ).one_or_none()


def block_sse_result(db: DB, block: models.Block) -> schemas.BlockSSEResult:
    return schemas.BlockSSEResult(
        block=block,
        hist=models.AddrHist.all_for_block(db, block)
    )
