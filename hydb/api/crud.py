import json
from typing import Optional, List

from deepdiff import DeepDiff
from sqlalchemy import func, and_

from hydb.db import DB
from hydb import db as models
from . import schemas


def server_info(db: DB) -> schemas.ServerInfo:
    return schemas.ServerInfo(mainnet=db.rpc.mainnet)


def user_map(db: DB) -> schemas.UserMap:
    users: List[models.User] = db.session.query(
        models.User
    ).all()

    return schemas.UserMap(
        map={
            u.tg_user_id: u.uniq.pkid
            for u in users
        }
    )


def user_get_by_tgid(db: DB, tg_user_id: int) -> Optional[models.User]:
    return db.session.query(
        models.User
    ).where(
        models.User.tg_user_id == tg_user_id
    ).one_or_none()


def user_get_by_pkid(db: DB, user_pk: int) -> Optional[models.User]:
    return db.session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    ).one_or_none()


def user_add(db: DB, user_create: schemas.UserCreate) -> models.User:
    return models.User.get(db, user_create.tg_user_id, create=True)


def user_del(db: DB, user_pk: int):
    u: models.User = db.session.query(
        models.User
    ).where(
        models.User.pkid == user_pk
    ).one()

    u.delete(db)


def user_info_update(db: DB, user: models.User, update: schemas.UserInfoUpdate) -> schemas.UserInfoUpdate.Result:
    return user.update_info(db, update)


def user_addr_get(db: DB, user_pk: int, user_addr_pk: int) -> Optional[models.UserAddr]:
    return db.session.query(
        models.UserAddr,
    ).where(
        and_(
            models.UserAddr.pkid == user_addr_pk,
            models.UserAddr.user_pk == user_pk,
        ),
    ).one_or_none()


def user_addr_add(db: DB, user: models.User, addr_add: schemas.UserAddrAdd) -> models.UserAddr:
    return user.addr_get(
        db=db,
        address=addr_add.address,
        create=addr_add.name or True
    )


def user_addr_update(db: DB, user_addr: models.UserAddr, addr_update: schemas.UserAddrUpdate) -> schemas.UserAddrUpdate.Result:
    updated = False

    if addr_update.name is not None and schemas.UserAddrUpdate.validate_name(addr_update.name) \
            and user_addr.user.addr_name_available(addr_update.name):
        user_addr.name = addr_update.name
        updated = True

    # noinspection DuplicatedCode
    if addr_update.info is not None:
        if addr_update.over is True:
            user_addr.info = addr_update.info
            updated = True
        else:
            info = dict(user_addr.info)
            info.update(addr_update.info)

            if DeepDiff(dict(user_addr.info), info):
                user_addr.info = info
                updated = True

    # noinspection DuplicatedCode
    if addr_update.data is not None:
        if addr_update.over is True:
            user_addr.data = addr_update.data
            updated = True
        else:
            data = dict(user_addr.data)
            data.update(addr_update.data)

            if DeepDiff(dict(user_addr.data), data):
                user_addr.data = data
                updated = True

    if updated:
        db.session.add(user_addr)
        db.session.commit()

    return schemas.UserAddrUpdate.Result(
        updated=updated
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


def block_get(db: DB, block_pk: int) -> Optional[schemas.Block]:
    return db.session.query(
        models.Block
    ).where(
        models.Block.pkid == block_pk
    ).one_or_none()


def sse_event_add(db: DB, event: str, data: schemas.BaseModel) -> models.Event:
    ev = models.Event(
        event=event,
        data=data.json(encoder=str)
    )

    db.session.add(ev)
    db.session.commit()
    db.session.refresh(ev)
    return ev


def block_sse_result(db: DB, block: models.Block, event: schemas.SSEBlockEvent) -> schemas.BlockSSEResult:
    return schemas.BlockSSEResult(
        id=(db.session.query(func.max(models.Event.pkid)).scalar() or 0) + 1,  # Best guess, but monoticity is all we really need here.
        event=event,
        block=block,
        hist=models.AddrHist.all_for_block(db, block)
    )


def block_sse_event_add(db: DB, block: models.Block, event: schemas.SSEBlockEvent) -> models.Event:
    return sse_event_add(
        db=db,
        event="block",
        data=block_sse_result(db, block, event)
    )


def stats_get(db: DB) -> Optional[schemas.Stats]:
    stats = models.Stat.current(db)

    if stats is None:
        return None

    return schemas.Stats(
        current=stats,
        quant_stat_1d=models.StatQuantView1d.get(db),
        quant_net_weight=models.StatQuantNetWeightView.get(db)
    )
