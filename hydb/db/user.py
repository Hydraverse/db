from __future__ import annotations
from typing import Optional, Union

from attrdict import AttrDict
from deepdiff import DeepDiff

from sqlalchemy import Column, Integer, BigInteger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship

from hydra import log

from .base import *
from .db import DB
from .user_uniq import UserUniq, DbUserUniqPkidColumn, DbUserUniqRelationship
from .user_addr import UserAddr, UserAddrHist
from ..api import schemas

__all__ = "User", "UserUniq", "UserAddr", "UserAddrHist"


class User(Base):
    __tablename__ = "user"
    __table_args__ = (
        DbInfoColumnIndex(__tablename__),
    )

    pkid = DbUserUniqPkidColumn()
    date_update = DbDateUpdateColumn()

    tg_user_id = Column(BigInteger, nullable=False, unique=True, primary_key=False, index=True)

    info = DbInfoColumn()
    data = DbDataColumn()

    uniq = DbUserUniqRelationship()

    user_addrs = relationship(
        UserAddr,
        order_by=UserAddr.pkid,
        back_populates="user",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def __str__(self):
        return f"{self.pkid} [{self.uniq.name}] {self.tg_user_id}"

    @staticmethod
    def get_pkid(db: DB, tg_user_id: int) -> Optional[int]:
        u = (
            db.Session.query(
                User.pkid
            ).filter(
                User.tg_user_id == tg_user_id
            ).one_or_none()
        )

        return u.pkid if u is not None else None

    @staticmethod
    def get(db: DB, tg_user_id: int, create: bool = False) -> Optional[User]:

        u: User = db.Session.query(
            User
        ).filter(
            User.tg_user_id == tg_user_id
        ).one_or_none()

        if u is not None:
            return u

        elif not create:
            return None

        while True:
            uniq = UserUniq(db)

            try:
                db.Session.add(uniq)
                db.Session.commit()
                db.Session.refresh(uniq)
                break
            except IntegrityError:
                db.Session.rollback()
                log.error("User unique PKID name clash! Trying again.")
                continue

        # noinspection PyArgumentList
        user_ = User(
            uniq=uniq,
            tg_user_id=tg_user_id,
        )

        db.Session.add(user_)
        db.Session.commit()
        db.Session.refresh(user_)

        return user_

    # noinspection PyUnusedLocal
    def on_verified_block(self, db: DB, user_addr: UserAddr):
        # TODO: More detailed recording here.
        self.info["block_c"] = self.info.get("block_c", 0) + 1
        db.Session.add(self)

    def on_block(self, db: DB, user_addr: UserAddr):
        pass

    @staticmethod
    def delete_by_id(db: DB, tg_user_id: int) -> None:
        u: User = db.Session.query(User).where(
            User.tg_user_id == tg_user_id,
        ).one_or_none()

        if u is not None:
            return u.delete(db)
        
    def delete(self, db: DB):
        for user_addr in list(self.user_addrs):
            user_addr._remove(db, self.user_addrs)

        db.Session.delete(self)
        db.Session.commit()

    def addr_name_available(self, name: str) -> bool:
        return name not in [ua.name for ua in self.user_addrs]

    def update_info(self, db: DB, update: schemas.UserInfoUpdate) -> schemas.UserInfoUpdate.Result:
        changed = False

        if update.over:
            if DeepDiff(dict(self.info), dict(update.info)):
                self.info = update.info
                changed = True
        else:
            info = dict(self.info)
            info.update(update.info)

            if DeepDiff(dict(self.info), info):
                self.info = info
                changed = True

        if changed:
            db.Session.add(self)
            db.Session.commit()
            db.Session.refresh(self)

        return schemas.UserInfoUpdate.Result(updated=changed)

    def addr_get(self, db: DB, address: str, create: Union[bool, str] = True) -> Optional[UserAddr]:
        return UserAddr.get(db=db, user=self, address=address, create=create)

    def addr_del(self, db: DB, user_addr_pk: int) -> bool:
        ua: Optional[UserAddr] = db.Session.query(
            UserAddr
        ).where(
            UserAddr.pkid == user_addr_pk
        ).one_or_none()

        if ua is not None:
            ua._remove(db, self.user_addrs)
            db.Session.commit()
            return True

        return False
