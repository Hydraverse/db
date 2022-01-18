from __future__ import annotations
from typing import Optional, Generator

from attrdict import AttrDict
from hydra import log
from sqlalchemy import Column, Integer, String, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship, lazyload

from .base import *
from .db import DB
from .addr import Addr, Smac, Tokn, NFT
from .user_uniq import UserUniq, DbUserUniqMixin
from .user_addr import UserAddr
from .user_addr_tx import UserAddrTX
from .tokn_addr import ToknAddr

__all__ = "User", "UserUniq", "UserAddr", "UserAddrTX"


@dictattrs("pkid", "uniq", "tg_user_id", "info", "data")
class User(DbUserUniqMixin, Base):
    __tablename__ = "user"
    __table_args__ = (
        DbInfoColumnIndex(__tablename__),
    )

    tg_user_id = Column(Integer, nullable=False, unique=True, primary_key=False, index=True)

    info = DbInfoColumn()
    data = DbDataColumn()

    user_addrs = relationship(
        UserAddr,
        back_populates="user",
        cascade="all, delete-orphan",
        single_parent=True,
        primaryjoin="""and_(
            UserAddr.user_pk == User.pkid,
            UserAddr.addr_pk == Addr.pkid,
            Addr.addr_tp == 'H'
        )"""
    )

    user_tokns = relationship(
        UserAddr,
        back_populates="user",
        cascade="all, delete-orphan",
        single_parent=True,
        primaryjoin="""and_(
            UserAddr.user_pk == User.pkid,
            UserAddr.addr_pk == Addr.pkid,
            or_(
                Addr.addr_tp == 'T',
                Addr.addr_tp == 'N',
            ),
        )""",
        overlaps="user_addrs",
    )

    user_addr_txes = relationship(
        "UserAddrTX",
        back_populates="user",
        cascade="all, delete-orphan",
        single_parent=True
    )

    def user_addrs_by_type(self, addr_tp: Addr.Type):
        return (
            self.user_addrs if addr_tp == Addr.Type.H else
            self.user_tokns
        )

    def __str__(self):
        return f"{self.pkid} [{self.uniq.name}] {self.tg_user_id}"

    def asdict(self, full=False) -> AttrDict:
        user_dict = super().asdict()

        if full:
            user_dict.user_addrs = list(
                ua.asdict()
                for ua in self.user_addrs
            )

            user_dict.user_tokns = list(
                ut.asdict()
                for ut in self.user_tokns
            )

        return user_dict

    @staticmethod
    async def get_pkid(db: DB, user_id: int) -> Optional[int]:
        return await db.run_in_executor_session(User._get_pkid, db, user_id)

    @staticmethod
    def _get_pkid(db: DB, user_id: int) -> Optional[int]:
        u = (
            db.Session.query(
                User.pkid
            ).filter(
                User.tg_user_id == user_id
            ).one_or_none()
        )

        return u.pkid if u is not None else None

    @staticmethod
    async def get(db: DB, user_id: int, create: bool = True, full: bool = False) -> Optional[AttrDict]:
        return await db.run_in_executor_session(User._get, db, user_id, create, full)

    @staticmethod
    def _get(db: DB, user_id: int, create: bool, full: bool) -> Optional[AttrDict]:

        u: User = db.Session.query(
            User
        ).filter(
            User.tg_user_id == user_id
        ).one_or_none()

        if u is not None:
            return u.asdict(full=full)

        elif not create:
            return None

        while True:
            uniq = UserUniq(db)

            try:
                db.Session.add(uniq)
                db.Session.commit()
                break
            except IntegrityError:
                db.Session.rollback()
                log.error("User unique PKID name clash! Trying again.")
                continue

        user_ = User(
            uniq=uniq,
            tg_user_id=user_id,
        )

        db.Session.add(user_)
        db.Session.commit()

        return user_.asdict(full=full)

    def on_new_addr_tx(self, db: DB, user_addr_tx: UserAddrTX) -> bool:
        # TODO: Also process and notify for all token TXes if user chooses
        return user_addr_tx.addr_tx.addr.addr_tp == Addr.Type.H

    @staticmethod
    async def update_info(db: DB, user_pk: int, info: dict, data: dict = None, over: bool = False) -> None:
        if (info is None and data is None) or (over and (not info or not data)):
            return
        return await db.run_in_executor_session(User._update_info, db, user_pk, info, data, over)

    @staticmethod
    def _update_info(db: DB, user_pk: int, info: dict, data: dict, over: bool) -> None:
        u: User = db.Session.query(User).where(
            User.pkid == user_pk,
        ).options(
            lazyload(User.user_addrs),
        ).one()

        if over:
            if info is not None:
                u.info = info
            if data is not None:
                u.data = data
        else:
            if info is not None:
                u.info.update(info)
            if data is not None:
                u.data.update(data)

        db.Session.add(u)
        db.Session.commit()

    @staticmethod
    async def delete(db: DB, user_id: int) -> None:
        return await db.run_in_executor_session(User.__delete, db, user_id)

    @staticmethod
    def __delete(db: DB, user_id: int) -> None:
        u: User = db.Session.query(User).where(
            User.tg_user_id == user_id,
        ).one_or_none()

        if u is not None:
            return u.___delete(db)
        
    def ___delete(self, db: DB):
        for user_addr_tx in list(self.user_addr_txes):
            user_addr_tx._remove(db, self.user_addr_txes)

        for user_addr_tokn in list(self.user_tokns):
            user_addr_tokn._remove(db, self.user_tokns)

        for user_addr in list(self.user_addrs):
            user_addr._remove(db, self.user_addrs)

        db.Session.delete(self)
        db.Session.commit()

    @staticmethod
    async def addr_add(db: DB, user_pk: int, address: str) -> AttrDict:
        return await db.run_in_executor_session(User._addr_add, db, user_pk, address)

    @staticmethod
    def _addr_add(db: DB, user_pk: int, address: str) -> AttrDict:
        u: User = db.Session.query(
            User,
        ).where(
            User.pkid == user_pk,
        ).options(
            lazyload(User.user_addrs),
        ).one()

        user_addr: UserAddr = u.__addr_add(db, address)
        db.Session.commit()
        return user_addr.asdict()

    def __addr_add(self, db, address: str) -> UserAddr:
        addr: [Addr, Smac, Tokn, NFT] = Addr.get(db, address, create=True)

        ua = UserAddr(user=self, addr=addr)
        db.Session.add(ua)

        if isinstance(addr, Tokn):  # Includes NFT
            self.__on_new_user_tokn(db, ua)
        else:
            self.__on_new_user_addr(db, ua)

        return ua

    def __on_new_user_tokn(self, db: DB, user_tokn_addr: UserAddr):
        for tokn_addr in self.enumerate_user_tokn_addrs(db, user_tokn_addr):
            tokn_addr.update_balance(db)

    def __on_new_user_addr(self, db: DB, user_addr: UserAddr):
        for tokn_addr in self.enumerate_user_addr_tokns(db, user_addr):
            tokn_addr.update_balance(db)

    @staticmethod
    async def addr_del(db: DB, user_pk: int, address: str) -> Optional[AttrDict]:
        return await db.run_in_executor_session(User._addr_del, db, user_pk, address)

    @staticmethod
    def _addr_del(db: DB, user_pk: int, address: str) -> Optional[AttrDict]:
        addr: [Addr, Smac, Tokn, NFT] = Addr.get(db, address, create=False)

        if addr is not None:
            ua: UserAddr = db.Session.query(
                UserAddr,
            ).where(
                and_(
                    UserAddr.user_pk == user_pk,
                    UserAddr.addr_pk == addr.pkid,
                )
            ).one_or_none()

            if ua is not None:
                ua_dict = ua.asdict()
                ua._remove(db, ua.user.user_addrs_by_type(addr.addr_tp))
                db.Session.commit()
                return ua_dict

    def enumerate_user_tokn_addrs(self, db: DB, user_addr_tokn: UserAddr) -> Generator[ToknAddr]:
        for user_addr in self.user_addrs:
            yield user_addr_tokn.get_addr_tokn(db, user_addr.addr, create=True)

    def enumerate_user_addr_tokns(self, db: DB, user_addr: UserAddr) -> Generator[ToknAddr]:
        for user_addr_tokn in self.user_tokns:
            yield user_addr_tokn.get_addr_tokn(db, user_addr.addr, create=True)
