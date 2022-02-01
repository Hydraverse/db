from __future__ import annotations

from typing import Optional, Union

from attrdict import AttrDict
from datetime import datetime
from sqlalchemy import Column, ForeignKey, Integer, and_, DateTime, String
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .addr import Addr
from .addr_hist import AddrHist
from .user_addr_hist import UserAddrHist

from ..util import namegen

__all__ = "UserAddr", "UserAddrHist"


class UserAddr(Base):
    __tablename__ = "user_addr"

    pkid = DbPkidColumn(seq="user_addr_seq")
    user_pk = Column(Integer, ForeignKey("user.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    date_create = DbDateCreateColumn()
    date_update = DbDateUpdateColumn()
    name = Column(String, nullable=False)
    block_t = Column(DateTime, nullable=True)
    block_c = Column(Integer, nullable=False, default=0)
    token_l = DbDataColumn(default=[])
    info = DbInfoColumn()
    data = DbDataColumn()

    user = relationship("User", back_populates="user_addrs")
    addr = relationship("Addr", back_populates="addr_users")

    user_addr_hist = relationship(
        UserAddrHist,
        order_by=UserAddrHist.pkid,
        back_populates="user_addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def token_addr_add(self, db: DB, address: str) -> AttrDict:
        addr_tp, addr_hx, addr_hy, addr_info = Addr.normalize(db, address)

        addr_info.addr_tp = addr_tp
        addr_info.addr_hx = addr_hx
        addr_info.addr_hy = addr_hy
        addr_info.added = False

        if addr_tp in (Addr.Type.T, Addr.Type.N) and addr_hx not in self.token_l:
            self.token_l.append(addr_hx)
            db.Session.add(self)
            db.Session.commit()
            addr_info.added = True

        return addr_info

    def token_addr_del(self, db: DB, address: str) -> bool:
        addr_tp, addr_hx, addr_hy, _ = Addr.normalize(db, address)

        if addr_tp not in (Addr.Type.T, Addr.Type.N):
            return False

        if addr_hx not in self.token_l:
            return False

        self.token_l.remove(addr_hx)
        db.Session.add(self)
        db.Session.commit()
        return True

    def on_new_addr_hist(self, db: DB, addr_hist: AddrHist):
        user_addr_hist = UserAddrHist(
            user_addr=self,
            addr_hist=addr_hist,
            block_t=self.block_t,
            block_c=self.block_c,
        )

        db.Session.add(user_addr_hist)

        if addr_hist.mined:
            self.block_t = datetime.utcfromtimestamp(addr_hist.block.info.get("timestamp"))
            self.block_c += 1
            self.user.block_c += 1
            db.Session.add(self)
            db.Session.add(self.user)

    def addr_hist_del(self, db: DB, user_addr_hist_pk: int) -> bool:
        for uah in self.user_addr_hist:
            if uah.pkid == user_addr_hist_pk:
                uah._remove(db, self.user_addr_hist)
                return True

        return False

    def _remove(self, db: DB, user_addrs):
        addr = self.addr
        user_addrs.remove(self)
        addr._removed_user(db)

    @staticmethod
    def get_by_addr(db: DB, user, addr: Addr, create: Union[bool, str] = True) -> Optional[UserAddr]:
        try:
            q = db.Session.query(UserAddr).where(
                and_(
                    UserAddr.user_pk == user.pkid,
                    UserAddr.addr_pk == addr.pkid,
                    )
            )

            if not create:
                return q.one_or_none()

            return q.one()

        except NoResultFound:
            # noinspection PyArgumentList
            ua = UserAddr(user=user, addr=addr)

            if isinstance(create, str):
                ua.name = create
            else:
                ua.name = namegen.make_single_name()

            while 1:
                for user_addr in filter(lambda ua_: ua_ != ua, user.user_addrs):
                    if ua.name == user_addr.name:
                        ua.name += " " + namegen.make_single_name()
                        break
                else:
                    break

            db.Session.add(ua)
            db.Session.commit()
            db.Session.refresh(ua)
            return ua

    @staticmethod
    def get_by_addr_pk(db: DB, user, addr_pk: int, create=True) -> Optional[UserAddr]:
        addr: Addr = db.Session.query(
            Addr
        ).where(
            Addr.pkid == addr_pk
        ).one_or_none()

        if addr is None:
            return None

        return UserAddr.get_by_addr(db, user, addr, create=create)

    @staticmethod
    def get(db: DB, user, address: str, create: Union[bool, str] = True) -> Optional[UserAddr]:
        addr: Addr = Addr.get(db, address, create=bool(create))

        if addr is None:
            return None

        return UserAddr.get_by_addr(db, user, addr, create=create)

