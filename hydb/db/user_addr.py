from __future__ import annotations

from typing import Optional, List, Dict

from sqlalchemy import Column, ForeignKey, Integer, and_, UniqueConstraint
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .addr import Addr
from .addr_hist import AddrHist
from .user_addr_hist import UserAddrHist

__all__ = "UserAddr", "UserAddrHist"


class UserAddr(Base):
    __tablename__ = "user_addr"

    pkid = DbPkidColumn(seq="user_addr_seq")
    user_pk = Column(Integer, ForeignKey("user.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    date_create = DbDateCreateColumn()
    date_update = DbDateUpdateColumn()
    block_c = Column(Integer, nullable=False, default=0)
    token_l = DbDataColumn(default={})

    user = relationship("User", back_populates="user_addrs")
    addr = relationship("Addr", back_populates="addr_users")

    user_addr_hist = relationship(
        UserAddrHist,
        back_populates="user_addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def token_addr_add(self, db: DB, address: str) -> Dict[str, dict]:
        addr_tp, addr_hx, addr_hy, sc_info = Addr.normalize(db, address)

        if addr_tp not in (Addr.Type.T, Addr.Type.N):
            return self.token_l

        if addr_hx in self.token_l:
            return self.token_l

        self.token_l[addr_hx] = sc_info
        db.Session.add(self)
        db.Session.commit()
        db.Session.refresh(self)
        return self.token_l

    def token_addr_del(self, db: DB, address: str) -> bool:
        addr_tp, addr_hx, addr_hy, _ = Addr.normalize(db, address)

        if addr_tp not in (Addr.Type.T, Addr.Type.N):
            return False

        if addr_hx not in self.token_l:
            return False

        del self.token_l[addr_hx]
        db.Session.add(self)
        db.Session.commit()
        return True

    def on_new_addr_hist(self, db: DB, addr_hist: AddrHist):
        user_addr_hist = UserAddrHist(
            user_addr=self,
            addr_hist=addr_hist,
            block_c=self.block_c,
        )

        db.Session.add(user_addr_hist)

        if addr_hist.block.info.get("miner", "") == self.addr.addr_hy:
            self.block_c += 1
            db.Session.add(self)

    def addr_hist_del(self, db: DB, user_addr_hist_pk: int) -> bool:
        uah: Optional[UserAddrHist] = db.Session.query(
            UserAddrHist
        ).where(
            and_(
                UserAddrHist.user_addr_pk == self.pkid,
                UserAddrHist.addr_hist_pk == user_addr_hist_pk
            )
        ).one_or_none()

        if uah is not None:
            uah._remove(db, self.user_addr_hist)
            db.Session.commit()
            return True

        return False

    def _remove(self, db: DB, user_addrs):
        addr = self.addr
        user_addrs.remove(self)
        addr._removed_user(db)

    @staticmethod
    def get_by_addr(db: DB, user, addr: Addr, create=True) -> Optional[UserAddr]:
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
    def get(db: DB, user, address: str, create=True) -> Optional[UserAddr]:
        addr: Addr = Addr.get(db, address, create=create)

        if addr is None:
            return None

        return UserAddr.get_by_addr(db, user, addr, create=create)

