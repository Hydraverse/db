from __future__ import annotations

from typing import Optional

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

    pkid = DbPkidColumn()
    user_pk = Column(Integer, ForeignKey("user.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    date_create = DbDateCreateColumn()
    date_update = DbDateUpdateColumn()
    block_c = Column(Integer, nullable=False, default=0)
    token_l = DbDataColumn(default=[])

    user = relationship("User", back_populates="user_addrs")
    addr = relationship("Addr", back_populates="addr_users")

    user_addr_hist = relationship(
        "UserAddrHist",
        back_populates="user_addr",
        cascade="all, delete-orphan",
        single_parent=True,
    )

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

    def _remove(self, db: DB, user_addrs):
        addr = self.addr
        user_addrs.remove(self)
        addr._removed_user(db)

    @staticmethod
    def get(db: DB, user, address: str, create=True) -> Optional[UserAddr]:
        addr: Addr = Addr.get(db, address, create=create)

        if addr is None:
            return None

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
