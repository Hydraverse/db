from __future__ import annotations

from typing import List

from sqlalchemy import Column, ForeignKey, Integer, and_, DateTime, Boolean
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .addr_hist import AddrHist
from .block import Block

__all__ = "UserAddrHist",


class UserAddrHist(Base):
    __tablename__ = "user_addr_hist"

    pkid = DbPkidColumn()
    user_addr_pk = Column(Integer, ForeignKey("user_addr.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    addr_hist_pk = Column(Integer, ForeignKey("addr_hist.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    date_create = DbDateCreateColumn()
    block_t = Column(DateTime, nullable=True)
    block_c = Column(Integer, nullable=False)
    data = DbDataColumn()

    # user = relationship("User")
    # addr = relationship("Addr")

    user_addr = relationship(
        "UserAddr",
        back_populates="user_addr_hist",
    )

    addr_hist = relationship(
        "AddrHist",
        back_populates="addr_hist_user",
    )

    def _remove(self, db: DB, user_addr_hist: List[UserAddrHist]):
        addr_hist = self.addr_hist
        user_addr_hist.remove(self)
        addr_hist._removed_user(db)

    def on_fork(self, db: DB):
        self.user_addr.on_fork(db, self)

    @staticmethod
    def all_for_block(db: DB, block: Block) -> List[UserAddrHist]:
        uahs: List[UserAddrHist] = db.Session.query(
            UserAddrHist
        ).join(
            AddrHist,
            and_(
                AddrHist.pkid == UserAddrHist.addr_hist_pk,
                AddrHist.block_pk == block.pkid,
            )
        ).all()

        return uahs

