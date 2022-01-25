from __future__ import annotations

from typing import List

from sqlalchemy import Column, ForeignKey, Integer, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship

from .base import *
from .db import DB

__all__ = "UserAddrHist",


class UserAddrHist(Base):
    __tablename__ = "user_addr_hist"

    pkid = DbPkidColumn()
    user_addr_pk = Column(Integer, ForeignKey("user_addr.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    addr_hist_pk = Column(Integer, ForeignKey("addr_hist.pkid", ondelete="CASCADE"), primary_key=True, nullable=False)
    date_create = DbDateCreateColumn()
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

