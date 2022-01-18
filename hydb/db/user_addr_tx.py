from __future__ import annotations

from sqlalchemy import Column, ForeignKey, Integer, Index
from sqlalchemy.orm import relationship

from .base import *
from .db import DB

__all__ = "UserAddrTX",


@dictattrs("user", "addr_tx")
class UserAddrTX(Base):
    __tablename__ = "user_addr_tx"

    user_pk = Column(Integer, ForeignKey("user.pkid", ondelete="CASCADE"), primary_key=True, index=True, nullable=False)
    addr_tx_pk = Column(Integer, ForeignKey("addr_tx.pkid", ondelete="CASCADE"), primary_key=True, index=True, nullable=False)

    user = relationship("User", back_populates="user_addr_txes", foreign_keys=[user_pk])
    addr_tx = relationship(
        "AddrTX",
        back_populates="addr_tx_users"
    )

    def on_new_addr_tx(self, db: DB) -> bool:
        if not self.user.on_new_addr_tx(db, self):
            self.user.user_addr_txes.remove(self)
            return False

        return True

    def _remove(self, db: DB, user_addr_txes):
        addr_tx = self.addr_tx
        user_addr_txes.remove(self)
        addr_tx._removed_user(db)


Index(UserAddrTX.__tablename__ + "_idx", UserAddrTX.user_pk, UserAddrTX.addr_tx_pk)
