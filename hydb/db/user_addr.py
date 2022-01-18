from __future__ import annotations

from typing import Optional

from attrdict import AttrDict
from sqlalchemy import Column, ForeignKey, Integer, Index
from sqlalchemy.orm import relationship


from .base import *
from .db import DB
from .addr import Addr, Tokn, Smac
from .tokn_addr import ToknAddr

__all__ = "UserAddr",


@dictattrs("user")
class UserAddr(Base):
    __tablename__ = "user_addr"

    user_pk = Column(Integer, ForeignKey("user.pkid", ondelete="CASCADE"), primary_key=True, index=True, nullable=False)
    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), primary_key=True, index=True, nullable=False)

    user = relationship("User", back_populates="user_addrs")
    addr = relationship("Addr", back_populates="addr_users", foreign_keys=[addr_pk])

    user_addr_txes = relationship(
        "UserAddrTX",
        viewonly=True,
        primaryjoin="""and_(
            UserAddrTX.user_pk == UserAddr.user_pk,
            UserAddrTX.addr_tx_pk == AddrTX.pkid,
            AddrTX.addr_pk == UserAddr.addr_pk,
        )"""
    )

    def asdict(self) -> AttrDict:
        d = super().asdict()

        d["tokn" if self.addr_is_tokn else "addr"] = self.addr.asdict()

        return d

    @property
    def addr_is_hydra(self) -> bool:
        return self.addr.addr_tp == Addr.Type.H

    @property
    def addr_is_smac(self) -> bool:
        return isinstance(self.addr, Smac)

    @property
    def addr_is_tokn(self) -> bool:
        return isinstance(self.addr, Tokn)

    def _remove(self, db: DB, user_addrs):

        for user_addr_tx in self.user_addr_txes:
            user_addr_tx._remove(db, self.user.user_addr_txes)

        addr = self.addr
        user_addrs.remove(self)
        addr._removed_user(db)

    def get_tokn_addr(self, db: DB, tokn: Tokn, create=True) -> Optional[ToknAddr]:
        return ToknAddr.get_for(db, tokn, self.addr, create=create)

    def get_addr_tokn(self, db: DB, addr: Addr, create=True) -> Optional[ToknAddr]:
        return ToknAddr.get_for(db, self.addr, addr, create=create)


Index(UserAddr.__tablename__ + "_idx", UserAddr.user_pk, UserAddr.addr_pk)
