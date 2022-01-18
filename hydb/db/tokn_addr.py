from __future__ import annotations

from typing import Optional

from attrdict import AttrDict
from sqlalchemy import Column, ForeignKey, Integer, Index, BigInteger
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .addr import Addr
from .tokn import Tokn
from .nft import NFT
from .tx import TX

__all__ = "ToknAddr",


@dictattrs("tokn", "addr", "balance", "nft_uri")
class ToknAddr(Base):
    __tablename__ = "tokn_addr"

    tokn_pk = Column(Integer, ForeignKey("tokn.pkid", ondelete="CASCADE"), nullable=False, primary_key=True)
    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), nullable=False, primary_key=True)
    block_h = Column(Integer, nullable=True)
    balance = Column(BigInteger, nullable=True)
    nft_uri = DbDataColumn()

    tokn = relationship("Tokn", back_populates="tokn_addrs", foreign_keys=[tokn_pk])
    addr = relationship("Addr", back_populates="addr_tokns", foreign_keys=[addr_pk])

    def asdict(self):
        return AttrDict(
            tokn=self.tokn.asdict(),
            addr=self.addr.asdict(),
            block_h=self.block_h,
            balance=self.balance,
        )

    def _remove(self, db: DB, tokn_addrs):
        tokn = self.tokn
        tokn_addrs.remove(self)
        tokn._removed_user(db)

    def update_balance(self, db: DB, tx: Optional[TX] = None):
        height = (
            tx.block.height if tx is not None else
            self.addr.block_h if self.addr.block_h else
            db.rpc.getblockcount()
        )

        if self.block_h != height:
            self.block_h = height

            balance = self.tokn.balance_of(db, self.addr)

            if self.balance != balance:
                self.balance = balance

                if isinstance(self.tokn, NFT):
                    nft_uris = self.tokn.nft_tokn_uris_for(db, self.addr, balance)

                    if self.nft_uri != nft_uris:
                        self.nft_uri = nft_uris

                db.Session.add(self)

    @staticmethod
    def get_for(db: DB, tokn: Tokn, addr: Addr, create=True) -> Optional[ToknAddr]:
        for tokn_addr in tokn.tokn_addrs:
            if tokn_addr.addr == addr:
                return tokn_addr

        if create:
            # noinspection PyArgumentList
            ta = ToknAddr(tokn=tokn, addr=addr)
            db.Session.add(ta)
            return ta


Index(ToknAddr.__tablename__ + "_idx", ToknAddr.addr_pk, ToknAddr.tokn_pk)
