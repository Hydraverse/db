from __future__ import annotations

from typing import List

from hydra import log
from sqlalchemy import Column, ForeignKey, Integer, Boolean
from sqlalchemy.orm import relationship

from .base import *
from .db import DB
from .block import Block

__all__ = "AddrHist",


class AddrHist(Base):
    __tablename__ = "addr_hist"

    pkid = DbPkidColumn(seq="addr_hist_seq")
    block_pk = Column(Integer, ForeignKey("block.pkid", ondelete="CASCADE"), nullable=False, primary_key=True)
    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), nullable=False, primary_key=True)
    info_old = DbInfoColumn()
    info_new = DbInfoColumn()

    block = relationship("Block", back_populates="addr_hist")
    addr = relationship("Addr", back_populates="addr_hist")

    addr_hist_user = relationship(
        "UserAddrHist",
        order_by="UserAddrHist.pkid",
        back_populates="addr_hist",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    @property
    def mined(self) -> bool:
        return self.block.info.get("miner", "") == self.addr.addr_hy

    def on_block_mature(self, db: DB):
        self.addr.update_info(db)

        self.info_old = self.info_new
        self.info_new = self.addr.info

        db.Session.add(self)

    def _removed_user(self, db: DB):
        if not len(self.addr_hist_user):
            log.info(f"Deleting Block #{self.block.height} history for addr {str(self.addr)} with no users.")
            self._remove(db, self.addr.addr_hist)

    def _remove(self, db: DB, addr_hist):
        block = self.block
        addr_hist.remove(self)
        block._removed_hist(db)

    @staticmethod
    def all_for_block(db: DB, block: Block) -> List[AddrHist]:
        ahs: List[AddrHist] = db.Session.query(
            AddrHist
        ).where(
            AddrHist.block_pk == block.pkid,
        ).all()

        return ahs
