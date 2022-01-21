from __future__ import annotations

from hydra import log
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

from .base import *
from .db import DB

__all__ = "AddrHist",


class AddrHist(Base):
    __tablename__ = "addr_hist"

    pkid = DbPkidColumn()
    block_pk = Column(Integer, ForeignKey("block.pkid", ondelete="CASCADE"), nullable=False, primary_key=True)
    addr_pk = Column(Integer, ForeignKey("addr.pkid", ondelete="CASCADE"), nullable=False, primary_key=True)
    info = DbInfoColumn()

    block = relationship("Block", back_populates="addr_hist")
    addr = relationship("Addr", back_populates="addr_hist")

    addr_hist_user = relationship(
        "UserAddrHist",
        back_populates="addr_hist",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def _removed_user(self, db: DB):
        if not len(self.addr_hist_user):
            log.info(f"Deleting Block #{self.block.height} history for addr {str(self.addr)} with no users.")
            self._remove(db, self.addr.addr_hist)

    def _remove(self, db: DB, addr_hist):
        block = self.block
        addr_hist.remove(self)
        block._removed_hist(db)
