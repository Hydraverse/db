from __future__ import annotations

from hydra import log
from sqlalchemy import Column, String, UniqueConstraint, Integer, ForeignKey, SmallInteger
from sqlalchemy.orm import relationship

from .base import *
from .db import DB

__all__ = "TX",


class TX(DbPkidMixin, Base):
    __tablename__ = "tx"
    __table_args__ = (
        UniqueConstraint("block_pkid", "block_txno"),
    )

    block_pkid = Column(Integer, ForeignKey("block.pkid", ondelete="CASCADE"), nullable=False)
    block_txno = Column(SmallInteger, nullable=False)
    block_txid = Column(String(64), nullable=False, unique=True)
    vouts_inp = DbDataColumn()
    vouts_out = DbDataColumn()
    logs = DbDataColumn()

    block = relationship(
        "Block",
        back_populates="txes"
    )

    addr_txes = relationship(
        "AddrTX",
        back_populates="tx",
        cascade="all, delete-orphan",
        single_parent=True
    )

    def _removed_addr(self, db: DB):
        if not len(self.addr_txes):
            # if not len(self.user_data):
            log.info(f"Removing TX #{self.block_txno} from block #{self.block.height}.")
            block = self.block
            block.txes.remove(self)
            block._delete_if_unused(db)
            # else:
            #     log.info(f"Keeping TX #{self.block_txno} from block #{self.block.height} with non-empty user data.")

    def on_new_block(self, db: DB) -> bool:
        block = self.block

        if not AddrTX.on_new_block_tx(db, self):
            log.debug(f"Not adding TX #{self.block_txno} from block #{block.height} with no current subscribers.")
            db.Session.delete(self)
            return False

        log.info(f"Adding TX #{self.block_txno} from block #{self.block.height} with {len(self.addr_txes)} subscriber(s).")
        db.Session.add(self)
        return True


from .addr_tx import AddrTX
