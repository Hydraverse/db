from __future__ import annotations

from sqlalchemy import Column, String, Integer, BigInteger, Numeric, SmallInteger, ForeignKey
from sqlalchemy.orm import relationship

from .base import *
from ..db import DB, Block

__all__ = "BlockStat",


class BlockStat(StatBase, Base):
    __tablename__ = "block"

    stat = relationship("Stat", back_populates="block")

    pkid = Column(Integer, ForeignKey("stat.pkid", ondelete="CASCADE"), primary_key=True)

    height = Column(Integer, nullable=False, primary_key=True)
    hash = Column(String(64), nullable=False, primary_key=True)

    time = Column(BigInteger, nullable=False, unique=True, index=True)
    median_time = Column(BigInteger, nullable=False, unique=True, index=True)  # RPC only

    version = Column(Integer, nullable=False)

    miner = Column(String(34), nullable=False)  # XPL
    reward = Column(Numeric, nullable=False)
    interval = Column(Integer, nullable=False)

    difficulty = Column(Numeric, nullable=False)  # RPC
    size = Column(Integer, nullable=False)
    size_s = Column(Integer, nullable=False)  # RPC only
    weight = Column(Integer, nullable=False)
    tx_count = Column(SmallInteger, nullable=False)

    def __init__(self, db: DB, block: Block, **kwds):
        rpc_block = db.rpc.getblock(block.hash)

        super().__init__(
            height=block.height,
            hash=block.hash,
            **kwds
        )

