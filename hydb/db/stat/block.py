from __future__ import annotations

from attrdict import AttrDict
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

    miner = Column(String(34), nullable=False)  # XPL only [but have code to retrieve from TX]
    reward = Column(Numeric, nullable=False)
    interval = Column(Integer, nullable=False)  # XPL only

    difficulty = Column(Numeric, nullable=False)  # RPC
    size = Column(Integer, nullable=False)
    size_s = Column(Integer, nullable=False)  # RPC only
    weight = Column(Integer, nullable=False)
    tx_count = Column(SmallInteger, nullable=False)

    def __init__(self, db: DB, block: Block, **kwds):
        rpc_block = db.rpc.getblock(block.hash)
        info = AttrDict(block.info)

        super().__init__(
            height=block.height,
            hash=block.hash,
            time=rpc_block.time,
            median_time=rpc_block.mediantime,
            version=rpc_block.version,
            miner=info.miner,
            reward=info.reward,
            interval=info.interval,
            difficulty=rpc_block.difficulty,
            size=rpc_block.size,
            size_s=rpc_block.strippedsize,
            weight=rpc_block.weight,
            tx_count=rpc_block.nTx,
            **kwds
        )

