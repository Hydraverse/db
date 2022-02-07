from __future__ import annotations

from sqlalchemy import Column, Integer, Numeric, SmallInteger, Sequence, func, DateTime
from sqlalchemy.orm import relationship

from .base import *
from .block import BlockStat
from ..db import DB, Block

__all__ = "Stat",

# class StatViewTestXXX:  # (Base)
#     __table__ = Table("some_view", Base.metadata, autoload=True)
#
#     id = Column(Integer, primary_key=True)
#
#     __mapper_args__ = {
#         'primary_key': [__table__.c.id]
#     }


class Stat(StatBase, Base):
    __tablename__ = "stat"

    pkid = Column(
        Integer, Sequence("stat_seq", metadata=StatBase.metadata), nullable=False, primary_key=True, unique=True
    )

    time = Column(DateTime, default=func.now(), nullable=False, unique=True, index=True)

    apr = Column(Numeric, nullable=False)
    blocks = Column(Integer, nullable=False)
    connections = Column(SmallInteger, nullable=False)
    time_offset = Column(Integer, nullable=False)

    block_value = Column(Numeric, nullable=False)  # RPC
    money_supply = Column(Numeric, nullable=False)
    burned_coins = Column(Numeric, nullable=False)

    net_weight = Column(Numeric, nullable=False)
    net_hash_rate = Column(Numeric, nullable=False)
    net_diff_pos = Column(Numeric, nullable=False)
    net_diff_pow = Column(Numeric, nullable=False)

    block = relationship(
        BlockStat,
        back_populates="stat",
        uselist=False
    )

    # will go in wallet table when tracking.
    #
    # search_interval = Column(Integer, nullable=False)
    # expected_time = Column(Integer, nullable=False)
    # weight = Column(Numeric, nullable=False)

    def __init__(self, db: DB, block: Block):

        info = db.rpc.getinfo()
        mining_info = db.rpc.getmininginfo()

        apr = db.rpc.getestimatedannualroi()

        # noinspection PyArgumentList
        super().__init__(
            apr=apr,
            blocks=info.blocks,
            connections=info.connections,
            time_offset=info.timeoffset,
            block_value=mining_info.blockvalue,
            money_supply=info.moneysupply,
            burned_coins=info.burnedcoins,
            net_weight=mining_info.netstakeweight,
            net_hash_rate=mining_info.networkhashps,
            net_diff_pos=mining_info.difficulty["proof-of-stake"],
            net_diff_pow=mining_info.difficulty["proof-of-work"],
            block=BlockStat(db, block, stat=self)
        )



