from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, Integer, Numeric, SmallInteger, Sequence, func, DateTime, and_, Table, Interval, desc
from sqlalchemy.orm import relationship

from .base import *
from .block import BlockStat
from ..db import DB, Block

__all__ = "Stat", "StatQuantNetWeightView", "StatQuantView1d"


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

    @staticmethod
    def exists_for_block(db: DB, block: Block) -> bool:
        return 1 == db.Session.query(

            func.count(BlockStat.pkid)

        ).where(
            and_(
                BlockStat.height == block.height,
                BlockStat.hash == block.hash,
            )

        ).limit(1).scalar()

    @staticmethod
    def current(db: DB) -> Optional[Stat]:
        return db.Session.query(Stat).order_by(desc(Stat.pkid)).limit(1).one_or_none()


class StatQuantNetWeightView(StatBase, Base):
    __tablename__ = "quant_net_weight"

    dummy = Column(Integer, default=0)

    median_1h = Column(Numeric, nullable=True)
    median_1d = Column(Numeric, nullable=True)
    median_1w = Column(Numeric, nullable=True)
    median_1m = Column(Numeric, nullable=True)

    __mapper_args__ = {
        'primary_key': [dummy]
    }

    @staticmethod
    def get(db: DB) -> StatQuantNetWeightView:
        return db.Session.query(StatQuantNetWeightView).one()


class StatQuantView1d(StatBase, Base):
    __tablename__ = "quant_stat_1d"

    pkid = Column(Integer,  primary_key=True)

    time = Column(Interval)

    apr = Column(Numeric)
    blocks = Column(Integer)
    connections = Column(SmallInteger)
    time_offset = Column(Integer)

    block_value = Column(Numeric)
    money_supply = Column(Numeric)
    burned_coins = Column(Numeric)

    net_weight = Column(Numeric)
    net_hash_rate = Column(Numeric)
    net_diff_pos = Column(Numeric)
    net_diff_pow = Column(Numeric)

    __mapper_args__ = {
        'primary_key': [pkid]
    }

    @staticmethod
    def get(db: DB) -> StatQuantView1d:
        return db.Session.query(StatQuantView1d).one()
