from __future__ import annotations

import time
from datetime import datetime
from decimal import Decimal
from typing import Optional

from hydra import log
from sqlalchemy import Column, Integer, Numeric, SmallInteger, Sequence, func, DateTime, and_, Table, Interval, desc, \
    text
from sqlalchemy.orm import relationship
from sqlalchemy.exc import NoResultFound

from .base import *
from .block import BlockStat
from ..db import DB, Block

__all__ = "Stat", "StatQuantNetWeightView", "StatQuantView1d"


class Stat(StatBase, Base):
    __tablename__ = "stat"

    pkid = Column(
        Integer, Sequence("stat_seq", metadata=StatBase.metadata), nullable=False, primary_key=True, unique=True
    )

    time = Column(DateTime, server_default=func.now(), nullable=False, unique=True, index=True)

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

        while 1:
            info = db.rpc.getinfo()

            if Decimal(info.moneysupply) == 0 or Decimal(info.burnedcoins) == 0:
                log.warning("Hydra RPC getinfo() returnd zero values, retrying.")
                time.sleep(1)
                continue

            break

        mining_info = db.rpc.getmininginfo()

        apr = db.rpc.getestimatedannualroi()

        # noinspection PyArgumentList
        super().__init__(
            time=datetime.utcnow(),
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
        return 1 == db.session.query(

            func.count(BlockStat.pkid)

        ).where(
            and_(
                BlockStat.height == block.height,
                BlockStat.hash == block.hash,
            )

        ).limit(1).scalar()

    @staticmethod
    def current(db: DB) -> Optional[Stat]:
        return db.session.query(Stat).order_by(desc(Stat.pkid)).limit(1).one_or_none()


class StatQuantNetWeightView(StatBase, Base):
    """
    create view stat.quant_net_weight(count, median_1h, median_1d, median_1w, median_1m) as
    SELECT (SELECT count(pkid) FROM stat.stat)                               AS count,
           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
            FROM stat.stat
            WHERE stat."time" > (now()::timestamp without time zone - '01:00:00'::interval)) AS median_1h,
           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
            FROM stat.stat
            WHERE stat."time" > (now()::timestamp without time zone - '1 day'::interval))    AS median_1d,
           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
            FROM stat.stat
            WHERE stat."time" > (now()::timestamp without time zone - '7 days'::interval))   AS median_1w,
           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile
            FROM stat.stat
            WHERE stat."time" > (now()::timestamp without time zone - '1 mon'::interval))    AS median_1m;
    """
    __tablename__ = "quant_net_weight"
    # __abstract__ = True

    count = Column(Integer, default=0)

    median_1h = Column(Numeric, nullable=True)
    median_1d = Column(Numeric, nullable=True)
    median_1w = Column(Numeric, nullable=True)
    median_1m = Column(Numeric, nullable=True)

    __mapper_args__ = {
        'primary_key': [count]
    }

    @staticmethod
    def get(db: DB) -> Optional[StatQuantNetWeightView]:
        return db.session.query(StatQuantNetWeightView).one_or_none()


class StatQuantView1d(StatBase, Base):
    """
    create view stat.quant_stat_1d
            (pkid, time, apr, blocks, connections, time_offset, block_value, money_supply, burned_coins, net_weight,
             net_hash_rate, net_diff_pos, net_diff_pow)
    as
    SELECT min(pkid)                                                                                AS "pkid",
           max(stat."time") - min(stat."time")                                                      AS "time",
           (SELECT quantile(stat.apr, 0.5::double precision) AS quantile)                           AS apr,
           count(stat.blocks)                                                                       AS blocks,
           (SELECT quantile(stat.connections::double precision, 0.5::double precision) AS quantile) AS connections,
           (SELECT quantile(stat.time_offset, 0.5::double precision) AS quantile)                   AS time_offset,
           (SELECT quantile(stat.block_value, 0.5::double precision) AS quantile)                   AS block_value,
           (SELECT quantile(stat.money_supply, 0.5::double precision) AS quantile)                  AS money_supply,
           max(stat.burned_coins) - min(stat.burned_coins)                                          AS burned_coins,
           (SELECT quantile(stat.net_weight, 0.5::double precision) AS quantile)                    AS net_weight,
           (SELECT quantile(stat.net_hash_rate, 0.5::double precision) AS quantile)                 AS net_hash_rate,
           (SELECT quantile(stat.net_diff_pos, 0.5::double precision) AS quantile)                  AS net_diff_pos,
           (SELECT quantile(stat.net_diff_pow, 0.5::double precision) AS quantile)                  AS net_diff_pow
    FROM stat.stat
    WHERE stat."time" > (now()::timestamp without time zone - '1 day'::interval);
    """
    __tablename__ = "quant_stat_1d"
    # __abstract__ = True

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
    def get(db: DB) -> Optional[StatQuantView1d]:
        return db.session.query(StatQuantView1d).one_or_none()
