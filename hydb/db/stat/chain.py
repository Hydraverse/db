from typing import Self
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

from hydra.rpc import HydraRPC

__all__ = "ChainStat",


@dataclass(slots=True)
class ChainStat:
    time: datetime
    apr: Decimal
    blocks: int
    connections: int
    time_offset: int

    block_value: Decimal
    money_supply: Decimal
    burned_coins: Decimal

    net_weight: Decimal
    net_hash_rate: Decimal
    net_diff_pos: Decimal
    net_diff_pow: Decimal

    @classmethod
    def get(cls, rpc: HydraRPC) -> Self:
        info = rpc.getinfo()

        apr = rpc.getestimatedannualroi()

        mining_info = rpc.getmininginfo()

        return cls(
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
        )
