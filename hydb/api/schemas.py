from __future__ import annotations

import decimal
import string
from datetime import datetime, timedelta
import enum
from decimal import Decimal
from typing import Sequence

import pytz
from attrdict import AttrDict
from pydantic import BaseModel, ConfigDict

from hydra.rpc import HydraRPC

__all__ = (
    "timedelta_str",
    "UserMap",
    "StatQuantNetWeight",
    "Stat",
    "StatQuant",
    "ChainInfo",
    "ServerInfo",
    "UpdateResult",
    "Block",
    "AddrHist",
    "AddrBase",
    "Addr",
    "UserUniq",
    "UserCreate",
    "UserDelete",
    "UserBase",
    "UserAddrHist",
    "UserAddr",
    "UserAddrFull",
    "UserAddrAdd",
    "UserAddrUpdate",
    "UserAddrTokenAdd",
    "User",
    "DeleteResult",
    "UserInfoUpdate",
    "UserAddrResult",
    "UserAddrHistResult",
    "AddrHistResult",
    "SSEBlockEvent",
    "BlockSSEResult",
)

_DecimalNew = Decimal | float | str | tuple[int, Sequence[int], int]


def timedelta_str(td: timedelta) -> str:
    td_msg = AttrDict()
    neg = False
    
    if td < timedelta(0):
        td = abs(td)
        neg = True

    if td.days > 0:
        td_msg.days = str(td.days) + "d"

    seconds = td.seconds

    if seconds >= 3600:
        hours = seconds // 3600
        seconds -= hours * 3600
        td_msg.hours = str(hours) + "h"

    if seconds >= 60:
        minutes = seconds // 60
        seconds -= minutes * 60
        td_msg.minutes = str(minutes) + "m"

    if not len(td_msg) or (len(td_msg) == 1 and "minutes" in td_msg):
        td_msg.seconds = str(seconds) + "s"

    return (
            ("-" if neg else "") +
            td_msg.get("days", "") +
            td_msg.get("hours", "") +
            td_msg.get("minutes", "") +
            td_msg.get("seconds", "")
    )


class Parent(BaseModel):
    model_config = ConfigDict(
        use_enum_values=True,
        validate_assignment=True,
        populate_by_name=True,
        arbitrary_types_allowed=True,
        from_attributes=True,
        defer_build=True,
    )

    def dict(self, **kwds):
        return super().model_dump(mode="python", **kwds)


class UserMap(Parent):
    map: dict[int, int]


class StatQuantNetWeight(Parent):
    count: int
    median_1h: Decimal | None
    median_1d: Decimal | None
    median_1w: Decimal | None
    median_1m: Decimal | None


class Stat(Parent):
    pkid: int
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


class StatQuant(Stat):
    time: timedelta


class Stats(Parent):
    current: Stat
    quant_stat_1d: StatQuant | None
    quant_net_weight: StatQuantNetWeight | None


class ChainInfo(Parent):
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
    def get(cls, rpc: HydraRPC) -> ChainInfo:
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


class ServerInfo(Parent):
    mainnet: bool


class UpdateResult(Parent):
    updated: bool


class Block(Parent):
    pkid: int
    height: int
    hash: str
    conf: int
    info: AttrDict
    tx: list[AttrDict]

    def filter_tx(self, address: str):
        return filter(lambda tx: address in list(Block.tx_yield_addrs(tx)), self.tx)

    @staticmethod
    def tx_yield_addrs(tx: dict):
        # Address locations:
        # .[inputs|outputs].address[Hex]
        #                  .receipt.[sender|contractAddressHex]
        #                          .logs.addressHex    <-- Other contracts involved will have a separate TX (I think?).
        # .contractSpends.[inputs|outputs].addressHex  <-- Duplicated in separate TX! Indicated by contractSpendSource from the second TX.
        # .qrc[20|721]TokenTransfers.[to|toHex|from|fromHex|addressHex]

        for vio in (tx.get("inputs", []) + tx.get("outputs", [])):
            if "addressHex" in vio:
                yield vio["addressHex"]
            elif "address" in vio:
                yield vio["address"]

            receipt = vio.get("receipt", None)

            if receipt is not None:

                a = receipt.get("sender", None)

                if a is not None:
                    yield a

                a = receipt.get("contractAddressHex", None)

                if a is not None:
                    yield a

                # for logi in receipt.get("logs", []):
                #     if "addressHex" in logi:
                #         yield logi["addressHex"]

        # contract_spends = tx.get("contractSpends", {})
        #
        # for contract_spend in contract_spends:
        #     for cs_io in contract_spend.get("inputs", []) + contract_spend.get("outputs", []):
        #         a = cs_io.get("addressHex", cs_io.get("address", ...))
        #         if a is not ...:
        #             yield a

        token_transfers = tx.get("qrc20TokenTransfers", []) + tx.get("qrc721TokenTransfers", [])

        for token_transfer in token_transfers:
            a = token_transfer.get("from", None)
            if a is not None:
                yield a
            a = token_transfer.get("fromHex", None)
            if a is not None:
                yield a
            a = token_transfer.get("to", None)
            if a is not None:
                yield a
            a = token_transfer.get("toHex", None)
            if a is not None:
                yield a
            a = token_transfer.get("addressHex", None)
            if a is not None:
                yield a


class AddrHistBase(Parent):
    pkid: int
    addr_pk: int
    block_pk: int
    info_old: AttrDict
    info_new: AttrDict
    mined: bool


class AddrHist(AddrHistBase):
    block: Block


class AddrBase(Parent):
    class Type(enum.StrEnum):
        H = "HYDRA"
        S = "smart contract"
        N = "NFT"
        T = "token"

    pkid: int
    addr_hx: str
    addr_hy: str
    addr_tp: Addr.Type
    block_h: int

    def __str__(self):
        return self.addr_hy if self.addr_tp.value == Addr.Type.H else self.addr_hx

    def filter_tx(self, block: Block):
        addr_match = lambda addrs: self.addr_hx in addrs or self.addr_hy in addrs
        return filter(lambda tx: addr_match(list(Block.tx_yield_addrs(tx))), block.tx)

    @staticmethod
    def soft_validate(address: str, testnet: bool | None = None) -> AddrBase.Type | None:
        length = len(address)

        if not address.isalnum():
            return None

        base = (
            36 if length == 34 else
            16 if length == 40 else
            None
        )

        if base is None:
            return None

        try:
            int(address, base)
        except ValueError:
            return None

        if base == 36:
            if (testnet is True and address[0].lower() != 't' or
               testnet is False and address[0].lower() != 'h'):
                return None

        return (
            Addr.Type.H if length == 34 else
            Addr.Type.S
        )

    @staticmethod
    def decimal(value: _DecimalNew, decimals: int = 8, prec: int = 16) -> Decimal:
        decimal.getcontext().prec = prec
        return Decimal(value) / Decimal(10**decimals)


class Addr(AddrBase):
    info: AttrDict


Addr.model_rebuild(force=True)  # TODO: Determine whether this is still necessary.


class UserUniq(Parent):
    pkid: int
    date_create: datetime
    date_update: datetime | None
    time_create: int
    name_weight: int
    name: str
    hyve_addr_hy: str


class UserCreate(Parent):
    tg_user_id: int


class UserDelete(Parent):
    tg_user_id: int


class UserBase(Parent):
    uniq: UserUniq

    tg_user_id: int

    info: AttrDict

    def user_time(self, dt: datetime):
        tz_name = self.info.get("tz", "UTC")
        tz_user = pytz.timezone(tz_name)

        return pytz.utc.localize(dt, is_dst=None).astimezone(tz_user)


class UserAddrHistBase(Parent):
    pkid: int
    user_addr_pk: int
    addr_hist_pk: int
    date_create: datetime
    block_t: datetime | None
    block_c: int
    data: AttrDict | None


class UserAddrHist(UserAddrHistBase):
    addr_hist: AddrHist


class UserAddrBase(Parent):
    pkid: int
    user_pk: int
    addr_pk: int
    date_create: datetime
    date_update: datetime | None
    name: str
    block_t: datetime | None
    block_c: int
    token_l: list[str]
    info: AttrDict
    data: AttrDict | None

    def filter_info_token_balances(self, info: dict):
        return filter(
            lambda bal: bal["addressHex"] in self.token_l,
            info.get("qrc20Balances", [])
            + info.get("qrc721Balances", [])
        )

    def likely_matches(self, address: str, addr: AddrBase | None = None) -> bool:
        if not address:
            return False

        if addr is None:
            if not isinstance(self, UserAddr):
                raise TypeError("Must supply addr when calling UserAddrBase.likely_matches().")

            addr = self.addr

        addr_str = str(addr)

        return (
                addr_str == address or
                self.name.lower().startswith(address.lower()) or
                addr_str.startswith(address) or (
                    len(address) >= 4 and
                    address.lower() in self.name.lower()
                )
        )


class UserAddr(UserAddrBase):
    addr: Addr

    def filter_addr_token_balances(self):
        return self.filter_info_token_balances(self.addr.info)

    def matches(self, address: str, *, allow_name: bool = False):
        return (
                str(self.addr) == address or
                (allow_name and self.name.lower() == address.lower())
        )


class UserAddrFull(UserAddr):
    user: UserBase

    # - Unused so far:
    # user_addr_hist: list[UserAddrHistBase]


class UserAddrAdd(Parent):
    address: str
    name: str | None


class UserAddrUpdate(Parent):
    name: str | None
    info: AttrDict | None
    data: AttrDict | None
    over: bool | None

    class Result(UpdateResult):
        pass

    @staticmethod
    def validate_name(name: str):
        return (
            len(name) >= 5 and
            not len(
                [
                    c
                    for c in name
                    if (c.isspace() and c != " ")
                    or not c.isprintable()
                    or c in string.punctuation
                ]
            )
        )


class UserAddrTokenAdd(Parent):
    address: str

    class Result(Parent):
        added: bool
        addr_tp: Addr.Type
        addr_hx: str
        addr_hy: str
        name: str | None
        symbol: int | None
        totalSupply: int | None
        decimals: int | None


class User(UserBase):
    user_addrs: list[UserAddr]

    def find_addr(self, address: str, *, allow_names: bool = False) -> UserAddr | None:
        for ua in self.user_addrs:
            if ua.matches(address, allow_name=allow_names):
                return ua

    def filter_likely_addr_matches(self, address: str):
        return filter(
            lambda ua: ua.likely_matches(address),
            self.user_addrs
        )


class DeleteResult(Parent):
    deleted: bool


class UserInfoUpdate(Parent):
    info: AttrDict
    over: bool

    class Result(UpdateResult):
        pass


class UserAddrResult(UserAddrBase):
    user: UserBase


class UserAddrHistResult(UserAddrHistBase):
    user_addr: UserAddrResult


class AddrHistResult(AddrHistBase):
    addr: AddrBase
    addr_hist_user: list[UserAddrHistResult]


class SSEBlockEvent(enum.StrEnum):
    create = "create"
    mature = "mature"


class BlockSSEResult(Parent):
    id: int
    event: SSEBlockEvent
    block: Block
    hist: list[AddrHistResult]
