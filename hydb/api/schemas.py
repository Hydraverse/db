import decimal
import string
from datetime import datetime, timedelta
import enum
from decimal import Decimal
from typing import Optional, List, Generic, TypeVar, Dict, Union, Tuple, Sequence

from attrdict import AttrDict
from pydantic import BaseModel, root_validator
from pydantic.generics import GenericModel

_DecimalNew = Union[Decimal, float, str, Tuple[int, Sequence[int], int]]


def timedelta_str(td: timedelta) -> str:
    td_msg = AttrDict()

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

    if not len(td_msg):
        td_msg.seconds = str(seconds) + "s"

    return (
            td_msg.get('days', '') +
            td_msg.get('hours', '') +
            td_msg.get('minutes', '') +
            td_msg.get('seconds', '')
    )


class ServerInfo(BaseModel):
    mainnet: bool


EnumTypeVar = TypeVar("EnumTypeVar")


class EnumModel(GenericModel, Generic[EnumTypeVar]):
    value: EnumTypeVar
    possible_values: List[str] = []

    class Config:
        validate_assignment = True
        orm_mode = True

    @root_validator
    def root_validate(cls, values):
        values["possible_values"] = [item for item in values['value'].__class__]
        return values


class Block(BaseModel):
    pkid: int
    height: int
    hash: str
    conf: int
    info: AttrDict
    tx: List[AttrDict]

    class Config:
        orm_mode = True

    def filter_tx(self, address: str):
        return filter(lambda tx: address in list(Block.tx_yield_addrs(tx)), self.tx)

    @staticmethod
    def tx_yield_addrs(tx: dict):
        # Address locations:
        # .[inputs|outputs].address[Hex]
        #                  .receipt.[sender|contractAddressHex]
        #                          .logs.addressHex
        # .contractSpends.[inputs|outputs].addressHex
        # .qrc[20|721]TokenTransfers.[to|from|addressHex]

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

                for logi in receipt.get("logs", []):
                    if "addressHex" in logi:
                        yield logi["addressHex"]

        contract_spends = tx.get("contractSpends", {})

        for contract_spend in contract_spends:
            for cs_io in contract_spend.get("inputs", []) + contract_spend.get("outputs", []):
                a = cs_io.get("addressHex", cs_io.get("address", ...))
                if a is not ...:
                    yield a

        token_transfers = tx.get("qrc20TokenTransfers", []) + tx.get("qrc721TokenTransfers", [])

        for token_transfer in token_transfers:
            a = token_transfer.get("from", None)
            if a is not None:
                yield a
            a = token_transfer.get("to", None)
            if a is not None:
                yield a
            a = token_transfer.get("addressHex", None)
            if a is not None:
                yield a


class AddrHistBase(BaseModel):
    pkid: int
    addr_pk: int
    block_pk: int
    info_old: AttrDict
    info_new: AttrDict
    mined: bool

    class Config:
        orm_mode = True


class AddrHist(AddrHistBase):
    block: Block

    class Config:
        orm_mode = True


class AddrBase(BaseModel):
    class Type(str, enum.Enum):
        H = "HYDRA"
        S = "smart contract"
        N = "NFT"
        T = "token"

    pkid: int
    addr_hx: str
    addr_hy: str
    addr_tp: EnumModel[Type]
    block_h: int

    class Config:
        orm_mode = True

    def __str__(self):
        return self.addr_hy if self.addr_tp.value == Addr.Type.H else self.addr_hx

    def filter_tx(self, block: Block):
        addr_match = lambda addrs: self.addr_hx in addrs or self.addr_hy in addrs
        return filter(lambda tx: addr_match(list(Block.tx_yield_addrs(tx))), block.tx)

    @staticmethod
    def soft_validate(address: str, testnet: Optional[bool] = None) -> Optional[Type]:
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
            if testnet is True  and address[0].lower() != 't' or \
               testnet is False and address[0].lower() != 'h':
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

    class Config:
        orm_mode = True


class UserUniq(BaseModel):
    pkid: int
    date_create: datetime
    date_update: Optional[datetime]
    time_create: int
    name_weight: int
    name: str
    hyve_addr_hy: str

    class Config:
        orm_mode = True


class UserCreate(BaseModel):
    tg_user_id: int


class UserDelete(BaseModel):
    tg_user_id: int


class UserBase(BaseModel):
    uniq: UserUniq

    tg_user_id: int

    block_c: int

    info: AttrDict

    class Config:
        orm_mode = True


class UserAddrHistBase(BaseModel):
    pkid: int
    user_addr_pk: int
    addr_hist_pk: int
    date_create: datetime
    block_t: Optional[datetime]
    block_c: int
    data: Optional[AttrDict]

    class Config:
        orm_mode = True


class UserAddrHist(UserAddrHistBase):
    addr_hist: AddrHist

    class Config:
        orm_mode = True


class UserAddrBase(BaseModel):
    pkid: int
    user_pk: int
    addr_pk: int
    date_create: datetime
    date_update: Optional[datetime]
    name: str
    block_t: Optional[datetime]
    block_c: int
    token_l: List[str]

    class Config:
        orm_mode = True

    def filter_info_token_balances(self, info: dict):
        return filter(
            lambda bal: bal["addressHex"] in self.token_l,
            info.get("qrc20Balances", [])
            + info.get("qrc721Balances", [])
        )


class UserAddr(UserAddrBase):
    addr: Addr

    class Config:
        orm_mode = True

    def filter_addr_token_balances(self):
        return self.filter_info_token_balances(self.addr.info)


class UserAddrFull(UserAddr):
    user_addr_hist: List[UserAddrHistBase]

    class Config:
        orm_mode = True


class UserAddrAdd(BaseModel):
    address: str
    name: Optional[str]


class UserAddrUpdate(BaseModel):
    name: Optional[str]
    # data: Optional[AttrDict]
    # over: Optional[bool]

    class Result(BaseModel):
        updated: bool

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


class UserAddrTokenAdd(BaseModel):
    address: str

    class Result(BaseModel):
        added: bool
        addr_tp: EnumModel[Addr.Type]
        addr_hx: str
        addr_hy: str
        name: Optional[str]
        symbol: Optional[int]
        totalSupply: Optional[int]
        decimals: Optional[int]


class User(UserBase):
    user_addrs: List[UserAddr]

    class Config:
        orm_mode = True


class DeleteResult(BaseModel):
    deleted: bool


class UserInfoUpdate(BaseModel):
    info: AttrDict
    over: bool

    class Result(BaseModel):
        info: AttrDict


class UserAddrResult(UserAddrBase):
    user: UserBase

    class Config:
        orm_mode = True


class UserAddrHistResult(UserAddrHistBase):
    user_addr: UserAddrResult

    class Config:
        orm_mode = True


class AddrHistResult(AddrHistBase):
    addr: AddrBase
    addr_hist_user: List[UserAddrHistResult]

    class Config:
        orm_mode = True


class SSEBlockEvent(str, enum.Enum):
    create = "create"
    mature = "mature"


class BlockSSEResult(BaseModel):
    id: int
    event: SSEBlockEvent
    block: Block
    hist: List[AddrHistResult]

    class Config:
        orm_mode = True
