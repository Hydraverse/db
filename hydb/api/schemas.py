from datetime import datetime
import enum
from typing import Optional, List, Generic, TypeVar, Dict

from attrdict import AttrDict
from pydantic import BaseModel, root_validator
from pydantic.generics import GenericModel


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
    info: AttrDict

    class Config:
        orm_mode = True


class AddrHist(AddrHistBase):
    block: Block

    class Config:
        orm_mode = True


class Addr(BaseModel):
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
    info: AttrDict

    def __str__(self):
        return self.addr_hy if self.addr_tp.value == Addr.Type.H else self.addr_hx

    class Config:
        orm_mode = True


class UserUniq(BaseModel):
    pkid: int
    name: str
    time: int
    nano: int

    class Config:
        orm_mode = True


class UserCreate(BaseModel):
    tg_user_id: int


class UserDelete(BaseModel):
    tg_user_id: int


class UserBase(BaseModel):
    uniq: UserUniq

    tg_user_id: int

    info: AttrDict

    class Config:
        orm_mode = True


class UserAddrHistBase(BaseModel):
    pkid: int
    user_addr_pk: int
    addr_hist_pk: int
    date_create: datetime
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
    block_c: int
    token_l: Dict[str, dict]
    addr: Addr

    class Config:
        orm_mode = True

    def filter_addr_token_balances(self):
        return filter(
            lambda bal: bal["addressHex"] in self.token_l,
            self.addr.info.get("qrc20Balances", [])
            + self.addr.info.get("qrc721Balances", [])
        )


class UserAddr(UserAddrBase):
    user_addr_hist: Optional[List[UserAddrHist]]

    class Config:
        orm_mode = True


class UserAddrAdd(BaseModel):
    address: str


class UserAddrTokenAdd(BaseModel):
    address: str

    class Result(BaseModel):
        token_l: Dict[str, dict]


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
    addr_hist: AddrHistBase

    class Config:
        orm_mode = True


class BlockSSEResult(BaseModel):
    block: Block
    user_addr_hist: List[UserAddrHistResult]

    class Config:
        orm_mode = True
