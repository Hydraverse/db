from datetime import datetime
import enum
from typing import Optional, List, Generic, TypeVar

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

        for contract_spend in (contract_spends.get("inputs", []) + contract_spends.get("outputs", [])):
            a = contract_spend.get("addressHex", contract_spend.get("address", ...))
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


class AddrHist(BaseModel):
    pkid: int
    block: Block
    addr_pk: int
    info: AttrDict

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
    pkid: int


class UserBase(BaseModel):
    uniq: UserUniq

    tg_user_id: int

    info: AttrDict

    class Config:
        orm_mode = True


class UserAddrHist(BaseModel):
    user_addr_pk: int
    date_create: datetime
    garbage: bool
    block_c: int
    addr_hist: AddrHist
    data: Optional[AttrDict]

    class Config:
        orm_mode = True


class UserAddr(BaseModel):
    pkid: int
    date_create: datetime
    date_update: Optional[datetime]
    block_c: int
    addr: Addr

    user_addr_hist: Optional[List[UserAddrHist]]

    class Config:
        orm_mode = True


class UserAddrAdd(BaseModel):
    address: str


class UserAddrDel(BaseModel):
    addr_pk: int


class User(UserBase):
    user_addrs: List[UserAddr]

    class Config:
        orm_mode = True

