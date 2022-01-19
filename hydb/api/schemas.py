import enum
from typing import Optional, List, Generic, TypeVar

from attrdict import AttrDict
from pydantic import BaseModel, root_validator
from pydantic.generics import GenericModel

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


class Addr(BaseModel):
    class Type(str, enum.Enum):
        H = "HYDRA"
        S = "smart contract"
        N = "NFT"
        T = "token"

    pkid: int
    addr_tp: EnumModel[Type]
    addr_hx: str
    addr_hy: str
    block_h: Optional[int]
    balance: Optional[int]
    info: AttrDict

    class Config:
        orm_mode = True


class Smac(Addr):
    name: str

    class Config:
        orm_mode = True


class Tokn(Smac):
    symb: str
    supt: int
    deci: Optional[int]

    # tokn_addrs: List[ToknAddr]

    class Config:
        orm_mode = True


class NFT(Tokn):

    class Config:
        orm_mode = True


class ToknAddr(BaseModel):
    tokn: Tokn
    addr: Addr
    balance: int
    nft_uri: Optional[dict]

    class Config:
        orm_mode = True


class TXBase(BaseModel):
    pkid: int
    block_pkid: int
    block_txno: int
    block_txid: str
    vouts_inp: AttrDict
    vouts_out: AttrDict
    logs: AttrDict

    class Config:
        orm_mode = True


class AddrTX(BaseModel):
    pkid: int
    addr_pk: int
    tx: TXBase

    class Config:
        orm_mode = True


class TX(TXBase):
    addr_txes: List[AddrTX]

    class Config:
        orm_mode = True


class Block(BaseModel):
    pkid: int
    height: int
    hash: str
    info: AttrDict
    logs: AttrDict

    txes: List[TX]

    class Config:
        orm_mode = True


class UserAddrTX(BaseModel):
    user_pk: int
    addr_tx: AddrTX

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
    pkid: int

    uniq: UserUniq

    tg_user_id: int

    info: AttrDict

    class Config:
        orm_mode = True


class UserAddr(BaseModel):
    user_pk: int
    addr: Addr

    class Config:
        orm_mode = True


class UserAddrAdd(BaseModel):
    address: str


class UserAddrDel(BaseModel):
    addr_pk: int


class UserTokn(BaseModel):
    addr: Tokn

    class Config:
        orm_mode = True


class User(UserBase):
    user_addrs: List[UserAddr]
    user_tokns: List[UserTokn]

    user_addr_txes: List[UserAddrTX]

    class Config:
        orm_mode = True

