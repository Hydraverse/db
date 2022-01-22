from datetime import datetime
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


class Block(BaseModel):
    pkid: int
    height: int
    hash: str
    info: AttrDict
    tx: AttrDict

    class Config:
        orm_mode = True


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

