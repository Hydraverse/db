from typing import Optional, List

from pydantic import BaseModel


class Addr(BaseModel):
    pkid: int
    addr_tp: str
    addr_hx: str
    addr_hy: str
    block_h: int
    balance: int
    info: dict

    class Config:
        orm_mode = True


class Smac(Addr):
    name: str

    class Config:
        orm_mode = True


class ToknBase(Smac):
    symb: str
    supt: int


class NFT(ToknBase):

    class Config:
        orm_mode = True


class ToknAddr(BaseModel):
    tokn: [ToknBase, NFT]
    addr: Addr
    balance: int
    nft_uri: Optional[dict]

    class Config:
        orm_mode = True


class Tokn(ToknBase):
    deci: Optional[int]

    tokn_addrs: List[ToknAddr]

    class Config:
        orm_mode = True


class UserUniq(BaseModel):
    pkid: int
    name: str
    time: int
    nano: int


class UserCreate(UserUniq):
    tg_user_id: int


class UserAddr(BaseModel):
    user: UserUniq
    addr: Addr


class UserTokn(BaseModel):
    user: UserUniq
    tokn: [Tokn, NFT]


class User(UserUniq):
    tg_user_id: int
    tg_user_at: str

    user_addrs: List[UserAddr]
    user_tokns: List[UserTokn]

    class Config:
        orm_mode = True



