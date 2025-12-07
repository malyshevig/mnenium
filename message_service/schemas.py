from pydantic import BaseModel
from typing import Optional
import datetime


class MessageBase(BaseModel):
    content_type: str
    update_id: int
    user_id: str
    ts_tg: datetime.datetime
    ts_bot: datetime.datetime
    text: Optional[str] = None
    voice_link: Optional[str] = None
    class_id: Optional[str] = None


class MessageCreate(MessageBase):
    pass


class MessageUpdate(BaseModel):
    text: Optional[str] = None
    class_id: Optional[str] = None


class Message(MessageBase):
    pass