from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from database import Base
import datetime

class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    content_type = Column(String, nullable=False)
    update_id = Column(Integer, nullable=False)
    user_id = Column(String, nullable=False)
    ts_tg = Column(DateTime, nullable=False)
    ts_bot = Column(DateTime, nullable=False, server_default=func.now())
    text = Column(String, nullable=True)
    voice_link = Column(String, nullable=True)
    class_id = Column(String, nullable=True)