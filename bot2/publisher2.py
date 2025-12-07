from datetime import datetime

import common.config
from rabbit.rabbit import RabbitMQ
from telegram import Update
from common.model import Message
from aiogram import types


class Publisher:
    def __init__(self):
        self.rabbit = RabbitMQ()
        self.queue = common.config.QUEUE_NEW
        self.rabbit.create_queue(self.queue)

    def publish_text(self, msg: types.Message):
        m = Message(content_type="text", update_id=msg.message_id, user_id=str(msg.from_user.id),
                    ts_tg=msg.date, ts_bot=datetime.now(), text=msg.text)
        self.rabbit.publish_message(self.queue, m.to_json())

    def publish_voice(self, msg: types.Message, link:str):
        m = Message(content_type="voice", update_id=msg.message_id, user_id=str(msg.from_user.id),
                    ts_tg=msg.date, ts_bot=datetime.now(), voice_link=link)
        self.rabbit.publish_message(self.queue, m.to_json())




