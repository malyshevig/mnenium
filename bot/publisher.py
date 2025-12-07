from datetime import datetime

import common.config
from rabbit.rabbit import RabbitMQ
from telegram import Update
from common.model import Message


class Publisher:
    def __init__(self):
        self.rabbit = RabbitMQ()
        self.queue = common.config.QUEUE_NEW
        self.rabbit.create_queue(self.queue)

    def publish_text(self, update:Update):
        m = Message(content_type="text", update_id=update.update_id, user_id=str(update.message.from_user.id),
                    ts_tg=update.message.date, ts_bot=datetime.now(), text=update.message.text)
        self.rabbit.publish_message(self.queue, m.to_json())

    def publish_voice(self, update:Update, link:str):
        m = Message(content_type="voice", update_id=update.update_id, user_id=str(update.message.from_user.id),
                    ts_tg=update.message.date, ts_bot=datetime.now(), text=update.message.text, voice_link=link)
        self.rabbit.publish_message(self.queue, m.to_json())




