import json

from common.model import Message
from common import config
from rabbit.rabbit import RabbitMQ
import requests
import logging

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class PersistHelper:
    @staticmethod
    def create_message(message: Message):

        logger.info(f"create message {message.to_json()}")

        r = requests.post("http://localhost:8003/message/", json=message.to_dict())
        logger.info(f"creeate message r={r}")
        return r

    @staticmethod
    def update_message_class(message_id, class_id):
         r = requests.patch(f"http://localhost:8003/message/{message_id}",
                            json=json.dumps({"class": class_id}))
         logger.info(f"update message class retcode={r.status_code}")
         return r

class Orc:

    def __init__(self):
        self.rabbit =  RabbitMQ()
        self.queue_new = config.QUEUE_NEW
        self.queue_to_classify = config.QUEUE_TO_CLASSIFY
        self.queue_classified = config.QUEUE_CLASSIFIED
        self.rabbit.create_queue(self.queue_new)
        self.rabbit.create_queue(self.queue_to_classify)
        self.rabbit.create_queue(self.queue_classified)

    def process_new_message(self, ch, method, props, body):
        message = Message.from_json(body)
        r= PersistHelper.create_message(message)
        if r.status_code == 200:
            if r.json()["id"] > 0:
                message.id = r.json()["id"]
                self.rabbit.publish_message(self.queue_to_classify, message.to_json())
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_classified_message (self,  ch, method, props, body):
        message = Message.from_json(body)
        logging.info(f"process classified message {message.to_json()}")
        r = requests.put(f"http://localhost:8003/message/{message.id}",
                         json={"class_id":message.class_id, "text": message.text })
        if r.status_code == 200:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.rabbit.listen(
            {self.queue_new: lambda ch, method, props, body: self.process_new_message(ch, method, props, body),
             self.queue_classified: lambda ch, method, props, body: self.process_classified_message(ch, method, props, body)
            })


def main():
    Orc().start()


if __name__ == "__main__":
    main()