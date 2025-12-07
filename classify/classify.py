
from common.model import Message
from common.config import *
from rabbit.rabbit import RabbitMQ
from text import TextClassifier
from audio import VoiceTranscriber

class Classifier:
    def __init__(self):
        self.rabbit = RabbitMQ()
        self.queue_to_classify =  QUEUE_TO_CLASSIFY
        self.queue_classified = QUEUE_CLASSIFIED
        self.text_classifier = TextClassifier()
        self.voice_transcriber = VoiceTranscriber()

    def process_new_message(self, ch, method, props, body):
        message = Message.from_json(body)
        if message.content_type == "text":
            class_id = self.text_classifier.classify_text(message.text)
            if class_id:
                message.class_id = class_id
                self.rabbit.publish_message(self.queue_classified, message.to_json())
                ch.basic_ack(delivery_tag=method.delivery_tag)
        if message.content_type == "voice":
            message.text = self.voice_transcriber.transcribe_audio(message.voice_link)
            class_id = self.text_classifier.classify_text(message.text)
            if class_id:
                message.class_id = class_id
                self.rabbit.publish_message(self.queue_classified, message.to_json())
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.rabbit.listen(
            {self.queue_to_classify: lambda ch, method, props, body: self.process_new_message(ch, method, props, body),
             })


def main():
    Classifier().start()

if __name__ == "__main__":
    main()