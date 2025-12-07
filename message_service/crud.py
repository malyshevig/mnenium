from common.model import Message
from persist.dbutil import DbUtil
from rabbit.rabbit import RabbitMQ


class Db(DbUtil):
    def add_message(self, message:Message):
        q=f"insert into message (content_type, update_id, user_id, ts_tg, ts_bot, text, voice_link) "
        q+=f"values ('{message.content_type}','{message.update_id}', '{message.user_id}', "
        q+=f"'{message.ts_tg}', '{message.ts_bot}', '{message.text}', '{message.voice_link}') "
        q+=f"RETURNING id;"

        r = self.execute_query_update_and_select(q, limit=1)
        if len(r) == 0:
            return None
        else:
            return r[0][0]

    def update_message_class(self, msg_id:int, class_id: str, text:str):
        q=f"update message set class='{class_id}', text='{text}' where id={msg_id};"
        return self.execute_query_update (q)

    def get_messages(self, offset=0, limit=100):
        q = f"select id, content_type, update_id, user_id, ts_tg, ts_bot, text, voice_link, class "
        q+=f"from message "
        q+=f"offset {offset} limit {limit};"

        return self.execute_query_select_dict(q, limit=limit)

class Persist:
    def __init__(self):
        self.rabbit = RabbitMQ()
        self.db = Db("museum")

    def create_message(self, message:Message):
        id = self.db.add_message(message)

        return id

    def update_message_class(self, message_id, class_id, text):
        return self.db.update_message_class(message_id, class_id, text)

    def get_messages(self, offset=0, limit=100):
        return self.db.get_messages(offset, limit)




