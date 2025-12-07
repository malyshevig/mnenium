import json

from flask import Flask, jsonify, request
from flask_restful import Resource

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

    def update_message_class(self, msg_id:int, class_id: str):
        q=f"update message set class='{class_id}' where id={msg_id};"
        self.execute_query_update (q)


class Persist:
    def __init__(self):
        self.rabbit = RabbitMQ()
        self.db = Db("museum")

    def create_message(self, message:Message):
        id = self.db.add_message(message)
        message.id = id
        return message

    def update_message_class(self, message:Message):
        self.db.update_message_class(message.id, message.class_id)
        pass



if __name__ == '__main__':

    app = Flask(__name__)
    persist = Persist()


    # create message
    @app.route('/api/message', methods=['POST'])
    def create_message():
        try:
            msg = json.loads(request.get_json())
            message = persist.create_message(msg)
            if message:
                return jsonify(message), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 503  #


    @app.route('/api/message/{id}/class', methods=['PUT'])
    def update_message_class():
        try:
            persist.update_message_class()
            return "Ok", 200
        except Exception as e:
            return jsonify({"error": str(e)}), 503  #


    @app.route('/api/message', methods=['GET'])
    def create_message():
        try:
            msg = json.loads(request.get_json())
            message = persist.create_message(msg)
            if message:
                return jsonify(message), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 503  #


    app.run(debug=True, port=8083, use_reloader=False)