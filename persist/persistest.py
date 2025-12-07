import datetime
import unittest

from common.model import Message
from persist import Db


class MyTestCase(unittest.TestCase):
    def test_something(self):

        db = Db("museum")
        m = Message(content_type="text", update_id=1, user_id="1", ts_tg=datetime.datetime.now(),
                    ts_bot=datetime.datetime.now(), text="Hello")

        m.id = db.add_message(m)


        db.update_message_class(m.id,"negative")

if __name__ == '__main__':
    unittest.main()
