import datetime
import unittest

from common.model import Message


class MyTestCase(unittest.TestCase):
    def test_something(self):
        m = Message(content_type="text", update_id=1, user_id="1", ts_tg=datetime.datetime.now(),
                    ts_bot=datetime.datetime.now(), text="Hello")
        print(m.to_json())


if __name__ == '__main__':
    unittest.main()
