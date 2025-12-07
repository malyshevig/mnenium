import unittest
from rabbit import  RabbitMQ


class QueueTest(unittest.TestCase):
    def test_queue(self):
        r = RabbitMQ()
        r.create_queue("test")

        r.publish_message("test", "hello")

        r.delete_queue("test")


if __name__ == '__main__':
    unittest.main()