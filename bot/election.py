import json
import logging
import threading
import time
import uuid

import etcd3

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class Election:
    def __init__(self, callback):
        self.etcd = etcd3.client(host='localhost', port=2379)
        self.leader_key = 'leader'
        self.node_id =  uuid.uuid4().hex
        self.callback = callback
        self.is_leader = False

    def elect(self):
       success = False

       while not success:
            logger.info(f"Node {self.node_id} trying to become leader")
            lease = self.etcd.lease(30)

            success = self.etcd.put_if_not_exists(
                self.leader_key,
                json.dumps({
                    'node_id': self.node_id,
                    'timestamp': time.time()
                }),
                lease=lease.id
            )

            if success:
                self.is_leader = True
                logger.info(f"Node {self.node_id} became leader")
                # Запускаем обновление lease
                threading.Thread(target=self.refresh_lease, args=(lease,), daemon=True).start()
                if self.callback:
                    self.callback("master")
            else:
                time.sleep(2)

    def refresh_lease(self, lease):
        """Обновление lease в фоне"""
        while self.is_leader:

            try:
                time.sleep(20)
                lease.refresh()
            except:
                self.is_leader = False
                if self.callback:
                    logger.info(f"Node {self.node_id} became leader")
                    self.callback("slave")
                break


def handler(event:str):
    print(event)


if __name__ == '__main__':
    election = Election(handler)
    election.elect()

    print("Running...")
    while True:
        pass
