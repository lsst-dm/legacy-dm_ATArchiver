import asyncio
import os
import time
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.publisher import Publisher
from lsst.dm.csc.base.base import base


class FileNotifier(base):
    def __init__(self, configfile, logfile):
        super().__init__(configfile, logfile)

    @classmethod
    async def create(cls, configfile, logfile):
        self = FileNotifier(configfile, logfile)

        cdm = self.getConfiguration()
        broker_addr = cdm['ROOT']['BASE_BROKER_ADDR']

        cred = self.getCredentials()
        service_user = cred.getUser('service_user')
        service_passwd = cred.getPasswd('service_passwd')
        url = f"amqp://{service_user}:{service_passwd}@{broker_addr}"
        self.base_broker_url = url
        self.reply_queue = "f99_consume"

        await self.setup_consumer()
        await self.setup_publisher()
        return self

    def on_message(self, ch, method, properties, body):
        ch.basic_ack(method.delivery_tag)
        print(body)
        # have to do this to get the basic_ack is sent out, to remove message from the queue; if
        # we stop the main event loop here, it never leaves the queue.
        task = asyncio.create_task(self.shutdown())

    async def shutdown(self):
        await asyncio.sleep(5)
        loop.stop()
        
    async def setup_consumer(self):
        self.consumer = Consumer(self.base_broker_url, None, self.reply_queue, self.on_message)
        self.consumer.start()

    async def setup_publisher(self):
        self.publisher = Publisher(self.base_broker_url, csc_parent=None)
        await self.publisher.start()

    async def publish_file_transfer_completed(self):
        d = {}
        d['MSG_TYPE'] = 'FILE_TRANSFER_COMPLETED'
        d['COMPONENT'] = 'ARCHIVE_CTRL'
        d['FILENAME'] = "/data/staging/atforwarder/myfile.txt"
        d['JOB_NUM'] = 1
        d['SESSION_ID'] = "session 1"
        d['REPLY_QUEUE'] = self.reply_queue

        await self.publisher.publish_message("archive_ctrl_consume", d)


async def main():
    note = await FileNotifier.create("L1SystemCfg.yaml", "FileNotifier.log")
    await note.publish_file_transfer_completed()

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
loop.close()
