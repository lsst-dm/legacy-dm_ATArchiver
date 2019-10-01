import asyncio
import datetime
import logging
import os
import os.path
import sys
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.publisher import Publisher
from lsst.ctrl.iip.base import base

LOGGER = logging.getLogger(__name__)


class ATArchiveController(base):
    def __init__(self, config_filename, log_filename):
        super().__init__(config_filename, log_filename)

    @classmethod
    async def create(cls, config_filename, log_filename):
        
        self = ATArchiveController(config_filename, log_filename)
        self._msg_actions = {'ARCHIVE_HEALTH_CHECK': self.process_health_check,
                             'NEW_AT_ARCHIVE_ITEM': self.process_new_at_archive_item,
                             'FILE_TRANSFER_COMPLETED': self.process_file_transfer_completed}

        cdm = self.getConfiguration()
        root = cdm['ROOT']
        archive = root['ARCHIVE']

        if 'ATFORWARDER_STAGING' in archive:
            self.atforwarder_staging_dir = archive['ATFORWARDER_STAGING']
            LOGGER.info(f"forwarder will stage to {self.atforwarder_staging_dir}")
        else:
            msg = "ARCHIVE.ATFORWARDER_STAGING does not exist in configuration file"
            LOGGER.warn(msg)
            sys.exit(0)
        self.oods_staging_dir = None
        if 'OODS_STAGING' in archive:
            self.oods_staging_dir = archive['OODS_STAGING']
            LOGGER.info(f"oods files will be staged to {self.oods_staging_dir}")
        else:
            msg = "ARCHIVE.OODS_STAGING does not exist in config file; will not link for OODS"
            LOGGER.warn(msg)

        self.dbb_staging_dir = None
        if 'DBB_STAGING' in archive:
            self.dbb_staging_dir = archive['DBB_STAGING']
            LOGGER.info(f"dbb files will be staged to {self.dbb_staging_dir}")
        else:
            msg = "ARCHIVE.DBB_STAGING does not exist in config file; will not link for DBB"
            LOGGER.warn(msg)

        self.CHECKSUM_TYPE = None
        if 'CHECKSUM_TYPE' in archive:
            self.CHECKSUM_TYPE = archive['CHECKSUM_TYPE']
            if (self.CHECKSUM_TYPE) != 'MD5' and (self.CHECKSUM_TYPE) != 'CRC32':
                msg = f"CHECKSUM_TYPE set to {self.CHECKSUM_TYPE}, which is not supported; "
                msg = msg + "defaulting to no checksum"
                LOGGER.info(msg)
                self.CHECKSUM_TYPE = None
                self.CHECKSUM_ENABLED = False
            else:
                msg = f"checksums enabled; CHECKSUM_TYPE set to {self.CHECKSUM_TYPE}"
                LOGGER.info(msg)
        else:
            LOGGER.info("CHECKSUM_TYPE not specified, defaulting to no checksum")
            self.CHECKSUM_TYPE = None

        self.base_broker_addr = root["BASE_BROKER_ADDR"]

        cred = self.getCredentials()

        service_user = cred.getUser('service_user')
        service_passwd = cred.getPasswd('service_passwd')

        url = f"amqp://{service_user}:{service_passwd}@{self.base_broker_addr}"

        self.base_broker_url = url

        dir_list = [self.atforwarder_staging_dir, self.oods_staging_dir, self.dbb_staging_dir]
        self.create_directories(dir_list)
        await self.setup_consumers()
        await self.setup_publishers()
        return self

    def create_directories(self, dir_list):
        for directory in dir_list:
            if directory is not None:
                os.makedirs(os.path.dirname(directory), exist_ok=True)

    async def setup_publishers(self):
        LOGGER.info("Setting up ATArchiveController publisher")
        self.publisher = Publisher(self.base_broker_url, csc_parent=None)
        await self.publisher.start()

    async def stop_publishers(self):
        LOGGER.info("stopping publishers")
        if self.publisher is not None:
            await self.publisher.stop()

    async def setup_consumers(self):
        """ Create ThreadManager object with base broker url and kwargs to setup consumers.

            :params: None.

            :return: None.
        """

        # messages from ATArchiverCSC and Forwarder
        self.consumer = Consumer(self.base_broker_url, None, "archive_ctrl_consume",
                                 self.on_message)
        self.consumer.start()

    def on_message(self, ch, method, properties, body):
        if 'MSG_TYPE' not in body:
            LOGGER.info(f'received invalid message: {body}')
            return
        msg_type = body['MSG_TYPE']
        if msg_type != 'ARCHIVE_HEALTH_CHECK':
            LOGGER.info("received message")
            LOGGER.info(body)
        ch.basic_ack(method.delivery_tag)
        handler = self._msg_actions.get(body['MSG_TYPE'])

        loop = asyncio.get_event_loop()
        task = loop.create_task(handler(body))

    def build_new_item_ack_message(self, target_dir, data):
        d = {}
        d['MSG_TYPE'] = 'NEW_AT_ARCHIVE_ITEM_ACK'
        d['TARGET_DIR'] = target_dir
        d['ACK_ID'] = data['ACK_ID']
        d['JOB_NUM'] = data['JOB_NUM']
        d['IMAGE_ID'] = data['IMAGE_ID']
        d['COMPONENT'] = 'ARCHIVE_CTRL'
        d['ACK_BOOL'] = "TRUE"
        d['SESSION_ID'] = data['SESSION_ID']
        return d

    def build_health_ack_message(self, data):
        d = {}
        d['MSG_TYPE'] = "ARCHIVE_HEALTH_CHECK_ACK"
        d['COMPONENT'] = 'ARCHIVE_CTRL'
        d['ACK_BOOL'] = "TRUE"
        d['ACK_ID'] = data['ACK_ID']
        d['SESSION_ID'] = data['SESSION_ID']
        return d

    def construct_send_target_dir(self, target_dir):
        today = datetime.datetime.now()
        day_string = today.date()

        final_target_dir = target_dir + "/" + str(day_string) + "/"

        # This code allows two users belonging to the same group (such as ARCHIVE)
        # to both create and write to a specific directory.
        # The common group must be made the primary group for both users like this:
        # usermod -g ARCHIVE ATS_user
        # and the sticky bit must be set when the group is created.
        # chmod is called after creation to deal with system umask
        if os.path.isdir(final_target_dir):
            pass
        else:
            try:
                os.mkdir(final_target_dir, 0o2775)
            except Exception as e:
                LOGGER.error(f'failure to create {final_target_dir}: {e}')
            os.chmod(final_target_dir, 0o775)

        return final_target_dir

    async def process_health_check(self, msg):
        ack_msg = self.build_health_ack_message(msg)
        await self.publisher.publish_message('at_foreman_ack_publish', ack_msg)

    async def process_new_at_archive_item(self, msg):
        # send this to the archive staging area
        target_dir = self.construct_send_target_dir(self.atforwarder_staging_dir)

        ack_msg = self.build_new_item_ack_message(target_dir, msg)

        reply_queue = msg['REPLY_QUEUE']
        await self.publisher.publish_message(reply_queue, ack_msg)

    def build_file_transfer_completed_ack(self, data):
        d = {}
        d['MSG_TYPE'] = 'FILE_TRANSFER_COMPLETED_ACK'
        d['COMPONENT'] = 'ARCHIVE_CTRL'
        d['FILENAME'] = data['FILENAME']
        d['JOB_NUM'] = data['JOB_NUM']
        d['SESSION_ID'] = data['SESSION_ID']
        return d

    async def process_file_transfer_completed(self, msg):
        filename = msg['FILENAME']
        reply_queue = msg['REPLY_QUEUE']
        self.create_links_to_file(filename)
        ack_msg = self.build_file_transfer_completed_ack(msg)
        await self.publisher.publish_message(reply_queue, ack_msg)

    async def process_file_transfer_completed_ack(self, msg):
        LOGGER.info(f'ack received: {msg}')

    def create_link_to_file(self, filename, dirname):
        # remove the staging area portion from the filepath
        basefile = filename.replace(self.atforwarder_staging_dir, '').lstrip('/')

        # create a new full path to where the file will be linked for the OODS
        new_file = os.path.join(dirname, basefile)

        # hard link the file in the staging area
        try:
            # create the directory path where the file will be linked for the OODS
            new_dir = os.path.dirname(new_file)
            os.makedirs(new_dir, exist_ok=True)
            # hard link the file in the staging area
            os.link(filename, new_file)
            LOGGER.info(f"created link to {new_file}")
        except Exception as e:
            LOGGER.info(f'{e.message}')
            return False
        return True

    def create_links_to_file(self, forwarder_filename):

        linked = False
        if self.dbb_staging_dir is not None:
            linked = self.create_link_to_file(forwarder_filename, self.dbb_staging_dir)

        if self.oods_staging_dir is not None:
            linked = self.create_link_to_file(forwarder_filename, self.oods_staging_dir)

        if linked:
            # remove the original file, since we've linked it
            LOGGER.info(f"link was created successfully; removing {forwarder_filename}")
            os.unlink(forwarder_filename)
