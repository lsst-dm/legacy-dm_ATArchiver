# This file is part of dm_ATArchiver
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
import logging
import traceback
from lsst.dm.csc.base.publisher import Publisher
from lsst.dm.csc.base.consumer import Consumer

from lsst.dm.csc.base.director import Director
from lsst.dm.ATArchiver.atscoreboard import ATScoreboard

LOGGER = logging.getLogger(__name__)


class Waiter:

    def __init__(self, evt, parent):
        self.evt = evt
        self.evt.set()
        self.parent = parent

    async def pause(self, code, report):
        await asyncio.sleep(5)
        if self.evt.is_set():
            LOGGER.info(report)
            self.parent.fault(code=code, report=report)


class ATDirector(Director):

    def __init__(self, parent, config_filename, log_filename):
        super().__init__(config_filename, log_filename)
        self.parent = parent

        self._msg_actions = {'AT_FWDR_HEALTH_CHECK_ACK': self.process_forwarder_health_check_ack,
                             'ARCHIVE_HEALTH_CHECK_ACK': self.process_archiver_health_check_ack,
                             'AT_FWDR_XFER_PARAMS_ACK': self.process_xfer_params_ack,
                             'AT_FWDR_END_READOUT_ACK': self.process_at_fwdr_end_readout_ack,
                             'AT_FWDR_HEADER_READY_ACK': self.process_header_ready_ack,
                             'AT_ITEMS_XFERD_ACK': self.process_at_items_xferd_ack,
                             'NEW_AT_ARCHIVE_ITEM_ACK': self.process_new_at_item_ack}

        cdm = self.getConfiguration()

        root = cdm["ROOT"]

        self.redis_host = root["REDIS_HOST"]
        self.redis_db = root["ATARCHIVER_REDIS_DB"]

        self.forwarder_consume_queue = root["FORWARDER_CONSUME_QUEUE"]
        self.forwarder_publish_queue = root["FORWARDER_PUBLISH_QUEUE"]
        self.forwarder_host = root["FORWARDER_HOST"]

        archive = root['ARCHIVE']
        self.archive_name = archive['ARCHIVE_LOGIN']
        self.archive_ip = archive['ARCHIVE_IP']
        self.archive_xfer_root = archive['ARCHIVE_XFER_ROOT']

        ats = root['ATS']
        self.wfs_raft = ats['WFS_RAFT']
        self.wfs_ccd = ats['WFS_CCD']

        self.forwarder_heartbeat_task = None
        self.archive_heartbeat_task = None

        self.startIntegration_evt = asyncio.Event()
        self.new_at_archive_item_evt = asyncio.Event()
        self.endReadout_evt = asyncio.Event()
        self.largeFileObjectAvailable_evt = asyncio.Event()

        self.forwarder_heartbeat_evt = asyncio.Event()
        self.archive_heartbeat_evt = asyncio.Event()

        self.services_started_evt = asyncio.Event()

        self.publisher = None
        self.archive_consumer = None
        self.forwarder_consumer = None
        self.telemetry_consumer = None

        self.scoreboard = None

    async def start_services(self):
        # this is the beginning of the start command, when we're about to go into disabled state
        LOGGER.info("start_services called")

        self.services_started_evt.set()
        try:
            self.scoreboard = ATScoreboard(db=self.redis_db, host=self.redis_host)
        except Exception as e:
            LOGGER.info(e)
            msg = "could't establish connection with redis broker"
            LOGGER.info(msg)
            self.parent.fault(5701, msg)
            return

        forwarder_info = None
        try:
            forwarder_info = self.scoreboard.pop_forwarder_from_list()

            LOGGER.info(f"pairing with forwarder {forwarder_info.hostname}")
            # record which forwarder we're paired to
            self.scoreboard.set_paired_forwarder_info(forwarder_info)
        except Exception as e:
            msg = f"no forwarder on forwarder_list in redis db {self.redis_db} on {self.redis_host}"
            LOGGER.info(msg)
            self.parent.fault(5701, msg)
            return

        await self.establish_connections(forwarder_info)

    async def stop_services(self):
        if self.services_started_evt.is_set():
            await self.rescind_connections()
            self.services_started_evt.clear()

    async def establish_connections(self, info):
        await self.setup_publishers()
        await self.setup_consumers()
        self.forwarder_heartbeat_task = asyncio.create_task(self.emit_heartbeat(info.hostname, info.consume_queue,
                                                                                'AT_FWDR_HEALTH_CHECK',
                                                                                self.forwarder_heartbeat_evt))

        self.archive_heartbeat_task = asyncio.create_task(self.emit_heartbeat("at_archive_controller",
                                                                               "archive_ctrl_consume",
                                                                               "ARCHIVE_HEALTH_CHECK",
                                                                               self.archive_heartbeat_evt))

    async def rescind_connections(self):
        LOGGER.info("rescinding connections")
        await self.stop_heartbeats()
        await self.stop_publishers()
        await self.stop_consumers()
        LOGGER.info("all connections rescinded")

    async def stop_heartbeats(self):
        LOGGER.info("stopping heartbeats")
        beats = [self.forwarder_heartbeat_task, self.archive_heartbeat_task]
        for beat in beats:
            await self.stop_heartbeat_task(beat)

    async def stop_heartbeat_task(self, task):
        if task is not None:
            task.cancel()
            await task

    async def setup_publishers(self):
        """ Set up base publisher with pub_base_broker_url by creating a new instance
            of AsyncioPublisher clas

            :params: None.

            :return: None.
        """
        LOGGER.info('Setting up ATArchiver publisher')
        self.publisher = Publisher(self.base_broker_url, csc_parent=self.parent)
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

        # messages from ArchiverController
        self.archive_consumer = Consumer(self.base_broker_url, self.parent, "archive_ctrl_publish",
                                         self.on_message)
        self.archive_consumer.start()

        # ack messages from Forwarder & ArchiveController
        self.forwarder_consumer = Consumer(self.base_broker_url,  self.parent, "at_foreman_ack_publish",
                                           self.on_message)
        self.forwarder_consumer.start()

        # telemetry messages from forwarder
        self.telemetry_consumer = Consumer(self.base_broker_url,  self.parent, "telemetry_queue",
                                           self.on_telemetry)
        self.telemetry_consumer.start()

    async def stop_consumers(self):
        LOGGER.info("stopping consumers")
        if self.archive_consumer is not None:
            self.archive_consumer.stop()
        if self.forwarder_consumer is not None:
            self.forwarder_consumer.stop()
        if self.telemetry_consumer is not None:
            self.telemetry_consumer.stop()

    def on_message(self, ch, method, properties, body):
        msg_type = body['MSG_TYPE']
        if (msg_type != 'AT_FWDR_HEALTH_CHECK_ACK') and (msg_type != 'ARCHIVE_HEALTH_CHECK_ACK'):
            LOGGER.info("received message")
            LOGGER.info(body)
        ch.basic_ack(method.delivery_tag)
        handler = self._msg_actions.get(body['MSG_TYPE'])
        handler(body)

    def on_telemetry(self, ch, method, properties, body):
        task = asyncio.create_task(self.parent.send_processingStatus(body['STATUS_CODE'],body['DESCRIPTION']))
        ch.basic_ack(method.delivery_tag)

    def process_ack(self, msg):
        pass

    def process_at_items_xferd_ack(self, msg):
        LOGGER.info("process_at_items_xferd: ack received")

    def process_forwarder_health_check_ack(self, msg):
        self.forwarder_heartbeat_evt.clear()

    def process_archiver_health_check_ack(self, msg):
        self.archive_heartbeat_evt.clear()

    async def publish_message(self, queue, msg):
        await self.publisher.publish_message(queue, msg)

    def send_telemetry(self, status_code, description):
        msg = {}
        msg['MSG_TYPE'] = 'TELEMETRY'
        msg['DEVICE'] = self.DEVICE
        msg['STATUS_CODE'] = status_code
        msg['DESCRIPTION'] = description
        self.publish_message(self.TELEMETRY_QUEUE, msg)

    def build_archiver_message(self, data):
        d = {}
        # these are the old names.  This will be removed when we can update the other
        # parts of the code that depend on these names.

        d['MSG_TYPE'] = 'NEW_AT_ARCHIVE_ITEM'
        d['ACK_ID'] = 0
        d['JOB_NUM'] = self.get_next_jobnum()
        d['SESSION_ID'] = self.get_session_id()
        d['IMAGE_ID'] = data.imageName
        d['REPLY_QUEUE'] = 'at_foreman_ack_publish'
        return d

        d['imageName'] = data.imageName
        # The container containing this XML imageIndex in ATCamera_Events.xml.
        # It's in the newer version of the XML
        # d['imageIndex'] = data.imageIndex
        d['imageSequenceName'] = data.imageSequenceName
        d['imagesInSequence'] = data.imagesInSequence
        # d['timeStamp'] = data.timeStamp
        d['exposureTime'] = data.exposureTime
        d['priority'] = data.priority
        return d

    def build_startIntegration_message(self, data):
        d = {}
        d['MSG_TYPE'] = 'AT_FWDR_XFER_PARAMS'
        d['SESSION_ID'] = self.get_session_id()
        d['IMAGE_ID'] = data['IMAGE_ID']
        d['DEVICE'] = 'AT'
        d['JOB_NUM'] = self.get_jobnum()
        d['ACK_ID'] = 0
        d['REPLY_QUEUE'] = 'at_foreman_ack_publish'
        targetDir = data['TARGET_DIR']
        location = f"{self.archive_name}@{self.archive_ip}:{targetDir}"
        d['TARGET_LOCATION'] = location

        xfer_params = {}
        xfer_params['RAFT_LIST'] = self.wfs_raft
        xfer_params['RAFT_CCD_LIST'] = self.wfs_ccd
        xfer_params['AT_FWDR'] = self.forwarder_host  # self._current_fwdr['FQN']

        d['XFER_PARAMS'] = xfer_params
        return d

    def build_endReadout_message(self, data):
        d = {}
        d['MSG_TYPE'] = 'AT_FWDR_END_READOUT'
        d['JOB_NUM'] = self.get_jobnum()
        d['SESSION_ID'] = self.get_session_id()
        d['IMAGE_ID'] = data.imageName
        d['ACK_ID'] = 0
        d['REPLY_QUEUE'] = 'at_foreman_ack_publish'
        d['IMAGE_SEQUENCE_NAME'] = data.imageSequenceName
        d['IMAGES_IN_SEQUENCE'] = data.imagesInSequence
        # uncomment this when the IDL is fixed
        # d['IMAGE_INDEX'] = data.imageIndex
        return d

    def build_largeFileObjectAvailable_message(self, data):
        d = {}
        d['MSG_TYPE'] = 'AT_FWDR_HEADER_READY'
        d['FILENAME'] = data.url
        d['IMAGE_ID'] = data.id
        d['ACK_ID'] = 0
        d['REPLY_QUEUE'] = 'at_foreman_ack_publish'
        return d

    def process_new_at_item_ack(self, data):
        self.new_at_archive_item_evt.clear()
        LOGGER.info(f"{data}")
        # this is scheduled, since process_new_at_item_ack is never await-ed
        task = asyncio.create_task(self.send_startIntegration(data))

    async def send_startIntegration(self, data):
        msg = self.build_startIntegration_message(data)

        await self.publish_message(self.forwarder_consume_queue, msg)
        LOGGER.info("startIntegration sent to forwarder")

        code = 5752
        report = f"No xfer_params response from forwarder. Setting fault state with code = {code}"

        waiter2 = Waiter(self.startIntegration_evt, self.parent)
        self.startIntegration_ack_task = asyncio.create_task(waiter2.pause(code, report))

    #
    # startIntegration
    #
    async def transmit_startIntegration(self, data):

        # first we send a message to the archiver, to obtain the correct target directory
        msg = self.build_archiver_message(data)

        await self.publish_message("archive_ctrl_consume", msg)

        code = 5752
        report = f"No ack response from at archive controller"

        # now we set up a wait for the ack. If the ack doesn't appear within the time 
        # frame allotted, a fault is thrown.  Otherwise, when the ack message is received,
        # the data is extracted within the "process_new_at_item_ack" method, and the
        # "startIntegration" message is build and sent to the forwarder from that method
        waiter1 = Waiter(self.new_at_archive_item_evt, self.parent)
        self.new_at_archive_item_ack_task = asyncio.create_task(waiter1.pause(code, report))

    def process_xfer_params_ack(self, msg):
        self.startIntegration_evt.clear()
        LOGGER.info("startIntegration ack received")
        pass

    #
    # endReadout
    #
    async def transmit_endReadout(self, data):
        msg = self.build_endReadout_message(data)
        await self.publish_message(self.forwarder_consume_queue, msg)

        code = 5753
        report = f"No endReadout ack from forwarder. Setting fault state with code = {code}"

        waiter = Waiter(self.endReadout_evt, self.parent)
        self.endReadout_ack_task = asyncio.create_task(waiter.pause(code, report))

    def process_at_fwdr_end_readout_ack(self, msg):
        self.endReadout_evt.clear()
        LOGGER.info("endReadout ack received")
        pass

    #
    # largeFileObjectAvailable
    #
    async def transmit_largeFileObjectAvailable(self, data):
        msg = self.build_largeFileObjectAvailable_message(data)
        await self.publish_message(self.forwarder_consume_queue, msg)

        code = 5754
        report = f"No largeFileObjectAvailable ack from forwarder. Setting fault state with code = {code}"

        waiter = Waiter(self.largeFileObjectAvailable_evt, self.parent)
        self.largeFileObjectAvailable_ack_task = asyncio.create_task(waiter.pause(code, report))

    def process_header_ready_ack(self, msg):
        self.largeFileObjectAvailable_evt.clear()
        LOGGER.info("largeFileObjectAvailable ack received")
        pass

    #
    # Heartbeat
    #
    async def emit_heartbeat(self, component_name, queue, msg_type, heartbeat_event):
        try:
            LOGGER.info(f"starting heartbeat with {component_name} on {queue}")

            interval = 5  # seconds

            pub = Publisher(self.base_broker_url, csc_parent=self.parent, logger_level=LOGGER.debug)
            await pub.start()

            while True:
                msg = {"MSG_TYPE": msg_type,
                       "ACK_ID": 0,
                       "SESSION_ID": self.get_session_id(),
                       "REPLY_QUEUE": "at_foreman_ack_publish"}
                LOGGER.debug(f"about to send {msg}")
                await pub.publish_message(queue, msg)

                code=5751
                report=f"failed to received heartbeat ack from {component_name}"

                waiter = Waiter(heartbeat_event, self.parent)
                heartbeat_task = asyncio.create_task(waiter.pause(code, report))
                await heartbeat_task
                if heartbeat_event.is_set():
                    await pub.stop()
                    return
        except asyncio.CancelledError:
            await pub.stop()
        except Exception as e:
            LOGGER.info(f"failed to publish message to {queue}: "+str(e))
            await pub.stop()
            self.parent.fault(code=5751, report=f"failed to publish message to {queue}")
            return
