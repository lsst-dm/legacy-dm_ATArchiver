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

from lsst.dm.csc.base.message_director import MessageDirector
import logging

LOGGER = logging.getLogger(__name__)


class ATDirector(MessageDirector):
    """ Specialization of MessageDirector to handle archiver transactions
    """
    def __init__(self, parent, name, config_filename, log_filename):
        super().__init__(parent, name, config_filename, log_filename)
        self.parent = parent

        self._msg_actions = {'IMAGE_IN_OODS': self.process_image_in_oods,
                             'ARCHIVE_HEALTH_CHECK_ACK': self.process_archiver_health_check_ack,
                             'AT_FWDR_XFER_PARAMS_ACK': self.process_xfer_params_ack,
                             'AT_FWDR_END_READOUT_ACK': self.process_fwdr_end_readout_ack,
                             'AT_FWDR_HEADER_READY_ACK': self.process_header_ready_ack,
                             'AT_ITEMS_XFERD_ACK': self.process_items_xferd_ack,
                             'ASSOCIATED_ACK': self.process_association_ack,
                             'NEW_AT_ARCHIVE_ITEM_ACK': self.process_new_item_ack}

        #self.CAMERA_NAME = "LATISS"
        #self.ARCHIVE_CONTROLLER_NAME = 'at_archive_controller'
        #self.FWDR_HEALTH_CHECK_ACK = 'AT_FWDR_HEALTH_CHECK_ACK'
        #self.ARCHIVER_NAME = 'ATArchiver'
        #self.SHORT_NAME = 'AT'
        #self.ASSOCIATION_KEY = 'atarchiver_association'
        #
        #self.FILE_INGEST_REQUEST = 'AT_FILE_INGEST_REQUEST'
        #self.NEW_ARCHIVE_ITEM = 'NEW_AT_ARCHIVE_ITEM'
        #self.FWDR_XFER_PARAMS = 'AT_FWDR_XFER_PARAMS'
        #self.FWDR_END_READOUT = 'AT_FWDR_END_READOUT'
        #self.FWDR_HEADER_READY = 'AT_FWDR_HEADER_READY'

    def configure(self):
        super().configure()
        config = self.getConfiguration()
        root = config['ROOT']
        self.CAMERA_NAME = self.config_val(root, 'CAMERA_NAME')
        self.ARCHIVE_CONTROLLER_NAME = self.config_val(root, 'ARCHIVE_CONTROLLER_NAME')
        self.FWDR_HEALTH_CHECK_ACK = self.config_val(root, 'FWDR_HEALTH_CHECK_ACK')
        self.ARCHIVER_NAME = self.config_val(root, 'ARCHIVER_NAME')
        self.SHORT_NAME = self.config_val(root, 'SHORT_NAME')
        self.ASSOCIATION_KEY = self.config_val(root, 'ASSOCIATION_KEY')
        
        self.FILE_INGEST_REQUEST = self.config_val(root, 'FILE_INGEST_REQUEST')
        self.NEW_ARCHIVE_ITEM = self.config_val(root, 'NEW_ARCHIVE_ITEM')
        self.FWDR_XFER_PARAMS = self.config_val(root, 'FWDR_XFER_PARAMS')
        self.FWDR_END_READOUT = self.config_val(root, 'FWDR_END_READOUT')
        self.FWDR_HEADER_READY = self.config_val(root, 'FWDR_HEADER_READY')

        self.OODS_CONSUME_QUEUE = self.config_val(root, 'OODS_CONSUME_QUEUE')
        self.OODS_PUBLISH_QUEUE = self.config_val(root, 'OODS_PUBLISH_QUEUE')
        self.ARCHIVE_CTRL_PUBLISH_QUEUE = self.config_val(root, 'ARCHIVE_CTRL_PUBLISH_QUEUE')
        self.ARCHIVE_CTRL_CONSUME_QUEUE = self.config_val(root, 'ARCHIVE_CTRL_CONSUME_QUEUE')
        self.TELEMETRY_QUEUE = self.config_val(root, 'TELEMETRY_QUEUE')
    
