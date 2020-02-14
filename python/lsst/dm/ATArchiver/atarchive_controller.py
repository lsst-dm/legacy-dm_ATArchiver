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
from lsst.dm.csc.base.archive_controller import ArchiveController

LOGGER = logging.getLogger(__name__)


class ATArchiveController(ArchiveController):
    def __init__(self, name, config_filename, log_filename):
        super().__init__(name, config_filename, log_filename)

    @classmethod
    async def create(cls, name, config_filename, log_filename):
        
        self = ATArchiveController(name, config_filename, log_filename)
        self._msg_actions = {'ARCHIVE_HEALTH_CHECK': self.process_health_check,
                             'NEW_AT_ARCHIVE_ITEM': self.process_new_archive_item,
                             'FILE_TRANSFER_COMPLETED': self.process_file_transfer_completed}

        await self.configure()
