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

import logging
import pathlib
from lsst.dm.csc.base.dm_csc import dm_csc
from lsst.dm.ATArchiver.atdirector import ATDirector
from lsst.dm.ATArchiver.atscoreboard import ATScoreboard
from lsst.ts import salobj

LOGGER = logging.getLogger(__name__)


class ATArchiverCSC(dm_csc):

    def __init__(self, index, config_dir=None, initial_state=salobj.State.STANDBY,
                 initial_simulation_mode=0):
        schema_path = pathlib.Path(__file__).resolve().parents[4].joinpath("schema", "ATArchiver.yaml")
        super().__init__("ATArchiver", index=index, schema_path=schema_path, config_dir=config_dir,
                         initial_state=initial_state, initial_simulation_mode=initial_simulation_mode)

        domain = salobj.Domain()

        salinfo = salobj.SalInfo(domain=domain, name="ATArchiver", index=0)

        camera_events = {'endReadout', 'startIntegration'}
        self.camera_remote = salobj.Remote(domain, "ATCamera", index=0, readonly=True, include=camera_events,
                                           evt_max_history=0)
        self.camera_remote.evt_endReadout.callback = self.endReadoutCallback
        self.camera_remote.evt_startIntegration.callback = self.startIntegrationCallback

        efd_events = {'largeFileObjectAvailable'}
        self.efd_remote = salobj.Remote(domain, "EFD", index=0, readonly=True, include=efd_events,
                                        evt_max_history=0)
        self.efd_remote.evt_largeFileObjectAvailable.callback = self.largeFileObjectAvailableCallback

        self.director = ATDirector(self, "L1SystemCfg.yaml", "ATArchiverCSC.log")

        config = self.director.getConfiguration()
        self.redis_host = config["ROOT"]["REDIS_HOST"]
        self.redis_db = int(config["ROOT"]["ATARCHIVER_REDIS_DB"])
        self.scoreboard = None
        LOGGER.info("************************ Starting ATArchiver ************************")

    async def send_processingStatus(self, statusCode, description):
        LOGGER.info(f"sending {statusCode}: {description}")
        self.evt_processingStatus.set_put(statusCode=statusCode, description=description)

    async def begin_start(self, data):
        # this is the beginning of the start command, when we're about to go into disabled state
        LOGGER.info("begin_start called")

        self.scoreboard = ATScoreboard(db=self.redis_db, host=self.redis_host)

        forwarder_info = self.scoreboard.pop_forwarder_from_list()

        LOGGER.info(f"pairing with forwarder {forwarder_info.name}")
        # record which forwarder we're paired to
        self.scoreboard.set_paired_forwarder_info(forwarder_info)

        # create consumers and publishers, and heartbeat
        await self.director.establish_connections(forwarder_info)

    async def begin_standby(self, data):
        # About to go into standby state
        await self.director.rescind_connections()

    async def do_resetFromFault(self, data):
        print("do_resetFromFault called")
        print(data)

    async def startIntegrationCallback(self, data):
        self.assert_enabled("startIntegration")
        LOGGER.info("startIntegration callback")
        await self.director.transmit_startIntegration(data)

    async def endReadoutCallback(self, data):
        self.assert_enabled("endReadout")
        LOGGER.info("endReadout")
        await self.director.transmit_endReadout(data)

    async def largeFileObjectAvailableCallback(self, data):
        self.assert_enabled("largeFileObjectAvailable")
        LOGGER.info("largeFileObjectAvailable")
        await self.director.transmit_largeFileObjectAvailable(data)
