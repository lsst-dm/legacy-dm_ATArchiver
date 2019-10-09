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

import json
import logging
from lsst.dm.ATArchiver.forwarder_info import ForwarderInfo
# from collections import namedtuple
from lsst.dm.csc.base.scoreboard import Scoreboard

LOGGER = logging.getLogger(__name__)


class ATScoreboard(Scoreboard):

    def __init__(self, db, host, port=6379):
        super().__init__("ATArchiver", db, host, port)

        self.JOBNUM = "jobnum"
        self.PAIRED_FORWARDER = "paired_forwarder"
        self.FORWARDER_LIST = "forwarder_list"

    def get_jobnum(self):
        return self.conn.hget(self.device, self.JOBNUM)

    def set_jobnum(self, jobnum):
        self.conn.hset(self.device, self.JOBNUM, jobnum)

    def pop_forwarder_from_list(self):
        LOGGER.info(f"popping from {self.FORWARDER_LIST}")
        data = self.conn.brpop(self.FORWARDER_LIST, 1)
        if data is None:
            LOGGER.info("No forwarder available on scoreboard list")
            raise RuntimeError("No forwarder available on scoreboard list")
        item = data[1]
        d = json.loads(item)

        return self.create_forwarder(d)

    def push_forwarder_onto_list(self, forwarder_info):
        info = forwarder_info.__dict__
        data = json.dumps(info)
        self.conn.lpush(self.FORWARDER_LIST, data)

    def get_paired_forwarder_info(self):
        data = self.conn.hget(self.device, self.PAIRED_FORWARDER)
        d = json.loads(data)
        return self.create_forwarder(d)


    def create_forwarder(self, d):
        try:
            hostname = d['hostname']
            ip_address = d['ip_address']
            consume_queue = d['consume_queue']
            forwarder_info = ForwarderInfo(hostname=hostname, ip_address=ip_address, consume_queue=consume_queue)
            return forwarder_info
        except Exception as e:
            LOGGER.info("Exception: "+str(e))
            return None

    def check_forwarder_presence(self, forwarder_key):
        return self.conn.get(self.device, forwarder_key)

    def set_forwarder_association(self, forwarder_hostname, timeout):
        LOGGER.info(f'setting atarchiver_association = {forwarder_hostname}')
        self.conn.set("atarchiver_association", forwarder_hostname, timeout)

    def delete_forwarder_association(self):
        LOGGER.info(f'deleting atarchiver_association')
        self.conn.delete("atarchiver_association")

    def set_paired_forwarder_info(self, forwarder_info, timeout):
        info = forwarder_info.__dict__
        data = json.dumps(info)
        self.conn.hset(self.device, self.PAIRED_FORWARDER, data)


if __name__ == "__main__":

    AT_ARCHIVER_DB = 15

    board = ATScoreboard(db=AT_ARCHIVER_DB, host="localhost")

    new_state = "DISABLED"
    board.set_state(new_state)
    state = board.get_state()
    if state != new_state:
        print(f"state was {state} and not {new_state} as expected")

    new_session = "Session 1"
    board.set_session(new_session)
    session = board.get_session()
    if session != new_session:
        print(f"session was {session} and not {new_session} as expected")

    new_jobnum = "Job 1"
    board.set_jobnum(new_jobnum)
    jobnum = board.get_jobnum()
    if jobnum != new_jobnum:
        print(f"jobnum was {jobnum} and not {new_jobnum} as expected")

    new_forwarder_info = ForwarderInfo("test", "test_queue")
    board.push_forwarder_onto_list(new_forwarder_info)
    info = board.pop_forwarder_from_list()
    if info.name != new_forwarder_info.name:
        print(f"info.name was {info.name} and not {new_forwarder_info.name} as expected")
    if info.queue != new_forwarder_info.queue:
        print(f"info.queue was {info.queue} and not {new_forwarder_info.queue} as expected")

    board.set_paired_forwarder_info(new_forwarder_info)

    paired = board.get_paired_forwarder_info()
    if paired.name != new_forwarder_info.name:
        print(f"paired.name was {paired.name} and not {new_forwarder_info.name} as expected")
    if paired.queue != new_forwarder_info.queue:
        print(f"paired.queue was {paired.queue} and not {new_forwarder_info.queue} as expected")
