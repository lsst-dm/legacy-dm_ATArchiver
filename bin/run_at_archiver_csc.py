#!/usr/bin/env python
import asyncio
from lsst.dm.ATArchiver.atarchiver_csc import ATArchiverCSC


csc = ATArchiverCSC(index=None, initial_simulation_mode=False)
asyncio.get_event_loop().run_until_complete(csc.done_task)
