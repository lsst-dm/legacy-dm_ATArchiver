#!/usr/bin/env python
import asyncio
from lsst.dm.ATArchiver.atarchiver_csc import ATArchiverCSC


csc = ATArchiverCSC(index=None)
asyncio.get_event_loop().run_until_complete(csc.done_task)
