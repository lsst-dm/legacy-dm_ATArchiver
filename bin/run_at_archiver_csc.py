#!/usr/bin/env python
import asyncio
from lsst.dm.ATArchiver.atarchiver_csc import ATArchiverCSC


asyncio.run(ATArchiverCSC.amain(index=None))
