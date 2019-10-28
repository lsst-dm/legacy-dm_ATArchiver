#!/usr/bin/env python3
import asyncio
from lsst.dm.ATArchiver.atarchive_controller import ATArchiveController

async def main():
    controller = await ATArchiveController.create("DM_ATARCHIVER", config_filename="atarchiver_config.yaml",
                                                  log_filename="ATArchiveController.log")
loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
loop.close()
