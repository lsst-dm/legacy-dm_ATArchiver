#!/bin/sh
echo "sending start"
python $DM_CSC_BASE_DIR/bin.src/command.py -D ATArchiver start -s normal
echo "sending enable"
python $DM_CSC_BASE_DIR/bin.src/command.py -D ATArchiver enable
