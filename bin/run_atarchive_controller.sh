#!/bin/bash
#source $DM_ATARCHIVER_DIR/bin/setup_atarchiver.sh
export IIP_LOG_DIR=/var/log/iip
LOGPATH=/tmp/ospl_logs.$$
mkdir $LOGPATH
export OSPL_LOGPATH=$LOGPATH
run_archiver_controller.py
