#!/bin/bash

source ~ARC/.bashrc

##
# Python setup
##

#setup ts_idl
#export TS_IDL_DIR=/opt/lsst/ts_idl
#export PYTHONPATH=${TS_IDL_DIR}/python:${PYTHONPATH}

# setup ts_salobj
#export TS_SALOBJ_DIR=/opt/lsst/ts_salobj
#export PYTHONPATH=${TS_SALOBJ_DIR}/python:${PYTHONPATH}

# setup ATArchiverCSC and ArchiveController

export DM_ATARCHIVER_DIR=/opt/lsst/dm_ATArchiver
export DM_CSC_BASE_DIR=/opt/lsst/dm_csc_base
export DM_CONFIG_AT_DIR=/opt/lsst/dm_config_at

export PYTHONPATH=${PYTHONPATH}:${DM_ATARCHIVER_DIR}/python:${DM_CSC_BASE_DIR}/python

# setup path for commands
export PATH=$PATH:${DM_ATARCHIVER_DIR}/bin:${DM_CSC_BASE_DIR}/bin

#setup sal
source /opt/lsst/setup_SAL.env

# LSST DDS DOMAIN for DDS/SAL communications
export LSST_DDS_DOMAIN=lsatmcs
