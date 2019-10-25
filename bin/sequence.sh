#!/bin/bash
$DM_ATARCHIVER_DIR/bin/startIntegration.sh $1
$DM_ATARCHIVER_DIR/bin/endReadout.sh $1
$DM_ATARCHIVER_DIR/bin/header.sh $1
