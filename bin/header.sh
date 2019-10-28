#!/bin/sh
#python3 atevent.py largeFileObjectAvailable -b 1 -c 1 -g "AtHeaderService" -m 1 -u "http://141.142.238.74:8000/AT_O_20190312_000007.header" -v 1.0 -i AT_O_20190312_000007
python3 atevent.py largeFileObjectAvailable -b 1 -c 1 -g "AtHeaderService" -m 1 -u "http://141.142.238.174:8000/$1.header" -v 1.0 -i $1
