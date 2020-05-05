#!/bin/bash
python3 -u main.py ssb4.nt.ntnu.no:19070 bgw-dev >> logs/updateDev.log 2> logs/updateDev.err &
