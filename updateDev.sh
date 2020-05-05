#!/bin/bash
python3 -u main.py ssb4.nt.ntnu.no:20030 bgw-dev >> logs/updateDev.log 2> logs/updateDev.err &
