#!/bin/bash
python3 -u main.py ssb4.nt.ntnu.no:20030 bgw-cache >> logs/updateProd.log 2> logs/updateProd.err &
