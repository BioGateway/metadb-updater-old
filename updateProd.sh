#!/bin/bash
python3 -u main.py http://ssb4.nt.ntnu.no:18110 bgw-cache >> logs/updateProd.log 2> logs/updateProd.err &
