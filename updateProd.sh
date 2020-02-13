#!/bin/bash
python3 -u main.py 18110 bgw-cache >> logs/updateProd.log 2> logs/updateProd.err &
