#!/bin/sh
python3 2020CS10567_server.py 5 & 
sleep 1 &
python3 2020CS10567_client.py 5