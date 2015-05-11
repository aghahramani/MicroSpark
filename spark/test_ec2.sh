#!/bin/bash
killall -9 python
killall -9 worker.py
killall -9 driver.py
python ./driver.py --ec2 --master 172.30.0.137
