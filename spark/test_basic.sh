#!/bin/bash

./worker.py 4242 & 
./worker.py 4243 &
./worker.py 4244 &
./worker.py 4245 &
./worker.py 4246 &
./worker.py 4247 &
./worker.py 4248 &
./worker.py 4249 &
./worker.py 4250 &

./driver.py
