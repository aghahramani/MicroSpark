#!/bin/bash

./worker.py 4242 4242 4250 &
./worker.py 4243 4242 4250 &
./worker.py 4244 4242 4250 &
./worker.py 4245 4242 4250 &
./worker.py 4246 4242 4250 &
./worker.py 4247 4242 4250 &
./worker.py 4248 4242 4250 &
./worker.py 4249 4242 4250 &
./worker.py 4250 4242 4250 &

./driver.py
