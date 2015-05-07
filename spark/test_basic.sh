#!/bin/bash
./driver.py > res_spark.txt
python word_count_script.py > res_wordcount.txt
