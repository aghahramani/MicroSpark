#!/bin/bash
echo spark run time
time ./driver.py > res_spark.txt
echo local run time
time python word_count_script.py > res_wordcount.txt
echo If no output then diff returns nothing.
diff res_spark.txt res_wordcount.txt
