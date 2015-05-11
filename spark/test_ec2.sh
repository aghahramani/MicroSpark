#!/bin/bash
killall -9 python
killall -9 worker.py
killall -9 driver.py
echo test1 : Word count sort
echo spark run time
time python ./driver.py --ec2 --master 172.30.0.137 > res_spark.txt
echo local run time
time python word_count_script.py ./Data ./Data1 > res_wordcount.txt
echo If no output after this line then diff returns nothing Which means the test is working.
diff res_spark.txt res_wordcount.txt
