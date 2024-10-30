#! /bin/bash 

#Q3
hdfs dfs -mkdir Assignment1
#Q4
hdfs dfs -copyFromLocal /nfs/home/omo6093/test.txt Assignment1
#Q5
hdfs dfs -mv Assignment1/test.txt Assignment1/renamed_test.txt
#Q6
hdfs dfs -copyToLocal Assignment1/renamed_test.txt /nfs/home/omo6093/renamed_test.txt
#Q7
hdfs dfs -mkdir Assignment1_part7
hdfs dfs -copyFromLocal /nfs/home/omo6093/test1.txt Assignment1_part7
hdfs dfs -copyFromLocal /nfs/home/omo6093/test2.txt Assignment1_part7
hdfs dfs -getmerge Assignment1_part7/test1.txt Assignment1_part7/test2.txt /nfs/home/omo6093/hw1_pt7.txt
#Q8
hdfs dfs -copyFromLocal hw1_pt7.txt Assignment1
hdfs dfs -cp Assignment1/hw1_pt7.txt /user/ml057868


