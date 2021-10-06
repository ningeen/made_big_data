hdfs dfs -rm -r /block3/output
mapred streaming \
-file /mapreduce/mapper.py -file /mapreduce/reducer.py \
-mapper /mapreduce/mapper.py -reducer /mapreduce/reducer.py \
-input /block3/AB_NYC_2019.csv -output /block3/output
hdfs dfs -cat /block3/output/part-00000  > /mapreduce/result.txt
/mapreduce/casual.py >> /mapreduce/result.txt