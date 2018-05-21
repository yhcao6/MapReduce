if [ -d output ]; then
  rm -rf output
fi

if [ -d output2 ]; then
  rm -rf output2
fi

hdfs dfs -rm -r output
hdfs dfs -rm -r output2

input='input'
s=0.0025
m1=40
r1=4

output="output"

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    -D mapred.map.tasks=$m1 \
    -D mapred.reduce.tasks=$r1 \
    -D mapred.compress.map.output=ture \
    -file mapper1.py -mapper mapper1.py  \
    -file reducer1.py -reducer reducer1.py  \
    -cmdenv "s=${s}" \
    -input $input \
    -output output \

hdfs dfs -get output ./

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    -D mapred.map.tasks=$m1 \
    -D mapred.reduce.tasks=$r1 \
    -D mapred.compress.map.output=ture \
    -file mapper2.py -mapper mapper2.py  \
    -file reducer2.py -reducer reducer2.py  \
    -file output \
    -cmdenv "s=${s}" \
    -input $input \
    -output output2 \

hdfs dfs -get output2 ./

cat output2/* > part_c_tmp
sort -k4nr part_c_tmp | head -n 20 > part_c_res
rm -rf part_c_tmp
rm -rf output
rm -rf output2
