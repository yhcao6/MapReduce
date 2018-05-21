hadoop fs -rm -r /output

input="/m.txt"
output="/output"
m1=64
r1=8

hadoop jar ~/hadoop-2.9.0/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=1 \
    -D map.output.key.field.separator=" " \
    -D mapreduce.partition.keycomparator.options="-k1,1n -k2,2n" \
    -D mapreduce.partition.keypartitioner.options=-k1,1 \
    -D mapred.map.tasks=$m1 \
    -D mapred.reduce.tasks=$r1 \
    -D mapred.compress.map.output=ture \
    -file mapper.py -mapper mapper.py  \
    -file combiner.py -combiner combiner.py \
    -file reducer.py -reducer reducer.py  \
    -input $input \
    -output $output \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
