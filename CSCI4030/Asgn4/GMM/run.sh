input='data/train_img.csv'
output="output"

m=40
r=1

error=999999
i=0


while [ $i -lt 20 ]; do
    hadoop fs -rm -r output
    rm -rf output
    hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
        -D mapred.map.tasks=$m \
        -D mapred.reduce.tasks=$r \
        -D mapred.compress.map.output=ture \
        -file ori_params.txt \
        -file mapper1.py -mapper mapper1.py  \
        -file reducer1.py -reducer reducer1.py  \
        -input $input \
        -output output
    hadoop fs -get output
    
    hadoop fs -rm -r output2
    rm -rf output2
    hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
        -D mapred.map.tasks=$m \
        -D mapred.reduce.tasks=$r \
        -D mapred.compress.map.output=ture \
        -file output \
        -file ori_params.txt \
        -file mapper2.py -mapper mapper2.py  \
        -file reducer2.py -reducer reducer2.py  \
        -input $input \
        -output output2
    hadoop fs -get output2
    cp output2/part-00000 ./ori_params.txt

    i=$(( i + 1 ))
done
