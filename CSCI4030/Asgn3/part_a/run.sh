hadoop fs -rm -r output

input='input'
output="output"

m=4
r=2

error=999999
i=0
cp ./ori_centroid_points.txt ./old_centroid_points.txt
cp ./ori_centroid_points.txt ./new_centroid_points.txt
rm error.txt

while [ $error -gt 1000 ]; do
    hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
        -D mapred.map.tasks=$m \
        -D mapred.reduce.tasks=$r \
        -D mapred.compress.map.output=ture \
        -file mapper1.py -mapper mapper1.py  \
        -file reducer1.py -reducer reducer1.py  \
        -file old_centroid_points.txt \
        -input $input \
        -output output \
    
    rm new_centroid_points.txt
    hadoop fs -get output ./
    hadoop fs -rm -r output
    cat output/* >> ./new_centroid_points.txt
    error="$(python cal_error.py)"
    echo "$iteration ${i}, error is ${error}" >> error.txt
    cp new_centroid_points.txt old_centroid_points.txt

    rm -rf output
    i=$(( i + 1 ))
done

