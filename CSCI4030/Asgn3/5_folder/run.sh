m=4
r=2

n=0

while [ $n -lt 5 ]; do
    hadoop fs -rm -r output
    rm -rf output
    input="5_folder_input/iter_${n}/train_image.csv"
    output="output"
    cp ./ori_centroid_points.txt ./old_centroid_points.txt
    cp ./ori_centroid_points.txt ./new_centroid_points.txt
    error=999999
    rm -rf "iter_${n}_error.txt"
    i=0

    while [ $error -ne 0 ]; do
    # while [ $i -lt 40 ]; do
        rm -rf output

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
        echo "$iteration ${i}, error is ${error}" >> "iter_${n}_error.txt"
        cp new_centroid_points.txt old_centroid_points.txt
    
        # rm -rf output
        i=$(( i + 1 ))
    done
    cp new_centroid_points.txt "iter_${n}_centroid_points.txt"
    n=$(( n + 1 ))
done
