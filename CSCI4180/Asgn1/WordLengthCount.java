import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthCount {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

            private Text word = new Text();
			private IntWritable len = new IntWritable();
			private IntWritable count = new IntWritable();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				Map<Integer, Integer> map = new HashMap<Integer, Integer>();
				Integer l;

                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
					l = itr.nextToken().length();
					if (map.containsKey(l)){
						map.put(l, map.get(l) + 1);
					}else{
						map.put(l, 1);
					}
                }
				for (Integer k: map.keySet()){
					len.set(k);
					count.set(map.get(k));
					context.write(len, count);
				}
            }
    }

    public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
                    ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
