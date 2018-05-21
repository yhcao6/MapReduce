import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Queue;
import java.util.LinkedList;
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

public class NgramCount{

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
			private IntWritable count = new IntWritable();
			Queue<String> q = new LinkedList<String>();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				String[] words = value.toString().split("\\P{Alpha}+");
				int N = Integer.parseInt(context.getConfiguration().get("N"));
				Map<String, Integer> map = new HashMap<String, Integer>();
				String ngram;
				for (int i = 0; i < words.length; i++){
					if (words[i].length() > 0)
					{
						q.add(words[i]);
					}

					if (q.size() > N)
					{
						q.remove();
					}

					if (q.size() == N)
					{
						ngram = String.join(" ", q);
						if (map.containsKey(ngram)){
							map.put(ngram, map.get(ngram) + 1);
						}else{
							map.put(ngram, 1);
						}
					}
               	}

				for (String k: map.keySet()){
					word.set(k);
					count.set(map.get(k));
					context.write(word, count);
				}
			}
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
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
		conf.set("N", args[2]);
        Job job = Job.getInstance(conf, "N gram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
