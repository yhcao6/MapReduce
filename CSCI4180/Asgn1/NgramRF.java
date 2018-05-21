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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// pair implement
public class NgramRF{

    public static class NgramRFMapper
            extends Mapper<Object, Text, Text, IntWritable>{

            private IntWritable count = new IntWritable();
            private Text word = new Text();
			Queue<String> q = new LinkedList<String>();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				String[] words = value.toString().split("\\P{Alpha}+");
				int N = Integer.parseInt(context.getConfiguration().get("N"));
				Map<String, Integer> map = new HashMap<String, Integer>();
				String ngram;
				String margin;
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
						margin = q.element() + " " + "*";
						if (map.containsKey(ngram)){
							map.put(ngram, map.get(ngram) + 1);
						}else{
							map.put(ngram, 1);
						}

						if (map.containsKey(margin)){
							map.put(margin, map.get(margin) + 1);
						}else{
							map.put(margin, 1);
							// map.put(q.element() + " " + "~", 0); //indicate the end
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

	public static class NgramRFCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text ngram, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			int totalCount = 0;
			for (IntWritable count: counts){
				totalCount += count.get();
			}
			context.write(ngram, new IntWritable(totalCount));
		}
	}

    public static class NgramRFReducer extends Reducer<Text,IntWritable,Text,FloatWritable> {
			private String currentKey;
			private String currentPost;
			private String currentNgram;
			private int marginalCount = 0;
			private int joinCount = 0;
			private int currentCount = 0; // indicate the end
			private FloatWritable res = new FloatWritable();
			private Text word = new Text();

            public void reduce(Text ngram, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
				float Theta = Float.parseFloat(context.getConfiguration().get("Theta"));
				String key = ngram.toString().split(" ")[0];
				String post = ngram.toString().substring(key.length() + 1);

				if (post.equals("*")){
					if (key.equals(currentKey)){
						marginalCount += getTotalCount(counts);
					}else{
						// at first the joinCount = 0, so no need to consider
						/*
						float rf = (float)joinCount / marginalCount;
						if (rf >= Theta){
							word.set(currentKey + " " + currentPost);
							res.set(rf);
							context.write(word, res);
						}*/
						marginalCount = 0;
						joinCount = 0;
						currentCount = 0;
						marginalCount += getTotalCount(counts);
					}
				}else{
					if (post == currentPost){
						int c = getTotalCount(counts);
						currentCount += c;
						joinCount += c;
					}else{
						float rf = (float)joinCount / marginalCount;
						if (rf >= Theta){
							word.set(currentKey + " " + currentPost);
							res.set(rf);
							context.write(word, res);
						}
						joinCount = 0;
						int c = getTotalCount(counts);
						currentCount += c;
						joinCount += c;
						if (currentCount == marginalCount){
							rf = (float)joinCount / marginalCount;
							if (rf >= Theta){
								word.set(key + " " + post);
								res.set(rf);
								context.write(word, res);
							}
						}
					}
				}
				currentKey = key;
				currentPost = post;
            }

			private int getTotalCount(Iterable<IntWritable> counts){
				int totalCount = 0;
				for (IntWritable count: counts){
					totalCount += count.get();
				}
				return totalCount;
			}
    }

	public static class NgramRFPartitioner extends Partitioner<Text, IntWritable>{
		public int getPartition(Text ngram, IntWritable num, int numPartitions){
			String key = ngram.toString().split(" ")[0];
			return key.hashCode() % numPartitions;
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", " ");
		conf.set("N", args[2]);
		conf.set("Theta", args[3]);
        Job job = Job.getInstance(conf, "N gram RF");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(NgramRFMapper.class);
        job.setCombinerClass(NgramRFCombiner.class);
		job.setPartitionerClass(NgramRFPartitioner.class);
        job.setReducerClass(NgramRFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
