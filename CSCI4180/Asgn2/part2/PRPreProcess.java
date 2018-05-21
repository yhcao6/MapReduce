import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class PRPreProcess {

    public static class countMapper
        extends Mapper<Text,Text,Text,Text>{
        
        public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException{
            context.write(key, value);
            context.write(new Text(value.toString().split(" ")[0]), new Text());
        }
    }

    public static class countReducer
            extends Reducer<Text, Text, Text, Text>{

            private long numNodes = 0;

            public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
                numNodes += 1;
            }

            public void cleanup(Context context)
                throws IOException, InterruptedException{
                context.getCounter(PageRank.myCounter.numNodes).setValue(numNodes);
            }
    }
    
    public static class PRPreProcessMapper
        extends Mapper<Text,Text,Text,Text>{

        public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException{
            context.write(key, value);
            Text neighbor = new Text(value.toString().split(" ")[0]);
            context.write(neighbor, new Text("0 0"));
        }
    }

    public static class PRPreProcessReducer
        extends Reducer<Text,Text,LongWritable,PRNodeWritable>{
        
        public LongWritable strToLongWritable(String str){
            return new LongWritable(Long.valueOf(str));
        }

        private MultipleOutputs<Text, Text> mos;

        public void setup(Context context)
            throws IOException, InterruptedException{
            mos = new MultipleOutputs(context);
        }

        public void reduce( Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException{
                Long numNodes = Long.parseLong(context.getConfiguration().get("numNodes"));
                PRNodeWritable node = new PRNodeWritable();
                node.setNodeId(strToLongWritable(key.toString()));
                
                node.setMass(new FloatWritable((float)1/numNodes));

                for (Text val : values){
                    // mos.write("outputText", key, val, "/mos");
                    String[] kv = val.toString().split(" ");
                    //kv[0] : key ; kv[1] : value
                    if (!kv[0].equals("0")){
                        node.putAdjList(strToLongWritable(kv[0]), strToLongWritable(kv[1]));
                    }
                }

                context.write(node.getNodeId(), node);
            }

        public void cleanup(Context context)
            throws IOException, InterruptedException{
            mos.close();
        }
        }

}
