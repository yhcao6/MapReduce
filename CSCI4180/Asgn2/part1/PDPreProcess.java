import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class PDPreProcess {
    
    public static class MyMapper
        extends Mapper<Text,Text,Text,Text>{
        //do nothing
    }

    public static class MyReducer
        extends Reducer<Text,Text,LongWritable,PDNodeWritable>{
        
        public LongWritable strToLongWritable(String str){
            return new LongWritable(Long.valueOf(str));
        }

        public void reduce( Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException{
                //get root from config, default 0
                Long rootNode = context.getConfiguration().getLong("ParallelDijkstra.root.node", 0);
                
                PDNodeWritable node = new PDNodeWritable();
                node.setNodeId(strToLongWritable(key.toString()));
                
                // if the current node is root node, set the distance to root be 0
                if (node.getNodeId().get() == rootNode){
                    node.setDistance(new LongWritable(0));
                }

                for (Text val : values){
                    String[] kv = val.toString().split(" ");
                    //kv[0] : key ; kv[1] : value
                    node.putAdjList(strToLongWritable(kv[0]), strToLongWritable(kv[1]));
                }

                context.write(node.getNodeId(), node);
                context.getCounter(ParallelDijkstra.NodeCounter.TOTAL).increment(1);
            }
    }
}
