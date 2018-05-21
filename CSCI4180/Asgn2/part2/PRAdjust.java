import java.io.IOException;

import java.util.Map;

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

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PRAdjust {


    public static class MyMapper 
            extends Mapper<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
             
            public void setup(Context context)
                throws IOException, InterruptedException{
            }

            public void map(LongWritable id, PRNodeWritable node, Context context)
                throws IOException, InterruptedException{

                float missMass = Float.parseFloat(context.getConfiguration().get("missMass"));
                long numNodes = Long.parseLong(context.getConfiguration().get("numNodes"));
                float alpha = Float.parseFloat(context.getConfiguration().get("alpha"));

                float newRank = alpha / numNodes + (1f - alpha) * (missMass / numNodes + node.getMass().get());
                node.setMassByFloat(newRank);
                context.write(id, node);
            }
            
            public void cleanup(Context context)
                throws IOException, InterruptedException{
            }
    }


    public static class MyReducer 
            extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
            private MultipleOutputs<LongWritable,FloatWritable> mos;

            public void setup(Context context)
                throws IOException, InterruptedException{
                mos = new MultipleOutputs(context);
            }

            public void reduce(LongWritable id, Iterable<PRNodeWritable> nodes, Context context)
                throws IOException, InterruptedException{

                long numIterations = Long.parseLong(context.getConfiguration().get("numIterations"));
                long maxIterations = Long.parseLong(context.getConfiguration().get("maxIterations"));
                String finalOutputPath = context.getConfiguration().get("finalOutputPath");

                float theta = Float.parseFloat(context.getConfiguration().get("theta"));

                for (PRNodeWritable node: nodes){
                    if (numIterations == maxIterations){
                        float mass = node.getMass().get();
                        if (mass > theta){
                            mos.write("outputText", id, node.getMass(), finalOutputPath);
                        }
                    } else {
                        context.write(id, node);
                    }
                }
            }

            public void cleanup(Context context)
                throws IOException, InterruptedException{
                mos.close();
            }
    }


}
