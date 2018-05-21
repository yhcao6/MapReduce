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

public class PageRank{

    enum myCounter{numNodes, numIterations, missMass};

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath;
        long maxIterate = Long.valueOf(args[2]);
        float alpha = Float.valueOf(args[3]);
        float theta = Float.valueOf(args[4]);

        // count num of nodes
        Configuration preProcessConf = new Configuration();
        preProcessConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator"," ");

        Job preProcessJob = Job.getInstance(preProcessConf, "count");
        preProcessJob.setJarByClass(PRPreProcess.class);
        preProcessJob.setInputFormatClass(KeyValueTextInputFormat.class);

        preProcessJob.setMapperClass(PRPreProcess.countMapper.class);
        preProcessJob.setMapOutputKeyClass(Text.class);
        preProcessJob.setMapOutputValueClass(Text.class);

        preProcessJob.setReducerClass(PRPreProcess.countReducer.class);
        preProcessJob.setOutputKeyClass(Text.class);
        preProcessJob.setOutputValueClass(Text.class);
        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(preProcessJob, inputPath);
        outputPath = new Path("/tmp-preprocess/");
        FileOutputFormat.setOutputPath(preProcessJob, outputPath);
        preProcessJob.waitForCompletion(true);

        long numNodes = preProcessJob.getCounters().findCounter(myCounter.numNodes).getValue();
        preProcessConf.setLong("numNodes", numNodes);

        // create graph
        preProcessJob = Job.getInstance(preProcessConf, "create graph");
        preProcessJob.setJarByClass(PRPreProcess.class);
        preProcessJob.setInputFormatClass(KeyValueTextInputFormat.class);

        preProcessJob.setMapperClass(PRPreProcess.PRPreProcessMapper.class);
        preProcessJob.setMapOutputKeyClass(Text.class);
        preProcessJob.setMapOutputValueClass(Text.class);

        preProcessJob.setReducerClass(PRPreProcess.PRPreProcessReducer.class);

        preProcessJob.setOutputKeyClass(LongWritable.class);
        preProcessJob.setOutputValueClass(PRNodeWritable.class);
        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        MultipleOutputs.addNamedOutput(preProcessJob, "outputText",
                TextOutputFormat.class, //output format class
                Text.class,//key format class
                Text.class//value format class
                );

        FileInputFormat.addInputPath(preProcessJob, inputPath);
        outputPath = new Path("/tmp-createGraph/");
        FileOutputFormat.setOutputPath(preProcessJob, outputPath);
        preProcessJob.waitForCompletion(true);

        //PageRank
        long i = 1;

        while (i <= maxIterate){

            Configuration prConf = new Configuration();
            prConf.set("mapreduce.output.textoutputformat.separator", " ");
            prConf.set("finalOutputPath", args[1]);
            prConf.set("mosOutputPath", "/mos"+i);

            Job prJob = Job.getInstance(prConf, "pagerank");
            prJob.setJarByClass(PageRank.class);
            prJob.setInputFormatClass(SequenceFileInputFormat.class);

            prJob.setMapperClass(PageRank.MyMapper.class);
            prJob.setMapOutputKeyClass(LongWritable.class);
            prJob.setMapOutputValueClass(PRNodeWritable.class);

            prJob.setReducerClass(PageRank.MyReducer.class);
            prJob.setOutputKeyClass(LongWritable.class);
            prJob.setOutputValueClass(PRNodeWritable.class);
            prJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            MultipleOutputs.addNamedOutput(prJob, "outputText",
                    TextOutputFormat.class, //output format class
                    LongWritable.class,//key format class
                    FloatWritable.class//value format class
                    );

            inputPath = outputPath;
            outputPath = new Path("/tmp-" + i + "-base/");
            FileInputFormat.addInputPath(prJob, inputPath);
            FileOutputFormat.setOutputPath(prJob, outputPath);
            prJob.waitForCompletion(true);

            // adjust 
            float missMass = Float.intBitsToFloat((int)prJob.getCounters().findCounter(myCounter.missMass).getValue());

            System.out.println("missMass is: " + missMass);

            prConf.setLong("numIterations", (long)i);

            prConf.setFloat("missMass", missMass);
            prConf.setLong("numNodes", numNodes);
            prConf.setFloat("alpha", alpha);
            prConf.setFloat("theta", theta);
            prConf.setLong("maxIterations", maxIterate);
            prConf.set("outputPath", args[1]);
            prJob = Job.getInstance(prConf, "adjust");
            prJob.setJarByClass(PRAdjust.class);
            prJob.setInputFormatClass(SequenceFileInputFormat.class);

            prJob.setMapperClass(PRAdjust.MyMapper.class);
            prJob.setMapOutputKeyClass(LongWritable.class);
            prJob.setMapOutputValueClass(PRNodeWritable.class);

            prJob.setReducerClass(PRAdjust.MyReducer.class);
            prJob.setOutputKeyClass(LongWritable.class);
            prJob.setOutputValueClass(PRNodeWritable.class);
            prJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            MultipleOutputs.addNamedOutput(prJob, "outputText", 
                    TextOutputFormat.class, //output format class
                    LongWritable.class,//key format class
                    FloatWritable.class//value format class
                    );

            inputPath = outputPath;
            outputPath = new Path( "/tmp-" + i);
            FileInputFormat.addInputPath(prJob, inputPath);
            FileOutputFormat.setOutputPath(prJob, outputPath);
            prJob.waitForCompletion(true);
            i++;
        }

        System.exit(0);


    }
    
    public static class MyMapper 
            extends Mapper<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
             
            private MultipleOutputs<LongWritable, FloatWritable> mos;

            public void setup(Context context)
                throws IOException, InterruptedException{
                mos = new MultipleOutputs(context);
            }

            public void map(LongWritable id, PRNodeWritable node, Context context)
                throws IOException, InterruptedException{

                // String mosPath = "/tmp/part";
                // mos.write("outputText", id, node.getMass(), mosPath);
                String mosOutputPath = context.getConfiguration().get("mosOutputPath"); 
                context.write(id, node);
                int numNeighbors = node.getAdjList().keySet().size();
                FloatWritable mass = new FloatWritable(node.getMass().get()/(float)numNeighbors);
                for (Map.Entry<Writable, Writable> edge: node.getAdjList().entrySet()){
                    LongWritable neighborId = (LongWritable)edge.getKey();
                    PRNodeWritable neighborNode = new PRNodeWritable(neighborId, mass);
                    neighborNode.setIsMass(new LongWritable(1L));
                    context.write(neighborId, neighborNode);
                }

                if (node.getAdjList().isEmpty()){
                    PRNodeWritable tmp = new PRNodeWritable();
                    tmp.setNodeId(new LongWritable(0l));
                    tmp.setMassByFloat(node.getMass().get());
                    context.write(new LongWritable(0l), tmp);
                }
            }
            
            public void cleanup(Context context)
                throws IOException, InterruptedException{
                mos.close();
            }
    }

    public static class MyReducer 
            extends Reducer<LongWritable,PRNodeWritable,LongWritable,PRNodeWritable> {
            private MultipleOutputs<LongWritable, FloatWritable> mos;

            public void setup(Context context)
                throws IOException, InterruptedException{
                mos = new MultipleOutputs(context);
            }

            public float missMass = 0f;

            public void reduce(LongWritable id, Iterable<PRNodeWritable> nodes, Context context)
                throws IOException, InterruptedException{

                String mosOutputPath = context.getConfiguration().get("mosOutputPath"); 

                PRNodeWritable M = new PRNodeWritable();
                M.setNodeId(id);
                float s = 0;

                if (id.get() == 0l){
                    for (PRNodeWritable node: nodes){
                        missMass += node.getMass().get();
                        // mos.write("outputText", node.getNodeId(), node.getMass(), mosOutputPath);
                    }
                } else {
                    for (PRNodeWritable node: nodes){
                        // mos.write("outputText", node.getNodeId(), node.getMass(), mosOutputPath);
                        if (!node.getAdjList().isEmpty()){
                            M.putAllAdjList(node.getAdjList());
                        } else {
                            if (node.getIsMass().get() == 1l){
                                s += node.getMass().get();
                            }
                        }
                    }
                    M.setMassByFloat(s);
                    context.write(id, M);
                }
            }

            public void cleanup(Context context)
                throws IOException, InterruptedException{
                Long longMissMass = new Long((long) Float.floatToIntBits(missMass));
                context.getCounter(myCounter.missMass).setValue(longMissMass);
                mos.close();
            }
    }
}
