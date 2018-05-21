import java.io.IOException;

import java.util.Map;
import java.util.UUID;

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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ParallelDijkstra{

    public enum NodeCounter {TOTAL,FOUND};


    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath;
        Path finalOutputPath = new Path(args[1]);
        long rootNode = Long.valueOf(args[2]);
        int maxIterate = Integer.valueOf(args[3]);

        //PreProcess
        Configuration preProcessConf = new Configuration();
        preProcessConf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator"," ");
        preProcessConf.setLong("ParallelDijkstra.root.node", rootNode);

        Job preProcessJob = Job.getInstance(preProcessConf, "preprocess");
        preProcessJob.setJarByClass(PDPreProcess.class);
        preProcessJob.setInputFormatClass(KeyValueTextInputFormat.class);

        preProcessJob.setMapperClass(PDPreProcess.MyMapper.class);
        preProcessJob.setMapOutputKeyClass(Text.class);
        preProcessJob.setMapOutputValueClass(Text.class);

        preProcessJob.setReducerClass(PDPreProcess.MyReducer.class);

        preProcessJob.setOutputKeyClass(LongWritable.class);
        preProcessJob.setOutputValueClass(PDNodeWritable.class);
        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(preProcessJob, inputPath);
        outputPath = new Path("/tmp/PreProcess/");
        FileOutputFormat.setOutputPath(preProcessJob, outputPath);
        preProcessJob.waitForCompletion(true);

        //paralellDijkstra
        long foundNodes = 1;
        int i = 1;
        long isFound = 1;

        // while ((i <= maxIterate && maxIterate > 0) || (isFound == 1L && maxIterate == 0)){
        while (i <= maxIterate  || maxIterate == 0){
            inputPath = outputPath;
            outputPath = new Path("/tmp/" + i + "/");

            Configuration pdConf = new Configuration();
            pdConf.set("mapreduce.output.textoutputformat.separator", " ");
            pdConf.set("finalOutputPath", args[1]);
            FileSystem hdfs = FileSystem.get(pdConf);

            Job pdJob = Job.getInstance(pdConf, "paralleldijkstra");
            pdJob.setJarByClass(ParallelDijkstra.class);
            pdJob.setInputFormatClass(SequenceFileInputFormat.class);

            pdJob.setMapperClass(ParallelDijkstra.MyMapper.class);
            pdJob.setMapOutputKeyClass(LongWritable.class);
            pdJob.setMapOutputValueClass(PDNodeWritable.class);

            pdJob.setReducerClass(ParallelDijkstra.MyReducer.class);

            pdJob.setOutputKeyClass(LongWritable.class);
            pdJob.setOutputValueClass(PDNodeWritable.class);
            pdJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            MultipleOutputs.addNamedOutput(pdJob, "outputText", 
                    TextOutputFormat.class, //output format class
                    LongWritable.class,//key format class
                    LongWritable.class//value format class
                    );

            FileInputFormat.addInputPath(pdJob, inputPath);
            FileOutputFormat.setOutputPath(pdJob, outputPath);
            pdJob.waitForCompletion(true);

            isFound = pdJob.getCounters().findCounter(NodeCounter.FOUND).getValue();

            if (isFound == 0L || i == maxIterate){
                System.exit(0);
            } else {
                hdfs.delete(finalOutputPath, true);
            }

            i++;

            pdJob.getCounters().findCounter(NodeCounter.FOUND).setValue(0L);
        }

        System.exit(0);
    }
    
    public static class MyMapper 
            extends Mapper<LongWritable,PDNodeWritable,LongWritable,PDNodeWritable> {
             
            public void map(LongWritable id, PDNodeWritable node, Context context)
                throws IOException, InterruptedException{
                if (node.getDistance().get() >= 0){ //-1 indicates inf distance
                    for (Map.Entry<Writable, Writable> edge : node.getAdjList().entrySet()){
                        if (((LongWritable) edge.getKey()).get() < 0){
                            continue;
                        }
                        PDNodeWritable neighbor = new PDNodeWritable();
                        neighbor.setNodeId((LongWritable) edge.getKey());
                        long edgeWeight = ((LongWritable) edge.getValue()).get();
                        long addedWeight = edgeWeight + node.getDistance().get();
                        neighbor.setDistanceByLong(addedWeight);
                        context.write(neighbor.getNodeId(), neighbor);
                    }
                }
                context.write(id, node);
            }
            
    }

    public static class MyReducer 
            extends Reducer<LongWritable,PDNodeWritable,LongWritable,PDNodeWritable> {
            private MultipleOutputs<LongWritable,LongWritable> mos;

            public void setup(Context context)
                throws IOException, InterruptedException{
                mos = new MultipleOutputs(context);
            }

            public void reduce(LongWritable id, Iterable<PDNodeWritable> nodes, Context context)
                throws IOException, InterruptedException{
                String mosPath = context.getConfiguration().get("finalOutputPath");
                PDNodeWritable currentNode = new PDNodeWritable();
                currentNode.setNodeId(id);
                boolean foundNewNode = false;
                long minDistance = -1;
                long currentDistance = -1;

                for (PDNodeWritable node : nodes){
                    if (!node.getAdjList().isEmpty()){
                        currentNode.putAllAdjList(node.getAdjList());
                        currentDistance = node.getDistance().get();
                        currentNode.setDistanceByLong(currentDistance);
                        if (node.getDistance().get() < 0){
                            foundNewNode = true;
                        }
                    } else {
                        long newDistance = node.getDistance().get();
                        if (newDistance >= 0 && (minDistance < 0 || newDistance < minDistance)){
                            minDistance = newDistance;
                        }
                    }
                }
                if (minDistance >= 0){
                    if (currentDistance < 0 || minDistance < currentDistance){
                        currentNode.setDistanceByLong(minDistance);
                        context.getCounter(NodeCounter.FOUND).setValue(1L);
                    }
                }
                if (currentNode.getAdjList().isEmpty()){
                    currentNode.putAdjList(new LongWritable(-1L), new LongWritable(-1L));
                }
                context.write(id,currentNode);

                if (currentNode.getDistance().get() >= 0){
                    mos.write("outputText", id, currentNode.getDistance().get(), mosPath);
                }
            }

            public void cleanup(Context context)
                throws IOException, InterruptedException{
                mos.close();
            }
    }

}
