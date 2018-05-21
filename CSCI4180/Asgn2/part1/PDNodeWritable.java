import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Map;

public class PDNodeWritable implements Writable{
    private LongWritable nodeId;
    private LongWritable distance;
    private MapWritable adjList;

    public PDNodeWritable(){
        this.nodeId = new LongWritable();
        this.distance = new LongWritable(-1); 
        this.adjList = new MapWritable();
    }

    public PDNodeWritable(LongWritable nodeId, LongWritable distance){
        this.nodeId = nodeId;
        this.distance = distance;
    }

    public LongWritable getNodeId(){
        return this.nodeId;
    }

    public LongWritable getDistance(){
        return this.distance;
    }

    public MapWritable getAdjList(){
        return this.adjList;
    }

    public void setNodeId(LongWritable nodeId){
        this.nodeId = nodeId;
    }

    public void setDistance(LongWritable distance){
        this.distance = distance;
    }

    public void setDistanceByLong(long dist){
        this.distance.set(dist);
    }

    public void putAdjList(LongWritable key, LongWritable value){
        this.adjList.put(key, value);
    }

    public void putAllAdjList(MapWritable mapW){
        this.adjList.putAll(mapW);
    }

    public void write(DataOutput dataOutput) throws IOException {
        nodeId.write(dataOutput); 
        distance.write(dataOutput);
        adjList.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        nodeId.readFields(dataInput);
        distance.readFields(dataInput);
        adjList.readFields(dataInput);
    }
}
