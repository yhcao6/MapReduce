import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Map;

public class PRNodeWritable implements Writable{
    private LongWritable nodeId;
    private FloatWritable mass;
    private MapWritable adjList;
    private LongWritable isMass;

    public PRNodeWritable(){
        this.nodeId = new LongWritable();
        this.mass = new FloatWritable(-1f); //initially -1 => inf mass
        this.adjList = new MapWritable();
        this.isMass = new LongWritable(0l);
    }

    public PRNodeWritable(LongWritable nodeId, FloatWritable mass){
        this.nodeId = nodeId;
        this.mass = mass;
        this.adjList = new MapWritable();
        this.isMass = new LongWritable(0l);
    }

    public LongWritable getNodeId(){
        return this.nodeId;
    }

    public FloatWritable getMass(){
        return this.mass;
    }

    public LongWritable getIsMass(){
        return this.isMass;
    }

    public MapWritable getAdjList(){
        return this.adjList;
    }

    public void setNodeId(LongWritable nodeId){
        this.nodeId = nodeId;
    }

    public void setMass(FloatWritable mass){
        this.mass = mass;
    }

    public void setMassByFloat(float mass){
        this.mass.set(mass);
    }

    public void putAdjList(LongWritable key, LongWritable value){
        this.adjList.put(key, value);
    }

    public void putAllAdjList(MapWritable mapW){
        this.adjList.putAll(mapW);
    }

    public void setIsMass(LongWritable isMass){
        this.isMass = isMass;
    }

    public void write(DataOutput dataOutput) throws IOException {
        nodeId.write(dataOutput); 
        mass.write(dataOutput);
        adjList.write(dataOutput);
        isMass.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        nodeId.readFields(dataInput);
        mass.readFields(dataInput);
        adjList.readFields(dataInput);
        isMass.readFields(dataInput);
    }

    public String toString(){
        String res = "";
        res += nodeId.get();
        res += " ";
        res += mass.get();
        res += " ";
        for (Writable k: adjList.keySet()){
            res += ((LongWritable) k).get();
            res += " ";
        }
        return res;
    }
}
