import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.io.IOException;
import java.lang.ClassNotFoundException;


public class Index{
    Map<String, Integer> indexOfChunk;
    Map<String, ArrayList<String>> indexOfFile;
    long totalChunks;
    long uniqueChunks;
    long s1;
    long s2;
    double saveSpace;

    public Index(){
        this.indexOfChunk = new HashMap<String, Integer>(); 
        this.indexOfFile = new HashMap<String, ArrayList<String>>();
        this.totalChunks = 0;
        this.uniqueChunks = 0;
        this.s1 = 0;
        this.s2 = 0;
        this.saveSpace = (double)0;
    }

    public Index(String indexPath) throws IOException, ClassNotFoundException{
        this.indexOfChunk = new HashMap<String, Integer>(); 
        this.indexOfFile = new HashMap<String, ArrayList<String>>();
        this.totalChunks = 0;
        this.uniqueChunks = 0;
        this.s1 = 0;
        this.s2 = 0;
        this.saveSpace = (double)0;
        read(indexPath);
    }

    public void putNewChunk(String checksumStr){
        this.indexOfChunk.put(checksumStr, 1);
    }

    public void putOldChunk(String checksumStr){
        this.indexOfChunk.put(checksumStr, this.indexOfChunk.get(checksumStr) + 1);
    }

    public boolean hasChunk(String checksumStr){
        return this.indexOfChunk.containsKey(checksumStr);
    }

    public boolean hasFile(String path){
        return this.indexOfFile.containsKey(path);
    }

    public void increment(String checksumStr){
        int currentRef = this.indexOfChunk.get(checksumStr);
        currentRef += 1;
        this.indexOfChunk.put(checksumStr, currentRef);
    }

    public void decrement(String checksumStr, long chunkSize){
        int currentRef = this.indexOfChunk.get(checksumStr);
        currentRef -= 1;
        this.indexOfChunk.put(checksumStr, currentRef);

        this.s2 -= chunkSize;  // update s2
        this.totalChunks -= 1;  // update totalChunks

        if (currentRef == 0){
            this.s1 -= chunkSize;  // update s1
            this.uniqueChunks -= 1;  // update uniqueChunks
            this.indexOfChunk.remove(checksumStr);
        }
    }

    public void read(String path) throws IOException, ClassNotFoundException{
        File f = new File(path); 
        if (f.exists()){
            ObjectInputStream in = new ObjectInputStream(new FileInputStream(path));
            indexOfChunk = (HashMap<String, Integer>) in.readObject();
            indexOfFile = (HashMap<String, ArrayList<String>>) in.readObject();
            totalChunks = (Long) in.readObject();
            uniqueChunks = (Long) in.readObject();
            s1 = (Long) in.readObject();
            s2 = (Long) in.readObject();
            saveSpace = (Double) in.readObject();
            in.close();
        } else {
            f.createNewFile();
        }
    } 

    public void write(String path) throws IOException, ClassNotFoundException{
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path));
        out.writeObject(this.indexOfChunk);
        out.writeObject(this.indexOfFile);
        out.writeObject(this.totalChunks);
        out.writeObject(this.uniqueChunks);
        out.writeObject(this.s1);
        out.writeObject(this.s2);
        out.writeObject(this.saveSpace);
        out.close();
    }
}
