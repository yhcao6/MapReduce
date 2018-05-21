import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileReader;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

import java.lang.Math;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.ClassNotFoundException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


class MyDedup {

    public static class InputArgs{
        String command;
        int minChunk;
        int avgChunk;
        int maxChunk;
        int d;
        String filePath;
        String storageType;

        public InputArgs(String command, int minChunk, int avgChunk, int maxChunk, int d, String filePath, String storageType){
            this.command = command;
            this.minChunk = minChunk;
            this.avgChunk = avgChunk;
            this.maxChunk = maxChunk;
            this.d = d;
            this.filePath = filePath;
            this.storageType = storageType;
        }

        public InputArgs(String command, String filePath, String storageType){
            this.command = command;
            this.filePath = filePath;
            this.storageType = storageType;
        }
    }

    public static void main(String args[]) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{
        // analysis input
        InputArgs inputArgs = MyDedup.parseParameters(args);

        Index index = new Index();
        index.read();  // read indexOfChunk

        if (inputArgs.command.equals("upload")){
            upload(inputArgs, index);
        } else if (inputArgs.command.equals("download")){
            download(inputArgs, index);
        } else if (inputArgs.command.equals("delete")){
            delete(inputArgs, index);
        }
    }

    public static void upload(InputArgs inputArgs, Index index) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{

        // config
        int m = 4;  // window size
        int d = inputArgs.d;  // base
        int q = 13;  // modulus
        int interest = 11;  // interest rfp

        boolean isFound = false;

        ByteBuffer buff = ByteBuffer.allocate(inputArgs.maxChunk);

        int currentChunkSize = 0;
        int currentOffset = 0;
        ArrayList<Integer> offsets = new ArrayList<Integer>(); // record anchor
        ArrayList<String> chunks = new ArrayList<String>();

        File file = new File(inputArgs.filePath);
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(inputArgs.filePath));

        int currentByte = 0;
        int lastByte = 0;
        int rfp = 0;
        long uniqueChunks = 0;
        long totalChunks = 0;
        long s1 = 0;
        long s2 = 0;
        int chunkSize = 0;

        while (true){
            currentByte = in.read();  // read next byte 

            if (currentByte == -1){
                if (currentChunkSize != 0){
                    s2 += chunkSize;
                    totalChunks += 1;

                    byte[] chunk = Arrays.copyOfRange(buff.array(), 0, currentOffset);
                    MessageDigest md = MessageDigest.getInstance("SHA-1");
                    md.update(chunk, 0, currentOffset);
                    byte[] checksum = md.digest();

                    // checksum String
                    String checksumStr = new BigInteger(1, checksum).toString();
                    chunks.add(checksumStr);

                    if (!index.hasChunk(checksumStr)){
                        //update statistic
                        s1 += chunkSize;
                        uniqueChunks += 1;

                        // save chunk
                        index.putNewChunk(checksumStr);
                        File outFile = new File("localStore/" + checksumStr);
                        FileChannel out = new FileOutputStream(outFile).getChannel();
                        buff.flip();
                        out.write(buff);
                        out.close();
                    } else {
                        index.putOldChunk(checksumStr);
                    }
                }
                break;  // end
            } else {
                buff.put((byte) currentByte);
                chunkSize += 1;
                // currentByte = currentByte - 48;
                currentChunkSize += 1;
                currentOffset += 1;
                if (currentChunkSize <= m){
                    rfp = rfp * (d % q) + currentByte % q;
                    rfp = rfp % q;
                } else {
                    lastByte = buff.get(currentChunkSize - m - 1);
                    // lastByte -= 48;
                    rfp = (rfp - ((int)Math.pow(d, m-1) % q) * (lastByte % q)) * (d % q) + currentByte % q; 
                    rfp = rfp % q;
                    if (rfp < 0){
                        rfp += q;
                    }
                }
            }

            if (currentChunkSize > inputArgs.minChunk){
                if (currentChunkSize == inputArgs.maxChunk || rfp == interest){
                    offsets.add(currentOffset);
                    isFound = true;
                }
            }

            if (isFound){
                //update s2
                s2 += chunkSize;
                totalChunks += 1;

                // get checksum
                byte[] chunk = Arrays.copyOfRange(buff.array(), 0, currentOffset);
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                md.update(chunk, 0, currentOffset);
                byte[] checksum = md.digest();

                // checksum String
                String checksumStr = new BigInteger(1, checksum).toString();
                chunks.add(checksumStr);

                if (!index.hasChunk(checksumStr)){
                    //update statistic
                    s1 += chunkSize;
                    uniqueChunks += 1;

                    // save chunk
                    index.putNewChunk(checksumStr);
                    File outFile = new File("localStore/" + checksumStr);
                    FileChannel out = new FileOutputStream(outFile).getChannel();
                    buff.flip();
                    out.write(buff);
                    out.close();
                } else {
                    index.putOldChunk(checksumStr);
                    // System.out.println("exist chunk " + checksumStr + " ref count is " + index.indexOfChunk.get(checksumStr));
                }

                // reset
                isFound = false;
                buff.clear();
                currentChunkSize = 0;
                rfp = 0;
                chunkSize = 0;
            }
        }
        index.totalChunks += totalChunks;
        index.uniqueChunks += uniqueChunks;
        index.s1 += s1;
        index.s2 += s2;
        index.indexOfFile.put(inputArgs.filePath, chunks);
        index.write();

        // statistic
        System.out.println("Report Output:");
        System.out.println("Total number of chunks in storage: " + index.totalChunks);
        System.out.println("Number of unique chunks in storage: " + index.uniqueChunks);
        System.out.println("Number of bytes in storage with deduplication: " + index.s1);
        System.out.println("Number of bytes in storage without deduplication " + index.s2);
        System.out.println("Space saving: " + (1 - (double)index.s1 / (double)index.s2));
    }

    public static void download(InputArgs inputArgs, Index index) throws IOException{
        if (!index.hasFile(inputArgs.filePath)){
            throw new FileNotFoundException("No such file");
        }

        // read corresponding chunks
        // write the single file
        FileOutputStream out = new FileOutputStream(inputArgs.filePath);
        for (String checksumStr: index.indexOfFile.get(inputArgs.filePath)){
            int b;
            FileInputStream in = new FileInputStream("localStore/" + checksumStr);
            while ((b = in.read()) != -1){
                out.write(b);
            }
            in.close();
        }
        out.close();
    }

    public static void delete(InputArgs inputArgs, Index index) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{
        if (!index.hasFile(inputArgs.filePath)){
            throw new FileNotFoundException("No such file");
        }

        File f = new File(inputArgs.filePath);
        for (String checksumStr: index.indexOfFile.get(inputArgs.filePath)){
            index.decrement(checksumStr);
        }
        index.write();
    }

    public static InputArgs parseParameters(String[] args){
        String command = args[0];
        if (command.equals("upload")){
            return new InputArgs(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]), Integer.valueOf(args[4]), args[5], args[6]);
        } else {
            return new InputArgs(args[0], args[1], args[2]);
        }
    }

    public static boolean indexExists(){
        File index = new File("mydedup.meta");
        return index.exists();
    }
}
