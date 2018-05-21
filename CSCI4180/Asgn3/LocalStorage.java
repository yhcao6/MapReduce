import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

import java.math.BigInteger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.Arrays;
import java.util.ArrayList;

import java.lang.ClassNotFoundException;


public class LocalStorage {
    public Index index;
    public String directory = "local/";

    public LocalStorage() throws IOException, ClassNotFoundException{
        File f = new File(this.directory);
        if (!f.exists()){
            f.mkdir();
        }
        index = new Index(directory + "mydedup.index");
    }

    public void upload(InputArgs inputArgs) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{

        int m = inputArgs.minChunk;  // window size
        int d = inputArgs.d;  // base
        int q = inputArgs.avgChunk;  // modulus
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

                    byte[] chunk = Arrays.copyOfRange(buff.array(), 0, chunkSize);
                    MessageDigest md = MessageDigest.getInstance("SHA-1");
                    md.update(chunk, 0, chunkSize);
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
                        File outFile = new File("local/" + checksumStr);
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

            if (currentChunkSize == inputArgs.maxChunk || rfp == interest){
                offsets.add(currentOffset);
                isFound = true;
            }

            if (isFound){
                //update s2
                s2 += chunkSize;
                totalChunks += 1;

                // get checksum
                byte[] chunk = Arrays.copyOfRange(buff.array(), 0, chunkSize);
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                md.update(chunk, 0, chunkSize);
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
                    File outFile = new File("local/" + checksumStr);
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
        index.write(directory + "mydedup.index");

        // statistic
        System.out.println("Report Output:");
        System.out.println("Total number of chunks in storage: " + index.totalChunks);
        System.out.println("Number of unique chunks in storage: " + index.uniqueChunks);
        System.out.println("Number of bytes in storage with deduplication: " + index.s1);
        System.out.println("Number of bytes in storage without deduplication " + index.s2);
        if (index.s1 == index.s2){
            System.out.println("Space saving: " + 0.0);
        } else {
            System.out.println("Space saving: " + (1 - (double)index.s1 / (double)index.s2));
        }
    }

    public void download(InputArgs inputArgs) throws IOException{
        if (!index.hasFile(inputArgs.filePath)){
            throw new FileNotFoundException("No such file");
        }

        // read corresponding chunks
        // write the single file
        FileOutputStream out = new FileOutputStream(inputArgs.filePath + ".download");
        for (String checksumStr: index.indexOfFile.get(inputArgs.filePath)){
            int b;
            FileInputStream in = new FileInputStream("local/" + checksumStr);
            while ((b = in.read()) != -1){
                out.write(b);
            }
            in.close();
        }
        out.close();
        System.out.println("Report Output:");
        System.out.println("Total number of chunks in storage: " + index.totalChunks);
        System.out.println("Number of unique chunks in storage: " + index.uniqueChunks);
        System.out.println("Number of bytes in storage with deduplication: " + index.s1);
        System.out.println("Number of bytes in storage without deduplication " + index.s2);
        if (index.s1 == index.s2){
            System.out.println("Space saving: " + 0.0);
        } else {
            System.out.println("Space saving: " + (1 - (double)index.s1 / (double)index.s2));
        }
    }

    public void delete(InputArgs inputArgs) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{
        System.out.println(inputArgs.filePath);
        if (!index.hasFile(inputArgs.filePath)){
            throw new FileNotFoundException("No such file");
        }

        for (String checksumStr: index.indexOfFile.get(inputArgs.filePath)){
            File f = new File("local/" + checksumStr);
            long len = f.length();
            if (index.indexOfChunk.get(checksumStr) == 1){
                f.delete();
            }
            index.decrement(checksumStr, len);
        }

        if (index.uniqueChunks == 0)
        {
            File f = new File(directory + "mydedup.index");
            f.delete();
        } else {
            index.write(directory + "mydedup.index");
        }

        System.out.println("Report Output:");
        System.out.println("Total number of chunks in storage: " + index.totalChunks);
        System.out.println("Number of unique chunks in storage: " + index.uniqueChunks);
        System.out.println("Number of bytes in storage with deduplication: " + index.s1);
        System.out.println("Number of bytes in storage without deduplication " + index.s2);
        if (index.s1 == index.s2){
            System.out.println("Space saving: " + 0.0);
        } else {
            System.out.println("Space saving: " + (1 - (double)index.s1 / (double)index.s2));
        }
    }
}
