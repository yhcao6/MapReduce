import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.nio.file.Paths;
import java.util.*;

import java.io.*;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

import java.math.BigInteger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.Arrays;
import java.util.ArrayList;

import java.lang.ClassNotFoundException;


import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class S3Storage {

    static {
        System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("https.proxyPort", "8000");
        System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("http.proxyPort", "8000");
    }

    public Index index;
    public AmazonS3 s3;
    public Bucket b;
    public String bucket_name;

    public S3Storage() throws IOException, ClassNotFoundException, URISyntaxException, InvalidKeyException{
        try{
            s3 = AmazonS3ClientBuilder.defaultClient();
            bucket_name = "csci4180group29";
            b = s3.createBucket(bucket_name);

            boolean exists = s3.doesObjectExist(bucket_name, "mydedup.index");
            if (exists){
			    S3Object o = s3.getObject(bucket_name, "mydedup.index");
                S3ObjectInputStream s3is = o.getObjectContent();
                FileOutputStream fos = new FileOutputStream(new File("mydedup.index"));
                byte[] read_buf = new byte[1024];
                int read_len = 0;
                while ((read_len = s3is.read(read_buf)) > 0) {
                    fos.write(read_buf, 0, read_len);
                }
                s3is.close();
                fos.close();
                index = new Index("mydedup.index");
            } else {
                index = new Index();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void upload(InputArgs inputArgs) throws IOException, NoSuchAlgorithmException, ClassNotFoundException, URISyntaxException{

        try{
            // config
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

                        byte[] chunk = Arrays.copyOfRange(buff.array(), 0, currentChunkSize);
                        MessageDigest md = MessageDigest.getInstance("SHA-1");
                        md.update(chunk, 0, currentChunkSize);
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
                            buff.flip();

                            ObjectMetadata metadata = new ObjectMetadata();
                            metadata.setContentLength(chunk.length);
                            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket_name, checksumStr, new ByteArrayInputStream(chunk), metadata);
                            s3.putObject(putObjectRequest);
                            //CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
                            //blob.upload(new ByteArrayInputStream(chunk), chunk.length);
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
                    byte[] chunk = Arrays.copyOfRange(buff.array(), 0, currentChunkSize);
                    MessageDigest md = MessageDigest.getInstance("SHA-1");
                    md.update(chunk, 0, currentChunkSize);
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
                        buff.flip();

                        ObjectMetadata metadata = new ObjectMetadata();
                        metadata.setContentLength(chunk.length);
                        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket_name, checksumStr, new ByteArrayInputStream(chunk), metadata);
                        s3.putObject(putObjectRequest);
                        
                        //CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
                        //blob.upload(new ByteArrayInputStream(chunk), chunk.length);
                    } else {
                        index.putOldChunk(checksumStr);
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
            index.write("mydedup.index");
            File f = new File("mydedup.index");

            s3.putObject(bucket_name, "mydedup.index", new File("mydedup.index"));
            // CloudBlockBlob blob = container.getBlockBlobReference("mydedup.index");
            // blob.upload(new FileInputStream(f), f.length());

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
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void download(InputArgs inputArgs) throws IOException{
        try{
            // read corresponding chunks
            // write the single file

            FileOutputStream out = new FileOutputStream(inputArgs.filePath + ".download");
            for (String checksumStr: index.indexOfFile.get(inputArgs.filePath)){
			    S3Object o = s3.getObject(bucket_name, checksumStr);
                S3ObjectInputStream s3is = o.getObjectContent();
                byte[] read_buf = new byte[1024];
                int read_len = 0;
                while ((read_len = s3is.read(read_buf)) > 0) {
                    out.write(read_buf, 0, read_len);
                }
                s3is.close();
                // CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
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
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void delete(InputArgs inputArgs) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{
        try{
            for (String checksumStr: index.indexOfFile.get(inputArgs.filePath)){
                long chunkSize = s3.getObjectMetadata(bucket_name, checksumStr).getContentLength();
                index.decrement(checksumStr, chunkSize);
                if (!index.indexOfChunk.containsKey(checksumStr)){
                    s3.deleteObject(bucket_name, checksumStr);
                    // CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
                    // blob.deleteIfExists();
                }
            }
            if (index.uniqueChunks == 0){
                s3.deleteObject(bucket_name, "mydedup.index");
            } else {
                index.write("mydedup.index");
                File f = new File("mydedup.index");
                s3.putObject(bucket_name, "mydedup.index", new File("mydedup.index"));
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
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
