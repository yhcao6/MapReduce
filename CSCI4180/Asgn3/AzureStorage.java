import java.io.*;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

import java.math.BigInteger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.Arrays;
import java.util.ArrayList;

import java.lang.ClassNotFoundException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureStorage {
    public static final String storageConnectionString ="DefaultEndpointsProtocol=https;AccountName=group29;AccountKey=ShC/Ckc5r8nwiL6bOU+JQg2sOrCnpCJbNJH6PFe39ytZsSqdoa5hOY4JRuom+PREf3/mgjWor+1wQi9mKRPzkA==;EndpointSuffix=core.windows.net";

    static {
        System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("https.proxyPort", "8000");
        System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("http.proxyPort", "8000");
    }

    public Index index;
    public CloudStorageAccount storageAccount;
    public CloudBlobClient blobClient;
    public CloudBlobContainer container;

    public AzureStorage() throws IOException, ClassNotFoundException, URISyntaxException, InvalidKeyException{
        try{
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
            blobClient = storageAccount.createCloudBlobClient();
            container = blobClient.getContainerReference("test");
            container.createIfNotExists();

            CloudBlockBlob blob = container.getBlockBlobReference("mydedup.index");
            if (blob.exists()){
                blob.download(new FileOutputStream("mydedup.index"));
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
                            CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
                            blob.upload(new ByteArrayInputStream(chunk), chunk.length);
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
                        CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
                        blob.upload(new ByteArrayInputStream(chunk), chunk.length);
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
            CloudBlockBlob blob = container.getBlockBlobReference("mydedup.index");
            blob.upload(new FileInputStream(f), f.length());

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
                CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                blob.download(stream);
                stream.writeTo(out);
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
                CloudBlockBlob blob = container.getBlockBlobReference(checksumStr);
                blob.downloadAttributes();
                long chunkSize = blob.getProperties().getLength();
                index.decrement(checksumStr, chunkSize);
                if (!index.indexOfChunk.containsKey(checksumStr)){
                    blob.deleteIfExists();
                }
            }
            CloudBlockBlob blob = container.getBlockBlobReference("mydedup.index");
            if (index.uniqueChunks == 0){
                blob.deleteIfExists();
            } else {
                index.write("mydedup.index");
                File f = new File("mydedup.index");
                blob.upload(new FileInputStream(f), f.length());
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
