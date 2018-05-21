import java.io.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class AzureAPIDemo {
  
  public static final String storageConnectionString ="DefaultEndpointsProtocol=https;AccountName=group29;AccountKey=ShC/Ckc5r8nwiL6bOU+JQg2sOrCnpCJbNJH6PFe39ytZsSqdoa5hOY4JRuom+PREf3/mgjWor+1wQi9mKRPzkA==;EndpointSuffix=core.windows.net";
  
  static {
    System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
    System.setProperty("https.proxyPort", "8000");
    System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
    System.setProperty("http.proxyPort", "8000");
  }

  public static void usage() {
    System.out.println("Usage: ");
    System.out.println("    java -cp .:./lib/* AzureAPIDemo upload [local_file_name] [remote_file_name]");
    System.out.println("    java -cp .:./lib/* AzureAPIDemo download [remote_file_name] [local_file_name]");
    System.out.println("    java -cp .:./lib/* AzureAPIDemo delete [file_name]");
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      usage();
      System.exit(1);
    }

    String operation = args[0];

    try {
      // Retrieve storage account from connection-string.
      CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
  
      // Create the blob client.
      CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
  
      // Retrieve reference to a previously created container.
      CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
  
      // Create the container if it does not exist.
      container.createIfNotExists();

      if (operation.equals("upload")) {
        if (args.length < 3) {
	  usage();
	  System.exit(1);
	}

	String localFileName = args[1];
	String remoteFileName = args[2];

        // Create or overwrite the remoteFileName blob with contents from a local file.
        CloudBlockBlob blob = container.getBlockBlobReference(remoteFileName);
        File source = new File(localFileName);
        blob.upload(new FileInputStream(source), source.length());
      } else if (operation.equals("download")) {
        if (args.length < 3) {
	  usage();
	  System.exit(1);
	}

	String remoteFileName = args[1];
	String localFileName = args[2];

	// Retrieve reference to a blob named "remoteFileName".
	CloudBlockBlob blob = container.getBlockBlobReference(remoteFileName);
	blob.download(new FileOutputStream(localFileName));
      } else if (operation.equals("delete")) {
        if (args.length < 2) {
	  usage();
	  System.exit(1);
	}

	String fileName = args[1];
	// Retrieve reference to a blob named "fileName".
	CloudBlockBlob blob = container.getBlockBlobReference(fileName);

	// Delete the blob.
	blob.deleteIfExists();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
