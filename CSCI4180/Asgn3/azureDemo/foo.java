import java.io.*;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class foo {
    public static final String storageConnectionString ="DefaultEndpointsProtocol=https;AccountName=group29;AccountKey=ShC/Ckc5r8nwiL6bOU+JQg2sOrCnpCJbNJH6PFe39ytZsSqdoa5hOY4JRuom+PREf3/mgjWor+1wQi9mKRPzkA==;EndpointSuffix=core.windows.net";

    public static CloudStorageAccount storageAccount;

    public static void main(String args[]) {
        try{
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
