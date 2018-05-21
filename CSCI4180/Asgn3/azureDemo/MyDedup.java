import java.io.IOException;
import java.lang.ClassNotFoundException;
import java.security.NoSuchAlgorithmException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;


class MyDedup {

    public static void main(String args[]) throws IOException, ClassNotFoundException, NoSuchAlgorithmException, URISyntaxException, InvalidKeyException{
        // analysis input
        InputArgs inputArgs = MyDedup.parseParameters(args);

        if (inputArgs.storageType.equals("local")){
            LocalStorage localStore = new LocalStorage();
            if (inputArgs.command.equals("upload")){
                localStore.upload(inputArgs);
            } else if (inputArgs.command.equals("download")){
                localStore.download(inputArgs);
            } else if (inputArgs.command.equals("delete")){
                localStore.delete(inputArgs);
            }
        }

        if (inputArgs.storageType.equals("azure")){
            AzureStorage azureStorage = new AzureStorage();
            if (inputArgs.command.equals("upload")){
                azureStorage.upload(inputArgs);
            } else if (inputArgs.command.equals("download")){
                azureStorage.download(inputArgs);
            } else if (inputArgs.command.equals("delete")){
                azureStorage.delete(inputArgs);
            }
        }

        /*
        if (inputArgs.storageType == "local"){
            LocalStorage localStore = new LocalStorage();
            if (inputArgs.command.equals("upload")){
                localStore.upload(inputArgs, index);
            } else if (inputArgs.command.equals("download")){
                localStore.download(inputArgs, index);
            } else if (inputArgs.command.equals("delete")){
                localStore.delete(inputArgs, index);
            }
        }
        */
    }

    public static InputArgs parseParameters(String[] args){
        String command = args[0];
        if (command.equals("upload")){
            return new InputArgs(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]), Integer.valueOf(args[4]), args[5], args[6]);
        } else {
            return new InputArgs(args[0], args[1], args[2]);
        }
    }
}
