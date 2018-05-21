public class InputArgs{
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
