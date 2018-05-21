import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.LinkedList;
import java.lang.Math;
import java.util.Map;
import java.util.HashMap;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.lang.ClassNotFoundException;


class test {
    public static void main(String args[]) throws IOException, ClassNotFoundException{
        FileInputStream in = new FileInputStream("./1.png");
        FileOutputStream out = new FileOutputStream("./2.png");
        int b;
        while ((b = in.read()) != -1){
            out.write(b); 
        }
        out.close();
        in.close();
    }
}

