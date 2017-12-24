import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;
import com.opencsv.CSVReader;
import org.bson.Document;

import java.io.*;
import java.util.*;

public class Test {
    public static void main(String[] args) throws IOException{
        /*try{
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("songs");
            System.out.println("Connect to database successfully");
            MongoCollection collection = mongoDatabase.getCollection("lyrics");
            Document result = (Document)collection.find(new Document("index", "0")).iterator().next();
            System.out.println(result.get("index"));
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }*/
        /*FileReader fr = new FileReader("/home/jason/Documents/Course/Web Search Engine/Project/data/test");
        CSVReader cr  = new CSVReader(fr);
        System.out.println(Arrays.toString(cr.readNext()));
        System.out.println(Arrays.toString(cr.readNext()));
        System.out.println(Arrays.toString(cr.readNext()));*/

        /*long start = 53071915, end = 53072426;
        RandomAccessFile raf = new RandomAccessFile("./result/InvertedList","r");
        VariableByteCode vByte = new VariableByteCode();
        raf.seek(start);
        byte[] result = new byte[(int)(end - start + 1)];
        raf.read(result);
        Queue<int[]> decode = vByte.decodeChunk(result, 275);
        for (int[] post : decode)
            System.out.println(Arrays.toString(post));*/

        List<Integer> list = new ArrayList<>(1);
        list.add(1);
        Integer[] test = new Integer[1];
        list.toArray(test);
        System.out.println(Arrays.toString(test));

        /*VariableByteCode vByte = new VariableByteCode();
        byte[] encode = vByte.encodeNumber(-2);
        System.out.println(vByte.decode(encode));*/
    }
}
