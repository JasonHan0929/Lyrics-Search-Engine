import java.io.*;
import java.util.*;
import com.opencsv.CSVReader;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class IndexBuilder {
    private final HashMap<String, Integer> map = new HashMap<>();
    private final int BUFFER_SIZE = 1024 * 1024;
    private MongoClient connection;
    private final VariableByteCode vByte = new VariableByteCode();
    private final int CHUNK_SIZE = 256;

    private void connetToDB(String localhost, int port) {
        try{
            connection = new MongoClient(localhost, port);
            System.out.println("Connect to MongoDB successfully");

        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    private void parseData(String fileName) throws IOException {
        try (FileReader dataFile = new FileReader(fileName);
             FileWriter postFile = new FileWriter("./result/Posts")) {
            BufferedReader dataBuffered = new BufferedReader(dataFile, BUFFER_SIZE);
            CSVReader data = new CSVReader(dataBuffered);
            BufferedWriter post = new BufferedWriter(postFile);
            MongoDatabase database = connection.getDatabase("songs");
            MongoCollection<Document> collection = database.getCollection("lyrics");
            data.readNext(); // the first line of csv file is header
            List<String[]> lyrics = data.readAll();
            for (String[] curr : lyrics) {
                Document document = new Document();
                String[] fields = {"index", "song", "year", "artist", "genre", "lyrics", "word_count"};
                boolean[] intFields = {true, false, true, false, false, false, true};
                for (int i = 0; i < fields.length; i++) {
                    if (intFields[i]) document.append(fields[i], Double.valueOf(curr[i]).intValue());
                    else document.append(fields[i], curr[i]);
                }
                collection.insertOne(document);
                makePost(curr, post);
            }
            post.flush();
            postFile.flush();
        }
    }

    private String parseContent(String raw) {
        raw = raw.replaceAll("[^A-Za-z0-9']", " ");
        raw = raw.replaceAll("\\s+", " ");
        return raw.toLowerCase().trim();
    }

    private String formatWord(String word) {
        word = word.trim();
        if (word.length() == 0 || word.equals("'")) return "";
        if (word.charAt(0) == '\'') word = word.substring(1, word.length());
        if (word.charAt(word.length() - 1) == '\'') word = word.substring(0, word.length() - 1);
        return word;
    }

    private void makePost(String[] lyrics, BufferedWriter post) throws IOException{
        map.clear();
        String content = parseContent(lyrics[5]);
        String[] words = content.split("\\s");
        for (String word : words) {
            word = formatWord(word);
            if (word.length() > 0) map.put(word, map.getOrDefault(word, 0) + 1);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            post.write(String.format("%s:%d:%d\n", entry.getKey(), Integer.valueOf(lyrics[0]), entry.getValue()));
        }
    }

    private void buildIndex(String dataFile) throws IOException, InterruptedException {
        connetToDB("localhost", 27017);
        System.out.println("Parsing the lyrics data...");
        parseData(dataFile);
        System.out.println("Sorting current posts...");
        Process process = new ProcessBuilder("/bin/bash", "-c", "sort -t ':' -k 1,1 -k 2n,2 ./result/Posts -o ./result/Posts").start();
        process.waitFor();
        System.out.println("Building Lexicon and InvertedList...");
        makeLexicon("./result/Posts");
        System.out.println("All done!");
    }

    private int writeVByte(BufferedOutputStream invertList, String[] pair, int lastIndex) throws IOException {
        int index = Integer.valueOf(pair[1]);
        int fre = Integer.valueOf(pair[2]);
        byte[] indexByte = vByte.encodeNumber(index - lastIndex);
        byte[] freByte = vByte.encodeNumber(fre);
        invertList.write(indexByte);
        invertList.write(freByte);
        return indexByte.length + freByte.length;
    }

    private void makeLexicon(String postFileName) throws IOException {
        try (FileOutputStream intertedListFile = new FileOutputStream("./result/InvertedList");
             FileWriter lexiconFile = new FileWriter("./result/Lexicon");
             FileWriter chunkFile = new FileWriter("./result/Chunk");
             FileReader postFile = new FileReader(postFileName)) {
            LexiconState formerLexicon = new LexiconState(null, 0, 0, 0);
            ChunkState formerChunk = new ChunkState(0, 0, 0, 0, 0);
            BufferedOutputStream invertedList = new BufferedOutputStream(intertedListFile, BUFFER_SIZE);
            BufferedWriter lexicon = new BufferedWriter(lexiconFile, BUFFER_SIZE);
            BufferedWriter chunk = new BufferedWriter(chunkFile, BUFFER_SIZE);
            BufferedReader post = new BufferedReader(postFile, BUFFER_SIZE);
            long formerBytes = -1; // not 0
            int lastIndex = -1; // to deal with vByte decoder could not decode 0
            while (post.ready()) {
                String currLine = post.readLine();
                String[] pair = currLine.split(":");
                int currIndex = Integer.valueOf(pair[1]);
                if (!pair[0].equals(formerLexicon.term)) {
                    if (formerLexicon.term != null) {
                        lexicon.write(String.format("%s,%d,%d,%d\n", formerLexicon.term, formerLexicon.countLyrics, formerLexicon.startChunk, formerLexicon.endChunk));
                        formerLexicon.term = pair[0];
                        formerLexicon.countLyrics = 1;
                        formerLexicon.startChunk = formerChunk.chunkNum + 1;
                        formerLexicon.endChunk = formerLexicon.startChunk;
                        lastIndex = -1;
                        /*chunk.write(String.format("%d,%d,%d,%d\n", formerChunk.chunkNum, formerChunk.lastIndex, formerChunk.startBytes, formerChunk.endBytes));
                        formerChunk.chunkNum += 1;
                        formerChunk.lastIndex = Integer.valueOf(pair[1]);
                        formerChunk.chunkSize = 1;
                        formerChunk.startBytes = formerChunk.endBytes + 1;
                        formerChunk.endBytes = currBytes;*/
                        formerChunk.chunkSize = CHUNK_SIZE;// update chunk later
                    } else {
                        formerLexicon.term = pair[0];
                        formerLexicon.countLyrics = 1;
                    }
                } else {
                    formerLexicon.endChunk = formerChunk.chunkNum;
                    formerLexicon.countLyrics += 1;
                }
                long currBytes = formerBytes + writeVByte(invertedList, pair, lastIndex);
                lastIndex = currIndex;
                if (formerChunk.chunkSize >= CHUNK_SIZE) {
                    chunk.write(String.format("%d,%d,%d,%d\n", formerChunk.chunkNum, formerChunk.lastIndex, formerChunk.startBytes, formerChunk.endBytes));
                    formerChunk.chunkNum += 1;
                    formerChunk.lastIndex = currIndex;
                    formerChunk.chunkSize = 1;
                    formerChunk.startBytes = formerChunk.endBytes + 1;
                    formerChunk.endBytes = currBytes;
                } else {
                    formerChunk.lastIndex = currIndex;
                    formerChunk.chunkSize += 1;
                    formerChunk.endBytes = currBytes;
                }
                formerBytes = currBytes;
            }
            lexicon.write(String.format("%s,%d,%d,%d\n", formerLexicon.term, formerLexicon.countLyrics, formerLexicon.startChunk, formerLexicon.endChunk));
            chunk.write(String.format("%d,%d,%d,%d\n", formerChunk.chunkNum, formerChunk.lastIndex, formerChunk.startBytes, formerChunk.endBytes));
            lexicon.flush();
            invertedList.flush();
            chunk.flush();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String fileName = args.length == 0 ? "../data/lyrics_clean.csv" : args[0];
        IndexBuilder builder = new IndexBuilder();
        long startTime = new Date().getTime();
        builder.buildIndex(fileName);
        long endTime = new Date().getTime();
        builder.connection.close();
        System.out.println(Calculator.timeCalculator(endTime - startTime));
        System.out.println(Calculator.sizeCalculator("InvertedLists", "Lexicon", "Chunk"));
    }
}
