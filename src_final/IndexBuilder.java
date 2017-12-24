import java.io.*;
import java.nio.file.*;
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
    private int id = 0;
    private final Set<String> englishWords = new HashSet<>();

    private void connetToDB(String localhost, int port) {
        try {
            connection = new MongoClient(localhost, port);
            System.out.println("Connect to MongoDB successfully");
        } catch(Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private void buildEnglishWords(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        byte[] readBytes = Files.readAllBytes(path);
        String wordListContents = new String(readBytes, "UTF-8");
        String wordsParsed = parseContent(wordListContents);
        String[] words = wordsParsed.split("\\s");
        for (String word : words) {
            word = formatWord(word);
            if (word.length() > 0)
            englishWords.add(word);
        }
    }

    private void parseData(String fileName) throws IOException {
        try (FileReader dataFile = new FileReader(fileName);
             FileWriter twoWordPostFile = new FileWriter("./result/TwoWordPosts");
             FileWriter oneWordPostFile = new FileWriter("./result/OneWordPosts");
             BufferedReader dataBuffered = new BufferedReader(dataFile, BUFFER_SIZE);
             BufferedWriter oneWordPost = new BufferedWriter(oneWordPostFile);
             BufferedWriter twoWordPost = new BufferedWriter(twoWordPostFile)) {
            CSVReader data = new CSVReader(dataBuffered);
            MongoDatabase database = connection.getDatabase("songs");
            MongoCollection<Document> collection = database.getCollection("lyrics");
            data.readNext(); // the first line of csv file is header
            List<String[]> lyrics = data.readAll();
            for (String[] curr : lyrics) {
                Document document = new Document();
                String[] fields = {"index", "song", "year", "artist", "genre", "lyrics", "word_count"};
                boolean[] intFields = {true, false, true, false, false, false, true};
                for (int i = 1; i < fields.length; i++) { // "index" is not used
                    if (intFields[i]) document.append(fields[i], Double.valueOf(curr[i]).intValue());
                    else document.append(fields[i], curr[i]);
                }
                int index = makePost(curr, oneWordPost, twoWordPost);
                if (index == -1) continue;
                document.append("index", index);
                collection.insertOne(document);
            }
            oneWordPost.flush();
            oneWordPostFile.flush();
            twoWordPost.flush();
            twoWordPostFile.flush();
        }
    }

    private String parseContent(String raw) {
        raw = raw.toLowerCase();
        raw = raw.replaceAll("[^a-z0-9']", " ");
        raw = raw.replaceAll("\\s+", " ");
        return raw.trim();
    }

    private String formatWord(String word) {
        word = word.trim();
        if (word.length() == 0 || word.equals("'")) return "";
        if (word.charAt(0) == '\'') word = word.substring(1, word.length());
        if (word.charAt(word.length() - 1) == '\'') word = word.substring(0, word.length() - 1);
        return word;
    }

    private int makePost(String[] lyrics, BufferedWriter oneWordPost, BufferedWriter twoWordPost) throws IOException{
        String content = parseContent(lyrics[5]);
        String[] words = content.split("\\s");
        List<String> formatedWord = new ArrayList<>(words.length);
        for (String word : words) {
            word = formatWord(word);
            if (englishWords.contains(word)) formatedWord.add(word);
        }
        if (formatedWord.size() < 10) return -1;
        String[] oneWords = new String[formatedWord.size()];
        formatedWord.toArray(oneWords);
        String[] twoWords = twoWords(oneWords);
        writePost(oneWords, id, oneWordPost);
        writePost(twoWords, id, twoWordPost);
        return id++;
    }

    private String[] twoWords(String[] words) throws IOException {
        int len = words.length;
        String[] twoWords = new String[len - 1];
        for (int i = 0; i < len - 1; i++) {
            twoWords[i] = words[i] + "-" + words[i + 1];
        }
        return twoWords;
    }

    private void writePost(String[] words, int index, BufferedWriter post) throws IOException {
        map.clear();
        for (String word : words) {
            map.put(word, map.getOrDefault(word, 0) + 1);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            post.write(String.format("%s:%d:%d\n", entry.getKey(), index, entry.getValue()));
        }
    }

    private void buildIndex(String dataFile) throws IOException, InterruptedException {
        System.out.println("Connecting to database...");
        connetToDB("localhost", 27017);
        System.out.println("Building English words set...");
        buildEnglishWords("./data/words_eng.txt");
        System.out.println("Parsing the lyrics data...");
        parseData(dataFile);
        System.out.println("Sorting posts...");
        Process process = new ProcessBuilder("/bin/bash", "-c", "sort -t ':' -k 1,1 -k 2n,2 ./result/OneWordPosts -o ./result/OneWordPosts").start();
        process.waitFor();
        process = new ProcessBuilder("/bin/bash", "-c", "sort -t ':' -k 1,1 -k 2n,2 ./result/TwoWordPosts -o ./result/TwoWordPosts").start();
        process.waitFor();
        System.out.println("Building Lexicon and InvertedList...");
        makeLexicon("./result/OneWordPosts");
        makeLexicon("./result/TwoWordPosts");
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
        try (FileOutputStream invertedListFile = new FileOutputStream(postFileName.replaceFirst("Posts", "InvertedList"));
             FileWriter lexiconFile = new FileWriter(postFileName.replaceFirst("Posts", "Lexicon"));
             FileWriter chunkFile = new FileWriter(postFileName.replaceFirst("Posts", "Chunk"));
             FileReader postFile = new FileReader(postFileName)) {
             LexiconState formerLexicon = new LexiconState(null, 0, 0, 0);
             ChunkState formerChunk = new ChunkState(0, 0, 0, 0, 0);
             BufferedOutputStream invertedList = new BufferedOutputStream(invertedListFile, BUFFER_SIZE);
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
            lexiconFile.flush();
            invertedList.flush();
            invertedListFile.flush();
            chunk.flush();
            chunkFile.flush();
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
        System.out.println(Calculator.sizeCalculator("OneWordInvertedList", "OneWordLexicon", "OneWordChunk", "TwoWordInvertedList", "TwoWordLexicon", "TwoWordChunk"));
    }
}
