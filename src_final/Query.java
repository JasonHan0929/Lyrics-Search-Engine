import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.io.*;
import java.util.*;

public class Query {

    private final String oneWordLexiconName;
    private final String oneWordChunkName;
    private final String oneWordInvertedName;
    private final String twoWordLexiconName;
    private final String twoWordChunkName;
    private final String twoWordInvertedName;
    private MongoClient connection;
    private final int BUFFER_SIZE = 1024 * 1024;
    private final List<Chunk> oneWordchunkList = new ArrayList<>();
    private final Map<String, InvertedIndex> oneWordLexicon = new HashMap<>();
    private final List<Chunk> twoWordchunkList = new ArrayList<>();
    private final Map<String, InvertedIndex> twoWordLexicon = new HashMap<>();
    private final int totalLyrics;
    private final long totalWords;
    private final MongoCollection<Document> lyrics;
    private final double k1 = 1.2;
    private final double b = 0.75;
    private final double theta1 = 1.5;
    private final double theta2 = -0.5;
    private final Map<Integer, Queue<int[]>> oneWordUncompressed = new HashMap<>();
    private final Map<Integer, Queue<int[]>> twoWordUncompressed = new HashMap<>();
    private final VariableByteCode vByte = new VariableByteCode();
    private final QueryResult[] resultMap;
    private final int resultSize;
    private final PriorityQueue<Integer> topK;
    private final int[] lyricsLength;
    private final double averageLyricsLength;
    private String queryInput;
    private List<String> filedsRestrains;
    private final Set<String> fileds = new HashSet<>(Arrays.asList("--index", "--song", "--year", "--artist", "--genre", "--lyrics", "--word_count"));

    public Query(String oneWordLexiconName, String oneWordChunkName, String oneWordInvertedName, String twoWordLexiconName, String twoWordChunkName, String twoWordInvertedName, int resultSize) {
        this.oneWordLexiconName = oneWordLexiconName;
        this.oneWordChunkName = oneWordChunkName;
        this.oneWordInvertedName = oneWordInvertedName;
        this.twoWordLexiconName = twoWordLexiconName;
        this.twoWordChunkName = twoWordChunkName;
        this.twoWordInvertedName = twoWordInvertedName;
        connetToDB("localhost", 27017);
        MongoDatabase database = connection.getDatabase("songs");
        lyrics = database.getCollection("lyrics");
        totalLyrics = (int)lyrics.count();
        List<BasicDBObject> aggregateOption = new ArrayList<>(1);
        aggregateOption.add (new BasicDBObject(
                "$group", new BasicDBObject("_id", null).append(
                "total", new BasicDBObject( "$sum", "$word_count" )
        )));
        totalWords = lyrics.aggregate(aggregateOption).iterator().next().getInteger("total");
        this.resultSize = resultSize;
        resultMap = new QueryResult[totalLyrics];
        topK = new PriorityQueue<>(resultSize, (x, y) -> Double.compare(resultMap[x].bm25, resultMap[y].bm25));
        lyricsLength = new int[totalLyrics];
        averageLyricsLength = (double)totalWords / totalLyrics;
    }

    private void connetToDB(String localhost, int port) {
        try {
            connection =  new MongoClient(localhost, port);
            System.out.println("Connect to MongoDB successfully");

        } catch(Exception e) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    private void getLyricsLength() {
        for (Document curr : lyrics.find()) {
            int index = curr.getInteger("index");
            int count = curr.getInteger("word_count");
            lyricsLength[index] = count;
        }
    }

    private void readChunkTable() throws IOException {
        try (FileReader oneWordChunkFile = new FileReader(oneWordChunkName);
             FileReader twoWordChunkFile = new FileReader(twoWordChunkName)) {
            BufferedReader[] chunks = new BufferedReader[2];
            chunks[0] = new BufferedReader(oneWordChunkFile, BUFFER_SIZE);
            chunks[1] = new BufferedReader(twoWordChunkFile, BUFFER_SIZE);
            for (int i = 0; i < chunks.length; i++) {
                List<Chunk> currList = i == 0 ? oneWordchunkList : twoWordchunkList;
                while (chunks[i].ready()) {
                    String[] words = chunks[i].readLine().split(",");
                    Chunk curr = new Chunk(Long.valueOf(words[2]), Long.valueOf(words[3]), Integer.valueOf(words[1]));
                    currList.add(curr);
                }
            }
        }
    }

    private void readLexicon() throws IOException {
        try (FileReader oneWordLexiconFile = new FileReader(oneWordLexiconName);
             FileReader twoWordLexiconFile = new FileReader(twoWordLexiconName)) {
            BufferedReader[] lexicons = new BufferedReader[2];
            lexicons[0] = new BufferedReader(oneWordLexiconFile, BUFFER_SIZE);
            lexicons[1] = new BufferedReader(twoWordLexiconFile, BUFFER_SIZE);
            for (int i = 0; i < lexicons.length; i++) {
                Map<String, InvertedIndex> currLexicon = i == 0 ? oneWordLexicon : twoWordLexicon;
                while (lexicons[i].ready()) {
                    String[] words = lexicons[i].readLine().split(",");
                    InvertedIndex curr = new InvertedIndex(Integer.valueOf(words[1]), Integer.valueOf(words[2]), Integer.valueOf(words[3]));
                    currLexicon.put(words[0], curr);
                }
            }
        }
    }

    private double calculateK(int lyricsLength) {
        return k1 * ((1 - b) + b * lyricsLength / averageLyricsLength);
    }

    private double calculateBM25(Map<String, Integer> freqMap, int lyricsLength) {
        double bm25 = 0;
        int minFreq = Collections.min(freqMap.values());
        for (String term : freqMap.keySet()) {
            int containsTerm = (twoWordLexicon.containsKey(term) ? twoWordLexicon.get(term) : oneWordLexicon.get(term)).countFiles;
            int freq = freqMap.get(term);
            double K = calculateK(lyricsLength);
            double part1 = (totalLyrics - containsTerm + 0.5) / (containsTerm + 0.5);
            double part2 = ((k1 + 1) * minFreq / (K + minFreq)) * theta1 + (k1 + 1) * (freq - minFreq) / (K + freq - minFreq) * theta2; // modified bm25
            bm25 += Math.log(part1) * part2;
        }
        return bm25;
    }

    private int[] nextGEQ(int chunkId, int k, String term, RandomAccessFile raf, Map<Integer, Queue<int[]>> uncompressed, List<Chunk> chunkList, Map<String, InvertedIndex> lexicon) throws IOException {
        Queue<int[]> uncompressedInvert;
        if (uncompressed.containsKey(chunkId)) {
            uncompressedInvert = uncompressed.get(chunkId);
        } else {
            long start = chunkList.get(chunkId).startByte;
            long end = chunkList.get(chunkId).endByte;
            byte[] chunkByte = new byte[(int)(end - start + 1)];
            raf.seek(start);
            raf.read(chunkByte);
            uncompressedInvert = vByte.decodeChunk(chunkByte, chunkId > lexicon.get(term).startChunk ? chunkList.get(chunkId - 1).lastId + 1: 0); // lastId should add 1
            uncompressed.put(chunkId, uncompressedInvert);
            if (uncompressed.containsKey(chunkId - 1) && lexicon.get(term).startChunk >= chunkId - 1) {
                uncompressed.remove(chunkId - 1);
            } // work ?
        }
        while (!uncompressedInvert.isEmpty()) {
            int currId = uncompressedInvert.peek()[0];
            if (currId >= k) {
                return uncompressedInvert.peek(); // should keep in queue when >=
            } else {
                uncompressedInvert.poll();
            }
        }
        uncompressed.remove(chunkId); // remove uncompressed list when used up
        return null;
    }

    private void combinationResultMap(Map<String, Integer> page, Map<String, Integer> temp) {
        for (Map.Entry<String, Integer> entry : temp.entrySet()) {
            if (page.containsKey(entry.getKey())) {
                page.put(entry.getKey(), entry.getValue() + page.get((entry.getKey())));
            } else {
                page.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private Set<Integer> andQuery(List<String> originTerms, LinkedList<Integer> restrains) throws IOException {
        Set<Integer> result = new HashSet<>();
        if (restrains != null && restrains.size() == 0) return result;
        int len = originTerms.size();
        if (len == 0) return result;
        try (RandomAccessFile oneWordRaf = new RandomAccessFile(oneWordInvertedName,"r");
             RandomAccessFile twoWordRaf = new RandomAccessFile(twoWordInvertedName,"r")) {
            Term[] terms = new Term[len];
            for (int i = 0; i < len; i++) {
                if (oneWordLexicon.containsKey(originTerms.get(i))) {
                    terms[i] = new Term(originTerms.get(i), oneWordLexicon, oneWordchunkList, oneWordRaf, oneWordUncompressed);
                } else if (twoWordLexicon.containsKey(originTerms.get(i))) {
                    terms[i] = new Term(originTerms.get(i), twoWordLexicon, twoWordchunkList, twoWordRaf, twoWordUncompressed);
                } else {
                    return result;
                }
            }
            int[] currChunk = new int[len];
            Arrays.sort(terms, (x, y) -> Integer.compare(x.lexicon.get(x.term).countFiles, y.lexicon.get(y.term).countFiles));
            for (int i = 0; i < len; i++) {
                Term term = terms[i];
                currChunk[i] = term.lexicon.get(term.term).startChunk;
            }
            int did = 0;
            Map<String, Integer> termFreq = new HashMap<>();
            int lastDid = restrains == null ? totalLyrics : restrains.peekLast();
            while (did <= lastDid) {
                if (restrains != null) {
                    while (!restrains.isEmpty()) {
                        int temp = restrains.poll();
                        if (temp >= did) {
                            did = temp;
                            break;
                        }
                    }
                }
                for (int i = 0; i < len; i++) {
                    Term currTerm = terms[i];
                    while (currChunk[i] <= currTerm.lexicon.get(currTerm.term).endChunk) {
                        if (did <= currTerm.chunkList.get(currChunk[i]).lastId) break;
                        currChunk[i]++;
                    }
                    if (currChunk[i] > currTerm.lexicon.get(currTerm.term).endChunk) return result;
                    int[] post = nextGEQ(currChunk[i], did, currTerm.term, currTerm.raf, currTerm.uncompressed, currTerm.chunkList, currTerm.lexicon);
                    if (post == null) {
                        throw new RuntimeException("Could not find lyrics index in this chunk");
                    }
                    if (post[0] == did) {
                        termFreq.put(currTerm.term, post[1]);
                        if (i == len - 1) {
                            double bm25 = calculateBM25(termFreq, lyricsLength[post[0]]);
                            if (resultMap[did] == null) resultMap[did] = new QueryResult();
                            resultMap[did].bm25 = bm25;
                            combinationResultMap(resultMap[did].termFreq, termFreq);
                            termFreq.clear(); // could not delete
                            result.add(did);
                            did++;
                        }
                    } else {
                        did = post[0];
                        termFreq.clear();
                        termFreq.put(currTerm.term, post[1]);
                        break;
                    }
                }
            }
            return result;
        }
    }

    private void orQuery(Set<Integer> candidates) {
        int len = candidates.size();
        if (len > 0) {
            for (int index : candidates) {
                if (topK.size() < resultSize) topK.offer(index);
                else if (resultMap[index].bm25 > resultMap[topK.peek()].bm25) {
                    int unuse = topK.poll();
                    topK.offer(index);
                    resultMap[unuse].bm25 = 0;
                    resultMap[unuse].termFreq.clear();
                } else {
                    resultMap[index].bm25 = 0;
                    resultMap[index].termFreq.clear();
                }
            }
        }
    }

    private void complexQuery(List<List<String>> query, LinkedList<Integer> restrains) throws IOException{
        if (query.size() == 0) return;
        Set<Integer> candidates = new HashSet<>();
        for (List<String> terms : query) {
            candidates.addAll(andQuery(terms, restrains));
        }
        orQuery(candidates);
    }

    private void afterQuery() {
        for (int index : topK) {
            resultMap[index].bm25 = 0;
            resultMap[index].termFreq.clear();
        }
        oneWordUncompressed.clear();
        twoWordUncompressed.clear();
        topK.clear();
        queryInput = null;
        filedsRestrains = null;
    }

    private LinkedList<Integer> getRestrains() {
        if (filedsRestrains.size() == 0) return null;
        System.out.println(filedsRestrains);
        for (int i = 0; i < filedsRestrains.size(); i++) {
            filedsRestrains.set(i, filedsRestrains.get(i).substring(2));
        }
        LinkedList<Integer> result = new LinkedList<>();
        Document query = new Document();
        Set<String> isInteger = new HashSet<>(Arrays.asList("index", "year", "word_count"));
        for (String entry : filedsRestrains) {
            String[] pairs = entry.split(":");
            query.append(pairs[0], isInteger.contains(pairs[0]) ? Integer.valueOf(pairs[1]) : pairs[1]);
        }
        for (Document curr : lyrics.find(query)) {
            result.add(curr.getInteger("index"));
        }
        Collections.sort(result);
        return result;
    }

    private void parseInput(String input) {
        input = input.toLowerCase().replaceAll("\\s+", " ");
        String[] segments = input.split(" ");
        List<String> restrains = new ArrayList<>();
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i].trim();
            if (segment.length() > 0) {
                if (segment.startsWith("--") && fileds.contains(segment)) {
                    String value = segments[i + 1].trim();
                    restrains.add(segment + ":" + value);
                    i++;
                } else query.append(segment).append(" ");
            }
        }
        queryInput = query.toString();
        filedsRestrains = restrains;
    }

    private List<List<String>> parseQuery() {
        String[] segments = queryInput.split("\\|");
        List<List<String>> result = new ArrayList<>(segments.length);
        for (String addSegment: segments) {
            addSegment = addSegment.trim();
            if (addSegment.length() > 0) {
                String[] parts = addSegment.split("\\+");
                List<String> currTemrs = new ArrayList<>();
                for (String segment : parts) {
                    segment = segment.trim();
                    if (segment.length() > 0) {
                        segment = segment.replaceAll("[^A-Za-z0-9']", " ");
                        segment = segment.trim();
                        segment = segment.replaceAll("[\\s&]+", "&");
                        String[] words = segment.split("&");
                        int len = words.length;
                        if (len > 1) {
                            for (int i = 0; i < len - 1; i++) {
                                currTemrs.add(words[i] + "-" + words[i + 1]);
                            }
                        } else if (len == 1) {
                            currTemrs.add(words[0]);
                        }
                    }
                }
                result.add(currTemrs);
            }
        }
        return result;
    }

    private String dataToString(int index) {
        StringBuilder sb = new StringBuilder();
        Document result = (Document)lyrics.find(new Document("index", index)).iterator().next();
        String[] fields = {"index", "song", "year", "artist", "genre", "lyrics", "word_count"};
        for (int i = 0; i < fields.length; i++) {
            if (i == 5) continue;
            sb.append(fields[i]).append(": ");
            sb.append(result.get(fields[i])).append("\t");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("\n");
        sb.append("lyrics:\n");
        sb.append(result.get(fields[5])).append("\n");
        return sb.toString();
    }

    private String output(long startTime, String input) {
        StringBuilder result = new StringBuilder();
        List<Integer> sorted = new ArrayList<>(topK);
        sorted.sort((x, y) -> Double.compare(resultMap[y].bm25, resultMap[x].bm25));
        for (int index : sorted) {
            result.append(String.format("Modified-BM25: %.2f\n", resultMap[index].bm25));
            result.append("Term Frequency: ").append(resultMap[index].termFreq).append("\n");
            result.append(dataToString(index)).append("\n");
        }
        if (result.length() > 0) result.deleteCharAt(result.length() - 1);
        long endTime = new Date().getTime();
        result.append("Query Input: ").append(input).append("\t");
        result.append("Number of Results: ").append(topK.size()).append("\t");
        result.append(String.format("Time Consumed: %.2fs\n", (double)endTime/1000 - (double)startTime/1000));
        result.append("===========================================================================\n\n");
        afterQuery();
        return result.toString();
    }

    public static void main(String[] args) throws IOException {
        String oneWordChunkName = "./result/OneWordChunk";
        String oneWordLexiconName= "./result/OneWordLexicon";
        String oneWordInvertedName = "./result/OneWordInvertedList";
        String twoWordChunkName = "./result/TwoWordChunk";
        String twoWordLexiconName= "./result/TwoWordLexicon";
        String twoWordInvertedName = "./result/TwoWordInvertedList";
        Query query = new Query(oneWordLexiconName, oneWordChunkName, oneWordInvertedName, twoWordLexiconName, twoWordChunkName, twoWordInvertedName, 5);
        System.out.println("Initializing ChunkTable");
        query.readChunkTable();
        System.out.println("Initializing Lexicon");
        query.readLexicon();
        System.out.println("Initializing LyricsLength");
        query.getLyricsLength();
        System.out.print("Finished Initialization, please input any query:");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String input = scanner.nextLine();
            if (input.equals("!q")) {
                break;
            }
            long start = new Date().getTime();
            query.parseInput(input);
            query.complexQuery(query.parseQuery(), query.getRestrains());
            System.out.print(query.output(start, input));
        }
        query.connection.close();
    }
}
