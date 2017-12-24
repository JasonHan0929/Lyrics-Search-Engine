import java.io.RandomAccessFile;
import java.util.Map;
import java.util.List;
import java.util.Queue;

public class Term {

    String term;
    Map<String, InvertedIndex> lexicon;
    List<Chunk> chunkList;
    RandomAccessFile raf;
    Map<Integer, Queue<int[]>> uncompressed;

    public Term(String term, Map<String, InvertedIndex> lexicon, List<Chunk> chunkList, RandomAccessFile raf, Map<Integer, Queue<int[]>> uncompressed) {
        this.term = term;
        this.lexicon = lexicon;
        this. chunkList = chunkList;
        this.raf = raf;
        this.uncompressed = uncompressed;
    }
}
