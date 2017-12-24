public class LexiconState {

    String term;
    int countLyrics;
    int startChunk;
    int endChunk;

    public LexiconState(String term, int countLyrics, int startChunk, int endChunk) {
        this.term = term;
        this.countLyrics = countLyrics;
        this.startChunk = startChunk;
        this.endChunk = endChunk;
    }
}
