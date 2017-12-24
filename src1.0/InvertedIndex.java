public class InvertedIndex {
    final int countFiles;
    final int startChunk;
    final int endChunk;

    InvertedIndex(int countFiles, int startChunk, int endChunk) {
        this.countFiles = countFiles;
        this.startChunk = startChunk;
        this.endChunk = endChunk;
    }

    @Override
    public String toString() {
        return String.format("%d %d %d", countFiles, startChunk, endChunk);
    }
}
