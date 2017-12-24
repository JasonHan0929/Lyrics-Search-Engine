public class ChunkState {
    int chunkNum;
    int lastIndex;
    int chunkSize;
    long startBytes;
    long endBytes;

    public ChunkState(int chunkNum, int lastIndex, int chunkSize,long startBytes, long endBytes) {
        this.chunkNum = chunkNum;
        this.lastIndex = lastIndex;
        this.chunkSize = chunkSize;
        this.startBytes = startBytes;
        this.endBytes = endBytes;
    }
}
