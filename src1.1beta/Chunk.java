public class Chunk {
    final long startByte;
    final long endByte;
    final int lastId;

    Chunk(long startByte, long endByte, int lastId) {
        this.startByte = startByte;
        this.lastId = lastId;
        this.endByte = endByte;
    }

    @Override
    public String toString() {
        return String.format("%d %d %d", lastId, startByte, endByte);
    }
}
