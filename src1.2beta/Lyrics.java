public class Lyrics {

    long index;
    final String song;
    final int year;
    final String artist;
    final String genre;
    final String lyrics;
    final int wordCount;

    public Lyrics(long index, String song, int year, String artist, String genre, String lyrics, int wordCount) {
        this.index = index;
        this.song = song;
        this.year = year;
        this.artist = artist;
        this.genre = genre;
        this.lyrics = lyrics;
        this.wordCount = wordCount;
    }
}
