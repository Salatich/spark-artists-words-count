import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.jmusixmatch.MusixMatch;
import org.jmusixmatch.MusixMatchException;
import org.jmusixmatch.entity.track.Track;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class WordUtils {


    public static Iterator<String> getWords(String string) {
        return Arrays.asList(string.split(" ")).iterator();
    }

    public static Set getNotValuableWords() {
        return new HashSet<>(Arrays.asList(
                "the", "i", "a", "and", "you", "to", "in",
                "of", "on", "me", "is", "your", "that", "my",
                "for", "we", "with", "it", "be", "do", "when", "dont",
                "im", "as", "its", "this", "at", "so", "an", "can", "not", "was", "if", "just", "no", "cant"));
    }

    public static List<String> getLyricsFromMusixMatch(String apiKey, String artist) throws MusixMatchException {
        MusixMatch musixMatch = new MusixMatch(apiKey);
        List<Track> tracks = new ArrayList<>();
        for (int i = 1; i < 10; i++) {
            tracks.addAll(musixMatch.searchTracks("", artist, "", i, 100, true));
        }
        Set<Integer> ids = tracks.stream().map(track -> track
                .getTrack()
                .getTrackId())
                .sorted(Integer::compareTo)
                .collect(Collectors.toSet());

        List<String> lyrics = new ArrayList<>();
        for (Integer id : ids) {
            String stringForAdding = musixMatch
                    .getLyrics(id)
                    .getLyricsBody()
                    .replace("******* This Lyrics is NOT for Commercial use *******", "")
                    .replaceAll("[^a-zA-Z\\s]", "");
            lyrics.add(stringForAdding);
        }
        return lyrics;
    }


    public static JavaPairRDD<Integer, String> getWords(JavaRDD<String> words) {
        return words
                .map(String::toLowerCase)
                .flatMap(WordUtils::getWords)
                .filter(x -> !WordUtils.getNotValuableWords().contains(x))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false);
    }
}
