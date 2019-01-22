import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.jmusixmatch.MusixMatchException;

import java.util.List;

public class Application {
    public static void main(String[] args) throws MusixMatchException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("wordCount");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        String apiKey = args[0];
        String artist = args[1];

        List<String> words = WordUtils.getLyricsFromMusixMatch(apiKey, artist);
        WordUtils.getWords(javaSparkContext.parallelize(words)).collect().forEach(System.out::println);
    }
}
