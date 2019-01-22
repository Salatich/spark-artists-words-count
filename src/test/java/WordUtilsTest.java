import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordUtilsTest {

    @Test
    public void test() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("wordCount");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        List<String> words = Arrays.asList("love", "everything", "love");
        List<Tuple2<Integer, String>> result = WordUtils.getWords(javaSparkContext.parallelize(words)).collect();
        Assert.assertEquals(result.get(0), new Tuple2<Integer, String>(2, "love"));
    }
}
