package eu.dnetlib.iis.core.examples.spark.rdd;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Simple service counting words in the specified file and writing the results to the given directory
 *
 * @author Łukasz Dumiszewski
 */

public class FileWordCounter {

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length > 1, "You must enter the input file and output directory");

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            JavaRDD<String> lines = sc.textFile(args[0], 2);

            JavaPairRDD<String, Integer> words = lines
                    .flatMap(line -> Arrays.asList(line.split("\\W+")).iterator())
                    .mapToPair(word -> new Tuple2<>(word, 1));

            JavaPairRDD<String, Integer> wordCounts = words.reduceByKey(Integer::sum);

            wordCounts.coalesce(1).saveAsTextFile(args[1]);
        }
    }

}