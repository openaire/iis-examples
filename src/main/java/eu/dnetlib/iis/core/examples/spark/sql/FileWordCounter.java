package eu.dnetlib.iis.core.examples.spark.sql;

import com.google.common.base.Preconditions;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class FileWordCounter {

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length > 1, "You must enter the input file and output directory");

        SparkConf conf = new SparkConf();
        conf.setAppName("File word count with spark sql");

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(conf)) {
            Dataset<String> lines = spark.read()
                    .textFile(args[0]);

            Dataset<Row> wordCounts = lines
                    .select(explode(split(col("value"), "\\W+")).as("word"))
                    .groupBy(col("word"))
                    .agg(count(expr("*")).as("word_count"));

            wordCounts
                    .coalesce(1)
                    .write()
                    .parquet(args[1]);
        }
    }

}