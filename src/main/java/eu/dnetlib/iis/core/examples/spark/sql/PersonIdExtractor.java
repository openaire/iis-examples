package eu.dnetlib.iis.core.examples.spark.sql;

import com.clearspring.analytics.util.Preconditions;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class PersonIdExtractor {

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length == 2, "You must provide input file and output directory");

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            Dataset<String> persons = spark.read()
                    .textFile(args[0]);

            Dataset<Row> persionIds = persons
                    .select(split(col("value"), ",").as("parts"))
                    .selectExpr("parts[0]");

            persionIds
                    .coalesce(1)
                    .write()
                    .text(args[1]);
        }
    }
}
