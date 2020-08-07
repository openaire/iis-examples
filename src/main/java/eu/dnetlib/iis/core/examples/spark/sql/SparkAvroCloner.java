package eu.dnetlib.iis.core.examples.spark.sql;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.common.utils.AvroUtils;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

/**
 * A spark generic avro cloner which uses sparkSql to read and write avro data. Parameters:
 * <p>
 * -inputAvroHdfsFilePath - hdfs path to file(s) with records that are supposed to be cloned
 * -inputAvroClass - fully qualified name of the class generated from avro schema
 * -outputAvroHdfsFilePath - hdfs path the cloned avro records will saved to
 * -numberOfCopies - number of copies of each avro record
 *
 * @author Łukasz Dumiszewski
 */
public class SparkAvroCloner {

    public static void main(String[] args) throws IOException {
        SparkClonerParameters params = new SparkClonerParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        Schema schema = AvroUtils.toSchema(params.avroSchemaClass);
        Job job = Job.getInstance();
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setOutputKeySchema(job, schema);

        SparkConf conf = new SparkConf();
        conf.setAppName("File word count with spark sql");

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(conf)) {
            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark);

            Dataset<Row> inputDf = avroDataFrameSupport
                    .read(params.inputAvroPath, Person.SCHEMA$);

            int numberOfCopies = params.numberOfCopies;
            Dataset<Row> outputDf = inputDf
                    .withColumn("dummy", explode(sequence(lit(1), lit(numberOfCopies))))
                    .drop("dummy");

            avroDataFrameSupport.write(
                    spark.createDataFrame(outputDf.javaRDD(), (StructType) SchemaConverters.toSqlType(schema).dataType()),
                    params.outputAvroPath,
                    schema);
        }
    }

    @Parameters(separators = "=")
    private static class SparkClonerParameters {

        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;

        @Parameter(names = "-avroSchemaClass", required = true, description = "fully qualified name of the class generated from avro schema")
        private String avroSchemaClass;

        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;

        @Parameter(names = "-numberOfCopies", required = true)
        private int numberOfCopies;
    }
}
