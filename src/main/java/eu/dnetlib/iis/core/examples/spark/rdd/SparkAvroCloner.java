package eu.dnetlib.iis.core.examples.spark.rdd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collections;

/**
 * A spark generic avro cloner. Parameters:
 * <p>
 * -inputAvroHdfsFilePath - hdfs path to file(s) with records that are supposed to be cloned
 * -inputAvroClass - fully qualified name of the class generated from avro schema
 * -outputAvroHdfsFilePath - hdfs path the cloned avro records will saved to
 * -numberOfCopies - number of copies of each avro record
 *
 * @author Łukasz Dumiszewski
 */

public class SparkAvroCloner {

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        SparkClonerParameters params = new SparkClonerParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        Schema schema = AvroUtils.toSchema(params.avroSchemaClass);
        Job job = Job.getInstance();
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setOutputKeySchema(job, schema);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {

            @SuppressWarnings("unchecked")
            JavaPairRDD<AvroKey<GenericRecord>, NullWritable> inputRecords =
                    (JavaPairRDD<AvroKey<GenericRecord>, NullWritable>) sc
                            .newAPIHadoopFile(params.inputAvroPath,
                                    AvroKeyInputFormat.class,
                                    GenericRecord.class,
                                    NullWritable.class,
                                    job.getConfiguration());

            int numberOfCopies = params.numberOfCopies;

            inputRecords = inputRecords
                    .flatMapToPair(record -> Collections.nCopies(numberOfCopies, record).iterator());

            inputRecords
                    .saveAsNewAPIHadoopFile(params.outputAvroPath, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class, job.getConfiguration());
        }
    }

    //------------------------ PRIVATE --------------------------

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
