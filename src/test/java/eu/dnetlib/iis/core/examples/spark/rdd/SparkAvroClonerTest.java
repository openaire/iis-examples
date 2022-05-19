package eu.dnetlib.iis.core.examples.spark.rdd;

import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Łukasz Dumiszewski
 */
public class SparkAvroClonerTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    private File workingDir;

    @BeforeEach
    public void before() throws IOException {
        workingDir = Files.createTempDirectory(SparkAvroClonerTest.class.getSimpleName()).toFile();
    }

    @AfterEach
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }

    //------------------------ TESTS --------------------------

    @Test
    public void sparkAvroCloner() throws IOException {
        // given
        String inputDirPath = workingDir + "/spark_avro_cloner/input";
        String outputDirPath = workingDir + "/spark_avro_cloner/output";
        AvroTestUtils.createLocalAvroDataStore(StandardDataStoreExamples.getPerson(), inputDirPath);
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName(this.getClass().getSimpleName())
                .addJobProperty("spark.driver.host", "localhost")
                .setMainClass(SparkAvroCloner.class)
                .addArg("-avroSchemaClass", Person.class.getName())
                .addArg("-inputAvroPath", inputDirPath)
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-numberOfCopies", "" + 3)
                .build();

        // execute
        executor.execute(sparkJob);

        // assert
        List<Person> people = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertEquals(15, people.size());
        assertEquals(3, people.stream().filter(p -> p.getName().equals(new Utf8("Stieg Larsson"))).count());
    }
}
