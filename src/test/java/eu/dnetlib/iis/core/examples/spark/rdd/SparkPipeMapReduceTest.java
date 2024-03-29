package eu.dnetlib.iis.core.examples.spark.rdd;

import eu.dnetlib.iis.common.avro.Document;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.core.examples.StandardDataStoreExamples;
import eu.dnetlib.iis.core.examples.schemas.WordCount;
import eu.dnetlib.iis.core.examples.schemas.documentandauthor.Person;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class SparkPipeMapReduceTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    private File workingDir;
    private String inputDirPath;
    private String outputDirPath;

    @BeforeEach
    public void before() throws IOException {
        workingDir = Files.createTempDirectory(SparkPipeMapReduceTest.class.getSimpleName()).toFile();
        inputDirPath = workingDir + "/input";
        outputDirPath = workingDir + "/output";
    }

    @AfterEach
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }

    //------------------------ TESTS --------------------------

    @Test
    public void mapReduceWordCount() throws IOException {
        // given
        AvroTestUtils.createLocalAvroDataStore(StandardDataStoreExamples.getDocument(), inputDirPath);
        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName(this.getClass().getSimpleName())
                .addJobProperty("spark.driver.host", "localhost")
                .setMainClass(SparkPipeMapReduce.class)
                .addArg("-inputAvroPath", inputDirPath)
                .addArg("-inputAvroSchemaClass", Document.class.getName())
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-outputAvroSchemaClass", WordCount.class.getName())
                .addArg("-mapperScript", "src/main/resources/eu/dnetlib/iis/core/examples/spark/rdd/pipe_cloner/oozie_app/scripts/wordcount_mapper.py")
                .addArg("-reducerScript", "src/main/resources/eu/dnetlib/iis/core/examples/spark/rdd/pipe_cloner/oozie_app/scripts/wordcount_reducer.py")
                .build();

        // execute
        executor.execute(sparkJob);

        // assert
        List<WordCount> wordCounts = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertThat(wordCounts, hasItem(new WordCount("basics", 2)));
        assertThat(wordCounts, hasItem(new WordCount("even", 1)));
        assertThat(wordCounts, hasItem(new WordCount("of", 2)));
        assertThat(wordCounts, hasSize(10));
    }

    @Test
    public void mapReduceCloner() throws IOException {
        // given
        AvroTestUtils.createLocalAvroDataStore(StandardDataStoreExamples.getPerson(), inputDirPath);

        SparkJob sparkJob = SparkJobBuilder
                .create()
                .setAppName("Spark Pipe Cloner")
                .addJobProperty("spark.driver.host", "localhost")
                .setMainClass(SparkPipeMapReduce.class)
                .addArg("-inputAvroPath", inputDirPath)
                .addArg("-inputAvroSchemaClass", Person.class.getName())
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-outputAvroSchemaClass", Person.class.getName())
                .addArg("-mapperScript", "src/main/resources/eu/dnetlib/iis/core/examples/spark/rdd/pipe_cloner/oozie_app/scripts/cloner.py")
                .addArg("-mapperScriptArgs", "--copies 3")
                .addArg("-reducerScript", "src/main/resources/eu/dnetlib/iis/core/examples/spark/rdd/pipe_cloner/oozie_app/scripts/cloner.py")
                .addArg("-reducerScriptArgs", "--copies 2")
                .build();

        // execute
        executor.execute(sparkJob);

        // assert
        List<Person> people = AvroTestUtils.readLocalAvroDataStore(outputDirPath);
        assertEquals(5 * 6, people.size());
        assertEquals(6, people.stream().filter(p -> p.getId() == 1).count());
        assertEquals(6, people.stream().filter(p -> p.getId() == 20).count());
    }

}
