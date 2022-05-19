package eu.dnetlib.iis.core.examples.spark.sql;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.WorkflowTestResult;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@IntegrationTest
public class FileWordCounterWorkflowTest extends AbstractOozieWorkflowTestCase {

    private static SparkSession spark;

    @BeforeAll
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(FileWordCounterWorkflowTest.class.getSimpleName());
        spark = SparkSessionFactory.withConfAndKryo(conf);
    }

    @AfterAll
    public static void afterClass() {
        spark.stop();
    }

    @Test
    public void fileWordCounter() {
        // given
        OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
        conf.addExpectedOutputFile("output/");

        // when
        WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/spark/sql/file_word_counter", conf);

        // then
        File outputFile = workflowTestResult.getWorkflowOutputFile("output/");
        List<Row> rows = spark.read()
                .parquet(outputFile.getAbsolutePath())
                .collectAsList();
        Map<String, Long> wordCount = rows.stream()
                .collect(Collectors.toMap(x -> x.getString(0), y -> y.getLong(1)));

        assertEquals(2, (long) wordCount.get("universe"));
        assertEquals(1, (long) wordCount.get("dance"));
    }
}
