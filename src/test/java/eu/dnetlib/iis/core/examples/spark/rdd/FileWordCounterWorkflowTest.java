package eu.dnetlib.iis.core.examples.spark.rdd;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.WorkflowTestResult;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 * @author Łukasz Dumiszewski
 */
@Category(IntegrationTest.class)
public class FileWordCounterWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void fileWordCounter() throws Exception {
        // given
        OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
        conf.addExpectedOutputFile("output/part-00000");

        // execute
        WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/spark/rdd/file_word_counter", conf);

        // assert
        File outputFile = workflowTestResult.getWorkflowOutputFile("output/part-00000");
        String output = FileUtils.readFileToString(outputFile, "UTF-8");
        assertTrue(output.contains("(universe,2)"));
        assertTrue(output.contains("(dance,1)"));
    }

}
