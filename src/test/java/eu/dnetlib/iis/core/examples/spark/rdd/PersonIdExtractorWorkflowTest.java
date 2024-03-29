package eu.dnetlib.iis.core.examples.spark.rdd;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.WorkflowTestResult;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;

/**
 * @author madryk
 */
public class PersonIdExtractorWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void personIdExtractTest() throws Exception {
        // given
        OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
        conf.addExpectedOutputFile("output/person_id/part-00000");

        // execute
        WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/spark/rdd/person_id_extractor", conf);

        // assert
        TestsIOUtils
                .assertUtf8TextContentsEqual(
                        this.getClass().getResourceAsStream("/eu/dnetlib/iis/core/examples/simple_csv_data/person_id.csv"),
                        new FileInputStream(workflowTestResult.getWorkflowOutputFile("output/person_id/part-00000")));
    }
}
