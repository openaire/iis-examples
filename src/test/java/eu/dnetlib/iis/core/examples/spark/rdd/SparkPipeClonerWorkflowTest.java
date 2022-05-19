package eu.dnetlib.iis.core.examples.spark.rdd;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author madryk
 */
@IntegrationTest
public class SparkPipeClonerWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void sparkPipeClonerTest() {
        testWorkflow("eu/dnetlib/iis/core/examples/spark/rdd/pipe_cloner");
    }
}
