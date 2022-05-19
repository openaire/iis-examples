package eu.dnetlib.iis.core.examples.pig.testing_with_json.removebelowthreshold.minioozie;

import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/core/examples/pig/testing_with_json/removebelowthreshold/minioozie");
    }

}
