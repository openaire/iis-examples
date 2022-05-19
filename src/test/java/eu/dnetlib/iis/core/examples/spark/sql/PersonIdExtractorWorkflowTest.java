package eu.dnetlib.iis.core.examples.spark.sql;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;
import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.WorkflowTestResult;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class PersonIdExtractorWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void personIdExtractTest() throws Exception {
        // given
        OozieWorkflowTestConfiguration conf = new OozieWorkflowTestConfiguration();
        conf.addExpectedOutputFile("output/person_id/");

        // execute
        WorkflowTestResult workflowTestResult = testWorkflow("eu/dnetlib/iis/core/examples/spark/sql/person_id_extractor", conf);

        // assert
        Path outputDir = workflowTestResult.getWorkflowOutputFile("output/person_id/").toPath();
        List<Path> files = Files.list(outputDir)
                .filter(x -> x.getFileName().toString().startsWith("part-00000"))
                .collect(Collectors.toList());

        assertEquals(1, files.size());
        TestsIOUtils
                .assertUtf8TextContentsEqual(
                        this.getClass().getResourceAsStream("/eu/dnetlib/iis/core/examples/simple_csv_data/person_id.csv"),
                        Files.newInputStream(files.get(0)));
    }
}
