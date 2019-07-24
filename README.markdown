Description of the project
--------------------------
This project depends on modules from <https://github.com/openaire/iis> project. It contains **examples of usage of various types of workflow nodes**. A single example is supposed to serve as a reference/template for a person implementing their own concrete workflow node of a certain type.

Workflow tests using Oozie testing facilities
---------------------------------------------
This project contains tests of various types of workflow nodes. If you want to **implement analogous workflow tests in some other Maven project**, your test case class will have to inherit from the `iis-common`'s ` eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase` class. Please read the description given in the javadoc of `eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase` class for more details and see other test case classes in this project for example workflow tests. 

Other
-----
See the `src/main/scripts` directory for sample scripts that build Oozie workflow packages.
