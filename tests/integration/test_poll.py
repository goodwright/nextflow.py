import os
import subprocess
import nextflow
from .base import RunTestCase

class BasicRunAndPollTests(RunTestCase):

    def test_can_run_basic(self):
        # Run basic execution
        os.chdir(self.rundirectory)

        # Run and collect basic executions
        executions = []
        last_stdout = ""
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        # Last execution is fine
        self.check_execution(executions[-1])

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].identifier, executions[-1].identifier)
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
    

    def test_can_handle_pipeline_error(self):
        os.chdir(self.rundirectory)
        executions = []
        last_stdout = ""
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            params={
                "input": self.get_path("files/data.txt"), "count": "string",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        self.assertEqual(executions[-1].status, "ERROR")
        self.assertEqual(executions[-1].return_code, "1")
        self.assertIn("Error executing process", executions[-1].stdout)
        proc_ex = self.get_process_execution(executions[-1], "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")
        proc_ex = self.get_process_execution(executions[-1], "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")
