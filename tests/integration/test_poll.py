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



class CustomRunningTests(RunTestCase):

    def test_can_run_with_specific_location(self):
        # Run basic execution
        executions = []
        last_stdout = ""
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            run_path=str(self.rundirectory),
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        # Execution is fine
        self.check_execution(execution)

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
    

    def test_can_run_with_runner(self):
        # Make runner function
        def runner(command):
            command = command.replace("--count='12'", "--count='5'")
            return subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Run execution
        executions = []
        last_stdout = ""
        os.chdir(self.rundirectory)
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            runner=runner,
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        # Execution is fine
        self.check_execution(execution, line_count=10)
    

    def test_can_run_with_specific_version(self):
        # Run basic execution
        executions = []
        os.chdir(self.rundirectory)
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            version="21.10.3",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            executions.append(execution)

        # Execution is fine
        self.check_execution(execution, version="21.10.3", check_stderr=False)

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
    

    def test_can_run_with_specific_config(self):
        # Run basic execution
        executions = []
        last_stdout = ""
        os.chdir(self.rundirectory)
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            configs=[self.get_path("pipeline.config")],
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        # Execution is fine
        self.check_execution(execution)
        self.assertIn("split_file", os.listdir(self.get_path("rundirectory/results")))

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
    

    def test_can_run_with_specific_profile(self):
        # Run basic execution
        executions = []
        last_stdout = ""
        os.chdir(self.rundirectory)
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            profiles=["special"],
            configs=[self.get_path("pipeline.config")],
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        # Execution is fine
        self.check_execution(execution)
        self.assertIn("Applying config profile: `special`", execution.log)

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
    

    def test_can_run_with_specific_timezone(self):
        # Run basic execution
        executions = []
        last_stdout = ""
        os.chdir(self.rundirectory)
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            timezone="UTC",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        # Execution is fine
        self.check_execution(execution, timezone="UTC")

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
    

    def test_can_run_with_reports(self):
        # Run basic execution
        executions = []
        last_stdout = ""
        os.chdir(self.rundirectory)
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            report="report.html", timeline="time.html", dag="dag.html",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(execution)

        # Execution is fine
        self.check_execution(execution, report="report.html", timeline="time.html", dag="dag.html")

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
