import os
import copy
import shutil
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
            executions.append(copy.deepcopy(execution))

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
            executions.append(copy.deepcopy(execution))

        self.assertEqual(executions[-1].status, "ERROR")
        self.assertEqual(executions[-1].return_code, "1")
        self.assertIn("Error executing process", executions[-1].stdout)
        proc_ex = self.get_process_execution(executions[-1], "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")
        proc_ex = self.get_process_execution(executions[-1], "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")

        # Retry
        executions = []
        last_stdout = ""
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            resume=True,
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(copy.deepcopy(execution))
        
        self.assertEqual(executions[-1].status, "OK")
        self.assertEqual(executions[-1].return_code, "0")
        self.assertIn("Cached process > SPLIT_FILE", executions[-1].stdout)



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
            executions.append(copy.deepcopy(execution))

        # Execution is fine
        self.check_execution(execution)

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
    

    def test_can_run_with_specific_output_location(self):
        # Make location for outputs
        outputs_path = os.path.sep + os.path.join(*self.rundirectory.split(os.path.sep)[:-1], "outputs")
        os.mkdir(outputs_path)
        try:
            # Run basic execution
            executions = []
            last_stdout = ""
            for execution in nextflow.run_and_poll(
                pipeline_path=self.get_path("pipeline.nf"),
                output_path=str(outputs_path),
                params={
                    "input": self.get_path("files/data.txt"), "count": "12",
                    "suffix": self.get_path("files/suffix.txt")
                }
            ):
                last_stdout = self.check_running_execution(execution, last_stdout, str(outputs_path))
                executions.append(copy.deepcopy(execution))

            # Execution is fine
            self.check_execution(execution, output_path=str(outputs_path))

            # Check that we have at least 2 executions
            self.assertGreater(len(executions), 1)

            # First execution is ongoing
            self.assertEqual(executions[0].return_code, "")
            self.assertIsNone(executions[0].finished)
        
        finally:
            # Remove outputs
            shutil.rmtree(outputs_path)
    

    def test_can_run_with_specific_output_location_and_run_location(self):
        # Make location for outputs
        outputs_path = os.path.sep + os.path.join(*self.rundirectory.split(os.path.sep)[:-1], "outputs")
        os.mkdir(outputs_path)
        try:
            # Run basic execution
            executions = []
            last_stdout = ""
            for execution in nextflow.run_and_poll(
                pipeline_path=self.get_path("pipeline.nf"),
                output_path=str(outputs_path),
                run_path=str(self.rundirectory),
                params={
                    "input": self.get_path("files/data.txt"), "count": "12",
                    "suffix": self.get_path("files/suffix.txt")
                }
            ):
                last_stdout = self.check_running_execution(execution, last_stdout, str(outputs_path))
                executions.append(copy.deepcopy(execution))

            # Execution is fine
            self.check_execution(execution, output_path=str(outputs_path))

            # Check that we have at least 2 executions
            self.assertGreater(len(executions), 1)

            # First execution is ongoing
            self.assertEqual(executions[0].return_code, "")
            self.assertIsNone(executions[0].finished)
        
        finally:
            # Remove outputs
            shutil.rmtree(outputs_path)
    

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
            executions.append(copy.deepcopy(execution))

        # Execution is fine
        self.check_execution(execution, line_count=10)
    

    def test_can_run_with_specific_version(self):
        # Run basic execution
        executions = []
        os.chdir(self.rundirectory)
        for execution in nextflow.run_and_poll(
            pipeline_path=self.get_path("pipeline.nf"),
            version="22.10.8",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            executions.append(copy.deepcopy(execution))

        # Execution is fine
        self.check_execution(execution, version="22.10.8", check_stderr=False)

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
            executions.append(copy.deepcopy(execution))

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
            executions.append(copy.deepcopy(execution))

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
            executions.append(copy.deepcopy(execution))

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
            report="report.html", timeline="time.html", dag="dag.html", trace="trace.txt",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        ):
            last_stdout = self.check_running_execution(execution, last_stdout)
            executions.append(copy.deepcopy(execution))

        # Execution is fine
        self.check_execution(execution, report="report.html", timeline="time.html", dag="dag.html", trace="trace.txt")

        # Check that we have at least 2 executions
        self.assertGreater(len(executions), 1)

        # First execution is ongoing
        self.assertEqual(executions[0].return_code, "")
        self.assertIsNone(executions[0].finished)
