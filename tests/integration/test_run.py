import os
import shutil
import subprocess
import nextflow
from .base import RunTestCase

class BasicRunningTests(RunTestCase):

    def test_can_run_basic(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution)

        
    def test_can_handle_pipeline_error(self):
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            params={
                "input": self.get_path("files/data.txt"), "count": "string",
                "suffix": self.get_path("files/suffix.txt")
            }
        )
        self.assertEqual(execution.status, "ERROR")
        self.assertEqual(execution.return_code, "1")
        self.assertIn("Error executing process", execution.stdout)
        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")
        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")



class CustomRunningTests(RunTestCase):

    def test_can_run_with_specific_run_location(self):
        # Run basic execution
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            run_path=str(self.rundirectory),
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution)
    

    def test_can_run_with_specific_output_location(self):
        # Make location for outputs
        outputs_path = os.path.sep + os.path.join(*self.rundirectory.split(os.path.sep)[:-1], "outputs")
        os.mkdir(outputs_path)

        try:
            # Run basic execution
            execution = nextflow.run(
                pipeline_path=self.get_path("pipeline.nf"),
                output_path=str(outputs_path),
                params={
                    "input": self.get_path("files/data.txt"), "count": "12",
                    "suffix": self.get_path("files/suffix.txt")
                }
            )

            # Execution is fine
            self.check_execution(execution, output_path=str(outputs_path))
        finally:
            # Remove outputs
            shutil.rmtree(outputs_path)
    

    def test_can_run_with_specific_run_location_and_output_location(self):
        # Make location for outputs
        outputs_path = os.path.sep + os.path.join(*self.rundirectory.split(os.path.sep)[:-1], "outputs")
        os.mkdir(outputs_path)

        try:
            # Run basic execution
            execution = nextflow.run(
                pipeline_path=self.get_path("pipeline.nf"),
                run_path=str(self.rundirectory),
                output_path=str(outputs_path),
                params={
                    "input": self.get_path("files/data.txt"), "count": "12",
                    "suffix": self.get_path("files/suffix.txt")
                }
            )

            # Execution is fine
            self.check_execution(execution, output_path=str(outputs_path))
        finally:
            # Remove outputs
            shutil.rmtree(outputs_path)
    

    def test_can_run_with_runner(self):
        # Make runner function
        def runner(command):
            command = command.replace("--count='12'", "--count='5'")
            return subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Run execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            runner=runner,
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution, line_count=10)
    

    def test_can_run_with_specific_version(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            version="22.10.8",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution, version="22.10.8", check_stderr=False)
    

    def test_can_run_with_specific_config(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            configs=[self.get_path("pipeline.config")],
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution)
        self.assertIn("split_file", os.listdir(self.get_path("rundirectory/results")))
    

    def test_can_run_with_stage_by_copy_config(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            configs=[self.get_path("pipeline.config"), self.get_path("copy.config")],
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution)
        self.assertIn("split_file", os.listdir(self.get_path("rundirectory/results")))
    

    def test_can_run_with_specific_profile(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            profiles=["special"],
            configs=[self.get_path("pipeline.config")],
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution)
        self.assertIn("Applying config profile: `special`", execution.log)
    

    def test_can_run_with_specific_timezone(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            timezone="UTC",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution, timezone="UTC")
    

    def test_can_run_with_reports(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            report="report.html", timeline="time.html", dag="dag.html", trace="trace.txt",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution, report="report.html", timeline="time.html", dag="dag.html", trace="trace.txt")