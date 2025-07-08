import os
import glob
import shutil
import subprocess
import nextflow
from datetime import datetime
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
        # Run with error
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
        self.assertEqual(len(execution.process_executions), 3)
        proc_ex = self.get_process_execution(execution, "SPLIT_FILE")
        self.assertEqual(proc_ex.status, "COMPLETED")
        self.assertEqual(proc_ex.return_code, "0")
        passed_identifier = proc_ex.identifier
        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")
        duplicate_identifier = proc_ex.identifier
        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.assertIn(proc_ex.status, ["FAILED", "-"])
        self.assertEqual(proc_ex.return_code, "1")
        duplicate_identifier = proc_ex.identifier

        # Retry - get a little further this time
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            resume=True,
            params={
                "input": self.get_path("files/data.txt"), "count": "9", "flag": "xxx",
                "suffix": self.get_path("files/suffix.txt")
            }
        )
        self.assertIn("pipeline.nf -resume --input", execution.command)
        self.assertEqual(execution.status, "ERROR")
        self.assertEqual(execution.return_code, "1")
        self.assertIn("Error executing process", execution.stdout)
        self.assertIn("Cached process > SPLIT_FILE", execution.stdout)
        uuid = execution.session_uuid
        self.assertEqual(len(execution.process_executions), 5)
        split = self.get_process_execution(execution, "SPLIT_FILE")
        self.assertEqual(split.identifier, passed_identifier)
        self.assertIsNone(split.submitted)
        self.assertIsNone(split.started)
        self.assertIsNone(split.finished)
        self.assertEqual(split.stdout, "Splitting...\n")
        self.assertEqual(split.stderr, "")
        self.assertTrue(split.bash.startswith("#!/usr/bin/env"))
        self.assertEqual(split.status, "COMPLETED")
        self.assertEqual(split.return_code, "0")
        self.assertTrue(split.cached)
        duplicate1 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.assertEqual(duplicate1.status, "COMPLETED")
        self.assertEqual(duplicate1.return_code, "0")
        self.assertFalse(duplicate1.cached)
        self.assertNotEqual(duplicate1.identifier, duplicate_identifier)
        duplicate1_identifier = duplicate1.identifier
        duplicate2 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.assertEqual(duplicate2.status, "COMPLETED")
        self.assertEqual(duplicate2.return_code, "0")
        self.assertFalse(duplicate2.cached)
        self.assertNotEqual(duplicate2.identifier, duplicate_identifier)
        duplicate2_identifier = duplicate2.identifier
        lower1 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_abc.dat)")
        self.assertEqual(lower1.status, "FAILED")
        self.assertEqual(lower1.return_code, "1")
        self.assertFalse(lower1.cached)
        lower2 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_xyz.dat)")
        self.assertEqual(lower2.status, "FAILED")
        self.assertEqual(lower2.return_code, "1")
        self.assertFalse(lower2.cached)

        # Retry with working params
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            resume=uuid,
            params={
                "input": self.get_path("files/data.txt"), "count": "9",
                "suffix": self.get_path("files/suffix.txt")
            }
        )
        self.assertIn(f"pipeline.nf -resume {uuid} --input", execution.command)
        self.assertEqual(execution.status, "OK")
        self.assertEqual(execution.return_code, "0")
        self.assertIn("Cached process > SPLIT_FILE", execution.stdout)
        self.assertIn("Cached process > PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)", execution.stdout)
        self.assertIn("Cached process > PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)", execution.stdout)
        self.assertEqual(len(execution.process_executions), 8)
        split = self.get_process_execution(execution, "SPLIT_FILE")
        self.assertEqual(split.identifier, passed_identifier)
        duplicate1 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.assertEqual(duplicate1.status, "COMPLETED")
        self.assertEqual(duplicate1.return_code, "0")
        self.assertTrue(duplicate1.cached)
        self.assertEqual(duplicate1.identifier, duplicate1_identifier)
        duplicate2 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.assertEqual(duplicate2.status, "COMPLETED")
        self.assertEqual(duplicate2.return_code, "0")
        self.assertTrue(duplicate2.cached)
        self.assertEqual(duplicate2.identifier, duplicate2_identifier)
        lower1 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_abc.dat)")
        self.assertEqual(lower1.status, "COMPLETED")
        self.assertEqual(lower1.return_code, "0")
        self.assertFalse(lower1.cached)
        lower2 = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_xyz.dat)")
        self.assertEqual(lower2.status, "COMPLETED")
        self.assertEqual(lower2.return_code, "0")
        self.assertFalse(lower2.cached)



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
    

    def test_can_run_with_specific_log_location(self):
        # Make location for outputs
        os.chdir(self.rundirectory)
        log_path = os.path.sep + os.path.join(*self.rundirectory.split(os.path.sep)[:-1], "log_loc")
        os.mkdir(log_path)

        try:
            # Run basic execution
            execution = nextflow.run(
                pipeline_path=self.get_path("pipeline.nf"),
                log_path=str(log_path),
                params={
                    "input": self.get_path("files/data.txt"), "count": "12",
                    "suffix": self.get_path("files/suffix.txt")
                }
            )

            # Execution is fine
            self.check_execution(execution, log_path=str(log_path))
        finally:
            # Remove outputs
            shutil.rmtree(log_path)
    

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
    

    def test_can_run_with_custom_io(self):
        # Make custom io
        class CustomIO:

            def write(self_c, content):
                with open(f"{self.rundirectory}/log.txt", "a") as f:
                    f.write(content)

            def abspath(self, path):
                self.write(f"abspath: {path}\n")
                return os.path.abspath(path)

            def listdir(self, path):
                self.write(f"listdir: {path}\n")
                return os.listdir(path)

            def read(self, path, mode="r"):
                self.write(f"read: {path} {mode}\n")
                try:
                    with open(path, mode) as f: return f.read()
                except FileNotFoundError:
                    return ""

            def ctime(self, path):
                self.write(f"ctime: {path}\n")
                return datetime.fromtimestamp(os.path.getctime(path))

            def glob(self, path):
                self.write(f"glob: {path}\n")
                return glob.glob(path)

        io = CustomIO()

        # Run execution
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            run_path=str(self.rundirectory),
            io=io,
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution)

        # The custom io functions were used
        with open(f"{self.rundirectory}/log.txt", "r") as f:
            text = f.read()
            self.assertIn("abspath: ", text)
            self.assertIn("listdir: ", text)
            self.assertIn("read: ", text)
            self.assertIn("ctime: ", text)
            self.assertIn("glob: ", text)
    

    def test_can_run_with_specific_version(self):
        # Run basic execution
        os.chdir(self.rundirectory)
        execution = nextflow.run(
            pipeline_path=self.get_path("pipeline.nf"),
            version="25.04.0",
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            }
        )

        # Execution is fine
        self.check_execution(execution, version="25.04.0", check_stderr=False)
    

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