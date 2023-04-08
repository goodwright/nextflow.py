import os
import re
import shutil
from datetime import datetime
from unittest import TestCase
import nextflow

class BasicRunningTests(TestCase):

    def setUp(self):
        self.rundirectory = self.get_path("rundirectory")
        if os.path.exists(self.rundirectory): shutil.rmtree(self.rundirectory)
        os.mkdir(self.rundirectory)
    

    def tearDown(self):
        shutil.rmtree(self.rundirectory)
        if os.path.exists(".nextflow"): shutil.rmtree(".nextflow")


    def get_path(self, name):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(
            dir_path, "pipelines", name.replace("/", os.path.sep)
        )


    def get_process_execution(self, execution, name):
        return [e for e in execution.process_executions if e.name == name][0]


    def check_process_execution(self, process_execution, execution, long):
        self.assertEqual(process_execution.started.year, datetime.now().year)
        self.assertLessEqual((datetime.now() - execution.started).seconds, 30 if long else 5)
        self.assertLessEqual((datetime.now() - execution.finished).seconds, 30 if long else 5)
        self.assertEqual(process_execution.return_code, "0")
        self.assertEqual(process_execution.status, "COMPLETED")
        self.assertIs(process_execution.execution, execution)
        self.assertGreaterEqual(process_execution.duration.seconds, 0)
        self.assertLessEqual(process_execution.duration.seconds, 6)


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

        # Files creared
        self.assertIn(".nextflow", os.listdir(self.get_path("rundirectory")))
        self.assertIn(".nextflow.log", os.listdir(self.get_path("rundirectory")))

        # Execution is correct
        self.assertTrue(re.match(r"[a-z]+_[a-z]+", execution.identifier))
        self.assertIn("N E X T F L O W", execution.stdout)
        self.assertFalse(execution.stderr)
        self.assertEqual(execution.return_code, "0")
        self.assertLessEqual((datetime.now() - execution.started).seconds, 5)
        self.assertLessEqual((datetime.now() - execution.finished).seconds, 5)
        self.assertGreater(execution.finished, execution.started)
        self.assertTrue(execution.command.startswith("NXF_ANSI_LOG=false nextflow -Duser.country=US run"))
        self.assertIn("Starting process", execution.log)
        self.assertIn("Execution complete -- Goodbye", execution.log)
        self.assertEqual(execution.path, self.get_path("rundirectory"))
        self.assertEqual(len(execution.process_executions), 8)
        self.assertLessEqual(execution.duration.seconds, 5)
        self.assertEqual(execution.status, "OK")

        # Process executions are fine
        proc_ex = self.get_process_execution(execution, "SPLIT_FILE")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "Splitting...\n")
        self.assertEqual(proc_ex.stderr, "")
        self.assertTrue(proc_ex.bash.startswith, "#!/usr/bin/env")
        self.assertEqual(proc_ex.process, "SPLIT_FILE")
        self.assertEqual(proc_ex.input_data(), [self.get_path("files/data.txt")])
        self.assertEqual(proc_ex.input_data(include_path=False), ["data.txt"])
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)),
            {"abc.dat", "xyz.dat", "log.txt"}
        )
        self.assertIn(proc_ex.identifier, proc_ex.all_output_data()[0])

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE")
        self.assertEqual(proc_ex.input_data(include_path=False), ["abc.dat"])
        self.assertIn(
            self.get_process_execution(execution, "SPLIT_FILE").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"duplicated_abc.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE")
        self.assertEqual(proc_ex.input_data(include_path=False), ["xyz.dat"])
        self.assertIn(
            self.get_process_execution(execution, "SPLIT_FILE").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"duplicated_xyz.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_abc.dat)")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER")
        self.assertEqual(proc_ex.input_data(include_path=False), ["duplicated_abc.dat"])
        self.assertIn(
            self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"lowered_duplicated_abc.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_xyz.dat)")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER")
        self.assertEqual(proc_ex.input_data(include_path=False), ["duplicated_xyz.dat"])
        self.assertIn(
            self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"lowered_duplicated_xyz.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:APPEND (lowered_duplicated_abc.dat)")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:APPEND")
        self.assertEqual(set(proc_ex.input_data(include_path=False)), {"lowered_duplicated_abc.dat", "suffix.txt"})
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"suffix_lowered_duplicated_abc.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:APPEND (lowered_duplicated_xyz.dat)")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:APPEND")
        self.assertEqual(set(proc_ex.input_data(include_path=False)), {"lowered_duplicated_xyz.dat", "suffix.txt"})
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"suffix_lowered_duplicated_xyz.dat"}
        )

        proc_ex = self.get_process_execution(execution, "JOIN:COMBINE_FILES")
        self.check_process_execution(proc_ex, execution, False)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "JOIN:COMBINE_FILES")
        self.assertEqual(
            set(proc_ex.input_data(include_path=False)),
            {"suffix_lowered_duplicated_abc.dat", "suffix_lowered_duplicated_xyz.dat"}
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"combined.txt"}
        )
    

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


