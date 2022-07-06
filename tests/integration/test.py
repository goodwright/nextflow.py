import os
import re
import time
import shutil
from datetime import datetime
from unittest import TestCase
import nextflow

class PipelineTest(TestCase):

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
    

    def check_process_execution(self, process_execution, execution, long):
        self.assertGreaterEqual(len(process_execution.started_string), 10)
        self.assertEqual(process_execution.started_dt.year, datetime.now().year)
        self.assertLessEqual(time.time() - process_execution.started, 30 if long else 5)
        self.assertGreater(process_execution.duration, 0)
        self.assertLessEqual(process_execution.duration, 6)
        self.assertEqual(process_execution.status, "COMPLETED")
        self.assertEqual(process_execution.returncode, "0")
        self.assertIs(process_execution.execution, execution)
    

    def get_process_execution(self, execution, name):
        return [e for e in execution.process_executions if e.name == name][0]

    
    def check_execution(self, execution, long=False, check_stderr=True):
        # Execution is fine
        self.assertIn(".nextflow", os.listdir(self.get_path("rundirectory")))
        self.assertIn(".nextflow.log", os.listdir(self.get_path("rundirectory")))
        self.assertEqual(execution.location, self.get_path("rundirectory"))
        self.assertTrue(re.match(r"[a-z]+_[a-z]+", execution.id))
        self.assertIn("N E X T F L O W", execution.stdout)
        if check_stderr: self.assertFalse(execution.stderr)
        self.assertEqual(execution.returncode, 0)
        self.assertLessEqual(time.time() - execution.started, 30 if long else 5)
        self.assertEqual(execution.started_dt.year, datetime.now().year)
        self.assertIn(str(datetime.now().year), execution.started_string)
        self.assertGreaterEqual(len(execution.started_string), 13)
        self.assertLessEqual(execution.duration, 30 if long else 5)
        self.assertEqual(execution.status, "OK")
        log = execution.log
        self.assertIn("Starting process", log)
        self.assertIn("Execution complete -- Goodbye", log)
        self.assertEqual(len(execution.process_executions), 8)

        # Process executions are fine
        proc_ex = self.get_process_execution(execution, "SPLIT_FILE")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "Splitting...\n")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "SPLIT_FILE")
        self.assertEqual(proc_ex.input_data(), [self.get_path("files/data.txt")])
        self.assertEqual(proc_ex.input_data(include_path=False), ["data.txt"])
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)),
            {"abc.dat", "xyz.dat", "log.txt"}
        )
        self.assertIn(proc_ex.hash, proc_ex.all_output_data()[0])

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE")
        self.assertEqual(proc_ex.input_data(include_path=False), ["abc.dat"])
        self.assertIn(
            self.get_process_execution(execution, "SPLIT_FILE").hash,
            proc_ex.input_data()[0]
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE")
        self.assertEqual(proc_ex.input_data(include_path=False), ["xyz.dat"])
        self.assertIn(
            self.get_process_execution(execution, "SPLIT_FILE").hash,
            proc_ex.input_data()[0]
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_abc.dat)")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER")
        self.assertEqual(proc_ex.input_data(include_path=False), ["duplicated_abc.dat"])
        self.assertIn(
            self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)").hash,
            proc_ex.input_data()[0]
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_xyz.dat)")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER")
        self.assertEqual(proc_ex.input_data(include_path=False), ["duplicated_xyz.dat"])
        self.assertIn(
            self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)").hash,
            proc_ex.input_data()[0]
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:APPEND (lowered_duplicated_abc.dat)")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:APPEND")
        self.assertEqual(set(proc_ex.input_data(include_path=False)), {"lowered_duplicated_abc.dat", "suffix.txt"})

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:APPEND (lowered_duplicated_xyz.dat)")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:APPEND")
        self.assertEqual(set(proc_ex.input_data(include_path=False)), {"lowered_duplicated_xyz.dat", "suffix.txt"})

        proc_ex = self.get_process_execution(execution, "JOIN:COMBINE_FILES")
        self.check_process_execution(proc_ex, execution, long)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "JOIN:COMBINE_FILES")
        self.assertEqual(
            set(proc_ex.input_data(include_path=False)),
            {"suffix_lowered_duplicated_abc.dat", "suffix_lowered_duplicated_xyz.dat"}
        )

        # Config was used
        self.assertIn("split_file", os.listdir(self.get_path("rundirectory/results")))



class DirectRunningTests(PipelineTest):

    def test_can_run_pipeline_directly(self):
        execution = nextflow.run(
            pipeline=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            },
            location=self.get_path("rundirectory")
        )
        self.check_execution(execution)
    

    def test_can_run_pipeline_directly_and_poll(self):
        ids = []
        returncodes = []
        process_executions = []
        for execution in nextflow.run_and_poll(
            pipeline=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
            sleep=3,
            profile=["special"],
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt"), "wait": "5"
            },
            location=self.get_path("rundirectory")
        ):
            self.assertEqual(execution.location, self.get_path("rundirectory"))
            returncodes.append(execution.returncode)
            ids.append(execution.id)
            self.assertGreater(len(execution.stdout), 0)
            process_executions.append(execution.process_executions)
        self.assertEqual(len(set(ids)), 1)
        for a, b in zip(ids[:-1], ids[1:]):
            self.assertGreaterEqual(b, a)
        for processes1, processes2 in zip(process_executions[:-1], process_executions[1:]):
            names1 = [p.process for p in processes1]
            names2 = [p.process for p in processes2]
            for name in names1:
                self.assertIn(name, names2)

            hashes1 = [p.hash for p in processes1]
            hashes2 = [p.hash for p in processes2]
            for hash_ in hashes1:
                self.assertIn(hash_, hashes2)

            for process in processes1:
                self.assertIn(process.status, ["-", "COMPLETED"])
            for process in processes2:
                self.assertIn(process.status, ["-", "COMPLETED"])

            for process in processes1:
                duration = process.duration
                for process2 in processes2:
                    if process2.name == process.name:
                        self.assertLessEqual(duration, process2.duration + 0.1)
        self.assertEqual(returncodes[-1], 0)
        self.assertEqual(set(returncodes[:-1]), {None})
        self.check_execution(execution, long=True)
        self.assertIn("Applying config profile: `special`", execution.log)
    

    def test_can_run_pipeline_directly_with_specific_version(self):
        execution = nextflow.run(
            pipeline=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            },
            location=self.get_path("rundirectory"),
            version="21.10.3"
        )
        self.check_execution(execution, check_stderr=False)
        self.assertIn("21.10.3", execution.log)
    

    def test_can_handle_pipeline_error(self):
        execution = nextflow.run(
            pipeline=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
            params={
                "input": self.get_path("files/data.txt"), "count": "string",
                "suffix": self.get_path("files/suffix.txt")
            },
            location=self.get_path("rundirectory"),
        )
        self.assertEqual(execution.status, "ERR")
        self.assertEqual(execution.returncode, 1)
        self.assertIn("Error executing process", execution.stdout)
        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.assertEqual(proc_ex.status, "FAILED")
        self.assertEqual(proc_ex.returncode, "1")
        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.assertEqual(proc_ex.status, "FAILED")
        self.assertEqual(proc_ex.returncode, "1")



class PipelineRunningTests(PipelineTest):

    def test_can_run_pipeline(self):
        pipeline = nextflow.Pipeline(
            path=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
        )
        self.assertEqual(pipeline.get_module_paths(), {
            "JOIN:COMBINE_FILES": self.get_path("modules/combine_files.nf"),
            "PROCESS_DATA:APPEND": self.get_path("modules/append.nf"),
            "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE": self.get_path("modules/duplicate.nf"),
            "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER": self.get_path("modules/lower.nf"),
            "SPLIT_FILE": self.get_path("modules/split_file.nf"),
        })
        execution = pipeline.run(
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            },
            location=self.get_path("rundirectory")
        )
        self.check_execution(execution)
    

    def test_can_run_pipeline_and_poll(self):
        pipeline = nextflow.Pipeline(
            path=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
        )
        ids = []
        returncodes = []
        process_executions = []
        for execution in pipeline.run_and_poll(
            sleep=3,
            params={
                "input": self.get_path("files/data.txt"), "count": "12",
                "suffix": self.get_path("files/suffix.txt")
            },
            location=self.get_path("rundirectory")
        ):
            self.assertEqual(execution.location, self.get_path("rundirectory"))
            returncodes.append(execution.returncode)
            ids.append(execution.id)
            process_executions.append(execution.process_executions)
        self.assertEqual(len(set(ids)), 1)
        for a, b in zip(ids[:-1], ids[1:]):
            self.assertGreaterEqual(b, a)