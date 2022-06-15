import os
import re
import time
import shutil
from datetime import datetime
from typing import Counter
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

        # Process executions are fine
        self.assertEqual(len(execution.process_executions), 5)
        for process in execution.process_executions:
            self.assertIn(process.stdout, ["", "Splitting...\n"])
            self.assertIn(process.stderr, ["", ":/\n"])
            self.assertIs(process.execution, execution)
            self.assertEqual(process.status, "COMPLETED")
            self.assertEqual(process.returncode, "0")
            self.assertIn(process.process, [
                "SPLIT_FILE",
                "PROCESS_DATA:COMBINE_LINES",
                "PROCESS_DATA:DUPLICATE_LINE",
            ])
            self.assertIn(process.name, [
                "SPLIT_FILE",
                "PROCESS_DATA:COMBINE_LINES (1)",
                "PROCESS_DATA:COMBINE_LINES (2)",
                "PROCESS_DATA:DUPLICATE_LINE (1)",
                "PROCESS_DATA:DUPLICATE_LINE (2)"
            ])
            self.assertGreaterEqual(len(process.started_string), 10)
            self.assertEqual(process.started_dt.year, datetime.now().year)
            self.assertLessEqual(time.time() - process.started, 30 if long else 5)
            self.assertGreater(process.duration, 0)
            self.assertLessEqual(process.duration, 6)

        # Config was used
        self.assertIn("split_file", os.listdir(self.get_path("rundirectory/results")))
        self.assertIn("abc.dat", os.listdir(self.get_path("rundirectory/results/split_file")))
        self.assertIn("xyz.dat", os.listdir(self.get_path("rundirectory/results/split_file")))
        self.assertIn("combine_lines", os.listdir(self.get_path("rundirectory/results")))
        self.assertIn("combined.txt", os.listdir(self.get_path("rundirectory/results/combine_lines")))
        self.assertIn("duplicate_line", os.listdir(self.get_path("rundirectory/results")))
        self.assertIn("duplicated.txt", os.listdir(self.get_path("rundirectory/results/duplicate_line")))



class DirectRunningTests(PipelineTest):

    def test_can_run_pipeline_directly(self):
        execution = nextflow.run(
            pipeline=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
            params={"file": self.get_path("data.txt"), "count": "12"},
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
            params={"file": self.get_path("data.txt"), "count": "12", "wait": "5"},
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
                        self.assertLessEqual(duration, process2.duration)
        self.assertEqual(returncodes[-1], 0)
        self.assertEqual(set(returncodes[:-1]), {None})
        self.check_execution(execution, long=True)
        self.assertIn("Applying config profile: `special`", execution.log)
    

    def test_can_run_pipeline_directly_with_specific_version(self):
        execution = nextflow.run(
            pipeline=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
            params={"file": self.get_path("data.txt"), "count": "12"},
            location=self.get_path("rundirectory"),
            version="21.10.3"
        )
        self.check_execution(execution, check_stderr=False)
        self.assertIn("21.10.3", execution.log)
    

    def test_can_handle_pipeline_error(self):
        execution = nextflow.run(
            pipeline=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
            params={"file": self.get_path("data.txt"), "count": "string"},
            location=self.get_path("rundirectory"),
        )
        self.assertEqual(execution.status, "ERR")
        self.assertEqual(execution.returncode, 1)
        self.assertIn("Error executing process", execution.stdout)
        self.assertIn(
            Counter([p.status for p in execution.process_executions]),
            [{"COMPLETED": 3, "FAILED": 2}, {"COMPLETED": 3, "FAILED": 1, "-": 1}]
        )
        self.assertTrue(
            any([p.returncode == "1" for p in execution.process_executions])
        )



class PipelineRunningTests(PipelineTest):

    def test_can_run_pipeline(self):
        pipeline = nextflow.Pipeline(
            path=self.get_path("pipeline.nf"),
            config=self.get_path("pipeline.config"),
        )
        execution = pipeline.run(
            params={"file": self.get_path("data.txt"), "count": "12"},
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
            params={"file": self.get_path("data.txt"), "count": "12", "wait": "5"},
            location=self.get_path("rundirectory")
        ):
            self.assertEqual(execution.location, self.get_path("rundirectory"))
            returncodes.append(execution.returncode)
            ids.append(execution.id)
            process_executions.append(execution.process_executions)
        self.assertEqual(len(set(ids)), 1)
        for a, b in zip(ids[:-1], ids[1:]):
            self.assertGreaterEqual(b, a)



class PipelineIntrospectionTests(PipelineTest):

    def test_basic_pipeline(self):
        pipeline = nextflow.Pipeline(self.get_path("pipeline.nf"))
        self.assertEqual(pipeline.path, self.get_path("pipeline.nf"))
        self.assertIsNone(pipeline.config)
        self.assertIsNone(pipeline.schema)
        self.assertIsNone(pipeline.input_schema)
    

    def test_schema(self):
        pipeline = nextflow.Pipeline(
            self.get_path("pipeline.nf"),
            schema=self.get_path("schema.json")
        )
        self.assertEqual(pipeline.path, self.get_path("pipeline.nf"))
        self.assertIsNone(pipeline.config)
        self.assertEqual(pipeline.schema, self.get_path("schema.json"))
        self.assertEqual(pipeline.input_schema, {
            "ultraplex_options": {
                "title": "Input/output options",
                "type": "object",
                "fa_icon": "fas fa-terminal",
                "description": "Ultraplex options.",
                "properties": {
                    "input": {
                        "type": "string",
                        "format": "file-path",
                        "mimetype": "text/csv",
                        "pattern": "^\\S+\\.csv$",
                        "schema": "assets/schema_input.json",
                        "description": "Path to comma-separated file.",
                        "help_text": "You will need to create a design file.",
                        "fa_icon": "fas fa-file-csv"
                    },
                    "outdir": {
                        "type": "string",
                        "description": "Path to the output directory where the results will be saved.",
                        "default": "./results",
                        "fa_icon": "fas fa-folder-open"
                    }
                }
            },
            "spreadsheet_options": {
                "title": "UMI options",
                "type": "object",
                "description": "Options for processing reads with unique molecular identifiers",
                "default": "",
                "properties": {
                    "with_umi": {
                        "type": "boolean",
                        "fa_icon": "fas fa-barcode",
                        "description": "Enable UMI-based read deduplication."
                    },
                    "umitools_extract_method": {
                        "type": "string",
                        "default": "string",
                        "fa_icon": "fas fa-barcode",
                        "description": "UMI pattern to use. Can be either 'string' (default) or 'regex'.",
                        "help_text": "More details can be found in the UMI-tools documentation."
                    },
                    "save_umi_intermeds": {
                        "type": "boolean",
                        "fa_icon": "fas fa-save",
                        "description": "If this option is specified, all is good."
                    }
                },
                "fa_icon": "fas fa-barcode"
            }
        })

        

'''

    

    def test_running_with_profile(self):
        pipeline = nextflow.Pipeline(self.get_path("pipeline.nf"))
        execution = pipeline.run(location=self.get_path("rundirectory"), profile=["profile1,profile2"])
        self.assertIn(".nextflow", os.listdir(os.path.join(self.get_path("rundirectory"))))
        self.assertIn(".nextflow.log", os.listdir(os.path.join(self.get_path("rundirectory"))))

        # Examine resultant execution
        self.assertIn("_", execution.id)
        self.assertEqual(execution.status, "OK")
        self.assertEqual(execution.command, f"nextflow run {os.path.abspath(self.get_path('pipeline.nf'))} -profile profile1,profile2\n")



class PipelineProcessTests(PipelineTest):

    def test_can_get_process_info(self):
        pipeline = nextflow.Pipeline(self.get_path("pipeline.nf"))
        execution = pipeline.run(location=self.get_path("rundirectory"))
        
        processes = sorted(execution.process_executions, key=str)
        self.assertEqual(len(processes), 4)
        self.assertEqual(processes[0].process, "sayHello")
        self.assertEqual(processes[0].name, "sayHello (1)")
        self.assertIs(processes[0].execution, execution)
        self.assertTrue(processes[0].stdout.endswith(" world!\n"))'''