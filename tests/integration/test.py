import os
import shutil
from unittest import TestCase
import nextflow

class PipelineTest(TestCase):

    def get_path(self, name):
        return os.path.join(
            ".", "tests", "integration", "pipelines", name.replace("/", os.path.sep)
        )

        

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
    

    def test_config(self):
        pipeline = nextflow.Pipeline(
            self.get_path("pipeline.nf"),
            config=self.get_path("custom.config")
        )
        self.assertEqual(pipeline.path, self.get_path("pipeline.nf"))
        self.assertIsNone(pipeline.schema)
        self.assertEqual(pipeline.config, self.get_path("custom.config"))



class PipelineRunningTests(PipelineTest):

    def setUp(self):
        self.rundirectory = self.get_path("rundirectory")
        if os.path.exists(self.rundirectory): shutil.rmtree(self.rundirectory)
        os.mkdir(self.rundirectory)
    

    def tearDown(self):
        shutil.rmtree(self.rundirectory)
        if os.path.exists(".nextflow"): shutil.rmtree(".nextflow")


    def test_pipeline_running(self):
        # Run pipeline that doesn't need any inputs
        pipeline = nextflow.Pipeline(self.get_path("pipeline.nf"))
        execution = pipeline.run(location=self.get_path("rundirectory"))
        self.assertIn(".nextflow", os.listdir(os.path.join(self.get_path("rundirectory"))))
        self.assertIn(".nextflow.log", os.listdir(os.path.join(self.get_path("rundirectory"))))

        # Examine resultant execution
        self.assertIn("_", execution.id)
        self.assertEqual(execution.status, "OK")
        self.assertEqual(execution.command, f"nextflow run {os.path.abspath(self.get_path('pipeline.nf'))}\n")

        # Examine original process
        self.assertEqual(execution.process.returncode, 0)
        self.assertEqual(execution.process.stderr, "")
        self.assertTrue(execution.process.stdout.startswith("N E X T F L O W"))
        self.assertIn(f"[{execution.id}]", execution.process.stdout)
    

    def test_pipeline_running_with_inputs(self):
        # Run command with inputs
        pipeline = nextflow.Pipeline(self.get_path("pipeline.nf"))
        execution = pipeline.run(location=self.get_path("rundirectory"), params={
            "param1": "xxx", "param2": "/path/to/file"
        })
        self.assertIn(".nextflow", os.listdir(os.path.join(self.get_path("rundirectory"))))
        self.assertIn(".nextflow.log", os.listdir(os.path.join(self.get_path("rundirectory"))))

        # Examine resultant execution
        self.assertIn("_", execution.id)
        self.assertEqual(execution.status, "OK")
        self.assertEqual(
            execution.command,
            f"nextflow run {os.path.abspath(self.get_path('pipeline.nf'))} --param1=xxx --param2=/path/to/file\n"
        )
    

    def test_pipeline_running_with_custom_config(self):
        pipeline = nextflow.Pipeline(self.get_path("pipeline.nf"), config=self.get_path("custom.config"))
        execution = pipeline.run(location=self.get_path("rundirectory"))
        self.assertIn(".nextflow", os.listdir(os.path.join(self.get_path("rundirectory"))))
        self.assertIn(".nextflow.log", os.listdir(os.path.join(self.get_path("rundirectory"))))

        # Examine resultant execution
        self.assertIn("_", execution.id)
        self.assertEqual(execution.status, "OK")
        self.assertEqual(
            execution.command,
            f"nextflow -C {os.path.abspath(self.get_path('custom.config'))} run {os.path.abspath(self.get_path('pipeline.nf'))}\n"
        )