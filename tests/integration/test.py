import os
from unittest import TestCase
import nextflow

class Tests(TestCase):

    def get_path(self, name):
        return os.path.join("tests", "integration", "pipelines", name.replace("/", os.path.sep))


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
        self.assertEqual(pipeline.input_schema, [{
            "type": "string", "format": "file-path", "mimetype": "text/csv", "pattern": "^\\S+\\.csv$",
            "schema": "assets/schema_input.json",
            "description": "Path to comma-separated file.",
            "help_text": "You will need to create a design file.",
            "fa_icon": "fas fa-file-csv"
        }, {
            "type": "string", "description": "Path to the output directory where the results will be saved.",
            "default": "./results", "fa_icon": "fas fa-folder-open"
        }, {
            "type": "boolean", "fa_icon": "fas fa-barcode", "description": "Enable UMI-based read deduplication."
        }, {
            "type": "string", "default": "string", "fa_icon": "fas fa-barcode",
            "description": "UMI pattern to use. Can be either 'string' (default) or 'regex'.",
            "help_text": "More details can be found in the UMI-tools documentation."
        }, {
            "type": "boolean", "fa_icon": "fas fa-save",
            "description": "If this option is specified, all is good."
        }])
    

    def test_config(self):
        pipeline = nextflow.Pipeline(
            self.get_path("pipeline.nf"),
            config=self.get_path("nextflow.config")
        )
        self.assertEqual(pipeline.path, self.get_path("pipeline.nf"))
        self.assertIsNone(pipeline.schema)
        self.assertEqual(pipeline.config, self.get_path("nextflow.config"))