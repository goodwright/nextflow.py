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
    

    def test_schema(self):
        pipeline = nextflow.Pipeline(
            self.get_path("pipeline.nf"),
            schema=self.get_path("schema.json")
        )
        self.assertEqual(pipeline.path, self.get_path("pipeline.nf"))
        self.assertIsNone(pipeline.config)
        self.assertEqual(pipeline.schema, self.get_path("schema.json"))
    

    def test_config(self):
        pipeline = nextflow.Pipeline(
            self.get_path("pipeline.nf"),
            config=self.get_path("nextflow.config")
        )
        self.assertEqual(pipeline.path, self.get_path("pipeline.nf"))
        self.assertIsNone(pipeline.schema)
        self.assertEqual(pipeline.config, self.get_path("nextflow.config"))