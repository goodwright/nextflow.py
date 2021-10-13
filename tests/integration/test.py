import os
from unittest import TestCase
import nextflow

class Tests(TestCase):

    def test(self):
        pipeline = nextflow.Pipeline(os.path.join("tests", "integration", "pipelines", "pipeline.nf"))
        self.assertEqual(pipeline.path, os.path.join("tests", "integration", "pipelines", "pipeline.nf"))
        self.assertIsNone(pipeline.config)
        self.assertIsNone(pipeline.schema)