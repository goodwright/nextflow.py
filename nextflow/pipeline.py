import json
import subprocess

class Pipeline:
    """A .nf file somewhere on the local filesystem."""

    def __init__(self, path, config=None, schema=None):
        self.path = path
        self.config = config
        self.schema = schema
    

    def __repr__(self):
        return f"<Pipeline ({self.path})>"
    

    @property
    def input_schema(self):
        if not self.schema: return None
        with open(self.schema) as f:
            schema = json.load(f)
        return schema["definitions"]
    

    def run(self, location="."):
        process = subprocess.run(
            f"nextflow run {self.path}",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd=location
        )