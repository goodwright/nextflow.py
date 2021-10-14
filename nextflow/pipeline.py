import json
import os
import re
import subprocess
from .execution import Execution

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
        full_run_location = os.path.abspath(location)
        full_pipeline_location = os.path.abspath(self.path)
        original_location = os.getcwd()
        try:
            os.chdir(full_run_location)
            process = subprocess.run(
                f"nextflow run {full_pipeline_location}",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, shell=True, cwd=full_run_location
            )
        finally: os.chdir(original_location)
        with open(os.path.join(full_run_location, ".nextflow.log")) as f:
            log_text = f.read()
        run_id = re.search(r"\[([a-z]+_[a-z]+)\]", log_text)[1]
        return Execution(full_run_location, run_id)
