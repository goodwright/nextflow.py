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
    

    @property
    def config_string(self):
        full_config_path = os.path.abspath(self.config) if self.config else ""
        return f" -C {full_config_path}" if self.config else ""
    

    def run(self, location=".", params=None):
        full_run_location = os.path.abspath(location)
        full_pipeline_location = os.path.abspath(self.path)
        original_location = os.getcwd()
        param_string = " ".join([
            f"--{param[0]}={param[1]}" for param in params.items()
        ]) if params else ""
        try:
            config_string = self.config_string
            os.chdir(full_run_location)
            process = subprocess.run(
                f"nextflow{config_string} run {full_pipeline_location} {param_string}",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, shell=True, cwd=full_run_location
            )
        finally: os.chdir(original_location)
        with open(os.path.join(full_run_location, ".nextflow.log")) as f:
            log_text = f.read()
        run_id = re.search(r"\[([a-z]+_[a-z]+)\]", log_text)[1]
        return Execution(full_run_location, run_id, process=process)
