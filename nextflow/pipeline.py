import json
import os
import time
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
        """The input JSON from the associated schema file."""

        if not self.schema: return None
        with open(self.schema) as f:
            schema = json.load(f)
        return schema["definitions"]
    

    @property
    def config_string(self):
        """Gets the full location of the config file as a command line
        argument."""
        
        if not self.config: return ""
        full_config_path = os.path.abspath(self.config)
        return f" -C \"{full_config_path}\""
    

    def create_command_string(self, params, profile):
        """Creates the full command line string to run for this pipeline."""
        
        full_pipeline_location = os.path.abspath(self.path)
        param_string = " ".join([
            f"--{param[0]}='{param[1]}'" if param[1][0] not in "'\""
            else f"--{param[0]}={param[1]}" for param in params.items()
        ]) if params else ""
        profile_string = (" -profile " + ",".join(profile)) if profile else ""
        command_string = f"nextflow{self.config_string} "
        command_string += f"run \"{full_pipeline_location}\" "
        command_string += f"{param_string}{profile_string}"
        return command_string

    
    def run(self, location=".", params=None, profile=None):
        """Runs the pipeline."""
        
        full_run_location = os.path.abspath(location)
        original_location = os.getcwd()
        command_string = self.create_command_string(params, profile)
        try:
            os.chdir(full_run_location)
            process = subprocess.run(
                command_string,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, shell=True, cwd=full_run_location
            )
        finally: os.chdir(original_location)
        return Execution.create_from_location(
            full_run_location, process.stdout, process.stderr, process.returncode, True
        )
    

    def run_and_poll(self, location=".", params=None, profile=None, sleep=5):
        """Runs the pipeline as creates executions at intervals, returning them
        as a generator."""
        
        full_run_location = os.path.abspath(location)
        original_location = os.getcwd()
        command_string = self.create_command_string(params, profile)
        try:
            os.chdir(full_run_location)
            process = subprocess.Popen(
                command_string,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, shell=True, cwd=full_run_location,
            )
            while True:
                time.sleep(sleep)
                returncode = process.poll()
                out, err = "", ""
                if returncode is not None: out, err = process.communicate()
                yield Execution.create_from_location(
                    full_run_location, out, err, returncode,
                    use_nextflow=not (returncode is None)
                )
                if returncode is not None: break
        finally: os.chdir(original_location)
