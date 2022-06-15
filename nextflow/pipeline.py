"""Tools for representing Nextflow pipelines."""

import json
import os
import time
import subprocess
from .execution import Execution

class Pipeline:
    """A .nf file somewhere on the local filesystem.
    
    :param str path: the path to the .nf file.
    :param str config: the path to the associated config file.
    :param str schema: the path to the associated JSON schema file."""

    def __init__(self, path, config=None, schema=None):
        self.path = path
        self.config = config
        self.schema = schema
    

    def __repr__(self):
        return f"<Pipeline ({self.path})>"
    

    @property
    def input_schema(self):
        """The input JSON from the associated schema file.
        
        :rtype: ``dict``"""

        if not self.schema: return None
        with open(self.schema) as f:
            schema = json.load(f)
        return schema["definitions"]
    

    @property
    def config_string(self):
        """Gets the full location of the config file as a command line
        argument.
        
        :rtype: ``str``"""
        
        if not self.config: return ""
        full_config_path = os.path.abspath(self.config)
        return f" -C \"{full_config_path}\""
    

    def create_command_string(self, params, profile, version):
        """Creates the full command line string to run for this pipeline.
        
        :param dict params: the parameters to pass at the command line.
        :param list profile: the names of profiles to use when running.
        :param str version: the Nextflow version to use.
        :rtype: ``str``"""
        
        full_pipeline_location = os.path.abspath(self.path)
        param_string = " ".join([
            f"--{param[0]}='{param[1]}'" if param[1][0] not in "'\""
            else f"--{param[0]}={param[1]}" for param in params.items()
        ]) if params else ""
        profile_string = (" -profile " + ",".join(profile)) if profile else ""
        command_string = "NXF_ANSI_LOG=false "
        if version: command_string += f"NXF_VER={version} "
        command_string += f"nextflow{self.config_string} "
        command_string += f"run \"{full_pipeline_location}\" "
        command_string += f"{param_string}{profile_string}"
        return command_string

    
    def run(self, location=".", params=None, profile=None, version=None):
        """Runs the pipeline and returns an :py:class:`.Execution` object once
        it is completed. You can specifiy where it will run, the command line
        parameters passed to it, the profile(s) it will run with, and the
        Nextflow version it will use.
        
        :param str location: the directory to run within and save outputs to.
        :param dict params: the parameters to pass at the command line.
        :param list profile: the names of profiles to use when running.
        :param str version: the Nextflow version to use.
        :rtype: ``Execution``"""
        
        full_run_location = os.path.abspath(location)
        original_location = os.getcwd()
        command_string = self.create_command_string(params, profile, version)
        try:
            os.chdir(full_run_location)
            process = subprocess.run(
                command_string,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, shell=True, cwd=full_run_location
            )
        finally: os.chdir(original_location)
        return Execution.create_from_location(
            full_run_location, process.stdout, process.stderr, process.returncode
        )
    

    def run_and_poll(self, location=".", params=None, profile=None, version=None, sleep=5):
        """Runs the pipeline and creates :py:class:`.Execution` objects at
        intervals, returning them as a generator. You can specifiy where it will
        run, the command line parameters passed to it, the profile(s) it will
        run with, and the Nextflow version it will use.
        
        :param str location: the directory to run within and save outputs to.
        :param dict params: the parameters to pass at the command line.
        :param list profile: the names of profiles to use when running.
        :param str version: the Nextflow version to use.
        :param int sleep: the amount of time between Execution updates.
        :rtype: ``Execution``"""
        
        full_run_location = os.path.abspath(location)
        original_location = os.getcwd()
        command_string = self.create_command_string(params, profile, version)
        try:
            os.chdir(full_run_location)
            with open("nfstdout", "w") as fout:
                with open("nfstderr", "w") as ferr:
                    process = subprocess.Popen(
                        command_string, stdout=fout, stderr=ferr,
                        universal_newlines=True, shell=True,
                        cwd=full_run_location,
                    )
                    while True:
                        time.sleep(sleep)
                        returncode = process.poll()
                        with open("nfstdout") as f: out = f.read()
                        with open("nfstderr") as f: err = f.read()
                        if os.path.exists(os.path.join(full_run_location, ".nextflow.log")):
                            if os.path.exists(os.path.join(full_run_location, ".nextflow", "history")):
                                yield Execution.create_from_location(
                                    full_run_location, out, err, returncode
                                )
                        if returncode is not None: break
        finally:
            if os.path.exists("nfstdout"): os.remove("nfstdout")
            if os.path.exists("nfstderr"): os.remove("nfstderr")
            os.chdir(original_location)



def run(pipeline, config, *args, **kwargs):
    """Runs a pipeline by pathand returns an :py:class:`.Execution` object once
    it is completed. You can specifiy where it will run, the command line
    parameters passed to it, the profile(s) it will run with, and the
    Nextflow version it will use.
    
    :param str path: the path to the .nf file.
    :param str config: the path to the associated config file.
    :param str location: the directory to run within and save outputs to.
    :param dict params: the parameters to pass at the command line.
    :param list profile: the names of profiles to use when running.
    :param str version: the Nextflow version to use.
    :rtype: ``Execution``"""

    pipeline = Pipeline(path=pipeline, config=config)
    return pipeline.run(*args, **kwargs)


def run_and_poll(pipeline, config, *args, **kwargs):
    """Runs a pipeline by path and creates :py:class:`.Execution` objects at
    intervals, returning them as a generator. You can specifiy where it will
    run, the command line parameters passed to it, the profile(s) it will
    run with, and the Nextflow version it will use.
    
    :param str path: the path to the .nf file.
    :param str config: the path to the associated config file.
    :param str location: the directory to run within and save outputs to.
    :param dict params: the parameters to pass at the command line.
    :param list profile: the names of profiles to use when running.
    :param str version: the Nextflow version to use.
    :param int sleep: the amount of time between Execution updates.
    :rtype: ``Execution``"""

    pipeline = Pipeline(path=pipeline, config=config)
    return pipeline.run_and_poll(*args, **kwargs)
