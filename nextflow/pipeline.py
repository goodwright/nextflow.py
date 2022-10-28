"""Tools for representing Nextflow pipelines."""

import os
import time
import subprocess
from .execution import Execution
from .utils import directory_is_ready, get_directory_id

class Pipeline:
    """A .nf file somewhere on the local filesystem.
    
    :param str path: the path to the .nf file.
    :param str config: the path to the associated config file.."""

    def __init__(self, path, config=None):
        self.path = path
        self.config = config
    

    def __repr__(self):
        return f"<Pipeline ({self.path})>"
    

    def config_string(self, extra_config=None):
        """Gets the full location of the config file as a command line
        argument.

        :param list extra_config: any additional config files to add in.
        :rtype: ``str``"""
        
        all_config = []
        if self.config: all_config.append(self.config)
        if extra_config: all_config += extra_config
        if not all_config: return ""
        all_config = [os.path.abspath(c) for c in all_config]
        return " " + " ".join(f"-c \"{c}\"" for c in all_config)
    

    def create_command_string(self, params, profile, version, extra_config=None):
        """Creates the full command line string to run for this pipeline.
        
        :param dict params: the parameters to pass at the command line.
        :param list profile: the names of profiles to use when running.
        :param str version: the Nextflow version to use.
        :param list extra_config: any additional config files to add in.
        :rtype: ``str``"""
        

        full_pipeline_location = os.path.abspath(self.path)
        param_string = " ".join([
            f"--{param[0]}='{param[1]}'" if param[1][0] not in "'\""
            else f"--{param[0]}={param[1]}" for param in params.items()
        ]) if params else ""
        profile_string = (" -profile " + ",".join(profile)) if profile else ""
        command_string = "NXF_ANSI_LOG=false "
        if version: command_string += f"NXF_VER={version} "
        command_string += f"nextflow -Duser.country=US{self.config_string(extra_config)} "
        command_string += f"run \"{full_pipeline_location}\" "
        command_string += f"{param_string}{profile_string}".strip()
        return command_string
    
    
    def run(self, location=".", params=None, profile=None, version=None, config=None):
        """Runs the pipeline and returns an :py:class:`.Execution` object once
        it is completed. You can specifiy where it will run, the command line
        parameters passed to it, the profile(s) it will run with, and the
        Nextflow version it will use.
        
        :param str location: the directory to run within and save outputs to.
        :param dict params: the parameters to pass at the command line.
        :param list profile: the names of profiles to use when running.
        :param str version: the Nextflow version to use.
        :param list config: any additional config files to use.
        :rtype: ``Execution``"""
        
        full_run_location = os.path.abspath(location)
        original_location = os.getcwd()
        command_string = self.create_command_string(params, profile, version, config)
        try:
            os.chdir(full_run_location)
            process = subprocess.run(
                command_string,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, shell=True, cwd=full_run_location
            )
        finally: os.chdir(original_location)
        return Execution.create_from_location(
            full_run_location, self, process.stdout, process.stderr, process.returncode
        )
    

    def run_and_poll(self, location=".", params=None, profile=None, version=None, config=None, sleep=5):
        """Runs the pipeline and creates :py:class:`.Execution` objects at
        intervals, returning them as a generator. You can specifiy where it will
        run, the command line parameters passed to it, the profile(s) it will
        run with, and the Nextflow version it will use.
        
        :param str location: the directory to run within and save outputs to.
        :param dict params: the parameters to pass at the command line.
        :param list profile: the names of profiles to use when running.
        :param str version: the Nextflow version to use.
        :param list config: any additional config files to use.
        :param int sleep: the amount of time between Execution updates.
        :rtype: ``Execution``"""
        
        full_run_location = os.path.abspath(location)
        original_location = os.getcwd()
        command_string = self.create_command_string(params, profile, version, config)
        try:
            os.chdir(full_run_location)
            existing_id = get_directory_id(full_run_location)
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
                        if directory_is_ready(full_run_location, existing_id):
                            yield Execution.create_from_location(
                                full_run_location, self, out, err, returncode
                            )
                        if returncode is not None: break
        finally:
            if os.path.exists("nfstdout"): os.remove("nfstdout")
            if os.path.exists("nfstderr"): os.remove("nfstderr")
            os.chdir(original_location)



def run(pipeline, *args, **kwargs):
    """Runs a pipeline by pathand returns an :py:class:`.Execution` object once
    it is completed. You can specifiy where it will run, the command line
    parameters passed to it, the profile(s) it will run with, and the
    Nextflow version it will use.
    
    :param str path: the path to the .nf file.
    :param str location: the directory to run within and save outputs to.
    :param dict params: the parameters to pass at the command line.
    :param list profile: the names of profiles to use when running.
    :param list config: any config files to use.
    :param str version: the Nextflow version to use.
    :rtype: ``Execution``"""

    pipeline = Pipeline(path=pipeline)
    return pipeline.run(*args, **kwargs)


def run_and_poll(pipeline, *args, **kwargs):
    """Runs a pipeline by path and creates :py:class:`.Execution` objects at
    intervals, returning them as a generator. You can specifiy where it will
    run, the command line parameters passed to it, the profile(s) it will
    run with, and the Nextflow version it will use.
    
    :param str path: the path to the .nf file.
    :param str config: the path to the associated config file.
    :param str location: the directory to run within and save outputs to.
    :param dict params: the parameters to pass at the command line.
    :param list profile: the names of profiles to use when running.
    :param list config: any config files to use.
    :param str version: the Nextflow version to use.
    :param int sleep: the amount of time between Execution updates.
    :rtype: ``Execution``"""

    pipeline = Pipeline(path=pipeline)
    return pipeline.run_and_poll(*args, **kwargs)
