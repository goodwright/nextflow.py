from shutil import which
from .exceptions import NextflowNotInstalledError
from .execution import Execution
from .pipeline import Pipeline

__author__ = "Sam Ireland"
__version__ = "0.3.0"

if not which("nextflow"):
    raise NextflowNotInstalledError(
        "Nextflow is either not installed, not in PATH, or is not executable."
    )


def run(pipeline, config, *args, **kwargs):
    pipeline = Pipeline(path=pipeline, config=config)
    return pipeline.run(*args, **kwargs)


def run_and_poll(pipeline, config, *args, **kwargs):
    pipeline = Pipeline(path=pipeline, config=config)
    return pipeline.run_and_poll(*args, **kwargs)