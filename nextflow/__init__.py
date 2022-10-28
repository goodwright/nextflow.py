from shutil import which
from .exceptions import NextflowNotInstalledError
from .execution import Execution
from .pipeline import Pipeline, run, run_and_poll

__author__ = "Sam Ireland"
__version__ = "0.5.0"

if not which("nextflow"):
    raise NextflowNotInstalledError(
        "Nextflow is either not installed, not in PATH, or is not executable."
    )