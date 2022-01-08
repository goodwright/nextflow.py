from shutil import which
from .exceptions import NextflowNotInstalledError
from .execution import Execution
from .pipeline import Pipeline

__author__ = "Sam Ireland"
__version__ = "0.1.4"

if not which("nextflow"):
    raise NextflowNotInstalledError(
        "Nextflow is either not installed, not in PATH, or is not executable."
    )