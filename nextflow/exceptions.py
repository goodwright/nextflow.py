class NextflowNotInstalledError(Exception):
    """Error raised if nextflow.py is imported but there is no Nextflow
    executable on the system."""