

def run(pipeline, configs=None, params=None, profiles=None, version=None, run_location=".", execution_location=None, work_location=None, log_location=None):
    pass

    # Set locations not provided
    if execution_location is None: execution_location = run_location
    if work_location is None: work_location = execution_location
    if log_location is None: log_location = execution_location

    # What is the command to run

    # Run command in run_location

    # Wait for execution to complete at execution_location

    # Create execution object using three locations