"""Utilities for parsing Nextflow's log outputs."""

import os
import re
from pathlib import Path
from datetime import datetime

def get_directory_id(path):
    """Gets the ID of the most recent execution in a given directory.
    
    :param str path: the path to the directory.
    :rtype: ``str``"""
    
    log_path = Path(path, ".nextflow.log")
    if not os.path.exists(log_path): return None
    with open(log_path) as f:
        log_text = f.read()
    run_id = re.search(r"\[([a-z]+_[a-z]+)\]", log_text)
    if not run_id: return None
    return run_id[1]
    

def directory_is_ready(path, existing_id=None):
    """Takes the path to a directory that should contain an execution, and
    checks if it is ready to be parsed. You can supply an execution ID that
    should be considered invalid if found.
    
    :param str path: the path to the directory.
    :param str existing_id: an ID to reject if found.
    :rtype: ``bool``"""
    
    log_path = Path(path, ".nextflow.log")
    if not os.path.exists(log_path): return False
    if not os.path.exists(Path(path, ".nextflow", "history")): return False
    with open(log_path) as f:
        log_text = f.read()
    run_id = re.search(r"\[([a-z]+_[a-z]+)\]", log_text)
    if not run_id: return False
    if existing_id and run_id[1] == existing_id: return False
    return True


def parse_datetime(dt):
    """Gets a Python datetime from a Nextflow datetime string. This can be with
    or without milliseconds.
    
    :param string dt: a datetime string in Nextflow format.
    :rtype: ``datetime``"""

    if len(dt) > 19:
        return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
    else:
        return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")


def parse_duration(duration):
    """Gets a duration in seconds from a Nextflow duration string.

    :param string duration: a string of the form '2.1s', '5m', '540ms' etc.
    :rtype: ``float``"""

    if duration == "-": return 0
    if " " in duration:
        values = duration.split()
        return sum(parse_duration(v) for v in values)
    elif duration.endswith("ms"):
        return float(duration[:-2]) / 1000
    elif duration.endswith("s"):
        return float(duration[:-1])
    elif duration.endswith("m"):
        return float(duration[:-1]) * 60
    elif duration.endswith("h"):
        return float(duration[:-1]) * 3600


def get_process_name_from_log(log, id):
    """Gets a process's name from the .nextflow.log file, given its unique ID.
    
    :param str log: The text of the log file.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``str``"""

    escaped_id = id.replace('/', '\\/')
    match = re.search(f"\\[{escaped_id}\\] Submitted process > (.+)", log)
    if match: return match[1]


def get_process_start_from_log(log, id):
    """Gets a process's start time from the .nextflow.log file as a string,
    given its unique ID.
    
    :param str log: The text of the log file.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``str``"""

    escaped_id = id.replace('/', '\\/')
    match = re.search(
        f"(...-\d+ \d+:\d+:\d+\.\d+).+\\[{escaped_id}\\] Submitted process", log
    )
    if match: return match[1]


def get_process_end_from_log(log, id):
    """Gets a process's end time from the .nextflow.log file as a datetime,
    given its unique ID, if it has finshed. If it hasn't finished, the current
    datetime is returned.
    
    :param str log: The text of the log file.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``datetime``"""

    escaped_id = id.replace('/', '\\/')
    match = re.search(
        f"(...-\d+ \d+:\d+:\d+\.\d+).+Task completed.+{escaped_id}", log
    )
    if not match: return datetime.now()
    return datetime.strptime(
        f"{str(datetime.now().year)}-{match[1]}",
        "%Y-%b-%d %H:%M:%S.%f"
    )


def get_process_status_from_log(log, id):
    """Gets a process's status (COMPLETED, ERROR etc.) from the .nextflow.log
    file, given its unique ID. If it's still running, '-' is returned.
    
    :param str log: The text of the log file.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``string``"""

    escaped_id = id.replace('/', '\\/')
    match = re.search(
        f".+Task completed.+status: ([A-Z]+).+exit: (\d+).+{escaped_id}", log
    )
    return ("FAILED" if match[2] != "0" else match[1]) if match else "-"


def get_process_stdout(execution, id):
    """Gets a process's stdout text, given its parent execution and its unique
    ID. If it can't be obtained, '-' is returned.
    
    :param Execution execution: The parent Execution object.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``string``"""

    location = get_process_directory(execution, id)
    try:
        with open(os.path.join(location, ".command.out")) as f:
            return f.read()
    except: return "-"


def get_process_stderr(execution, id):
    """Gets a process's stderr text, given its parent execution and its unique
    ID. If it can't be obtained, '-' is returned.
    
    :param Execution execution: The parent Execution object.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``string``"""

    location = get_process_directory(execution, id)
    try:
        with open(os.path.join(location, ".command.err")) as f:
            return f.read()
    except: return "-"


def get_process_bash(execution, id):
    """Gets a process's bash script text, given its parent execution and its
    unique ID. If it can't be obtained, '-' is returned.
    
    :param Execution execution: The parent Execution object.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``string``"""

    location = get_process_directory(execution, id)
    try:
        with open(os.path.join(location, ".command.sh")) as f:
            return f.read()
    except: return "-"


def get_process_returncode(execution, id):
    """Gets a process's returncode (typically 0 or 1), given its parent
    execution and its unique ID. If it can't be obtained, an empty string is
    returned.
    
    :param Execution execution: The parent Execution object.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``string``"""

    location = get_process_directory(execution, id)
    try:
        with open(os.path.join(location, ".exitcode")) as f:
            return f.read()
    except: return ""


def get_process_directory(execution, id):
    """Gets a process's work directory, given its parent execution and its
    unique ID.
    
    :param Execution execution: The parent Execution object.
    :param str id: The process's ID (eg. '1a/234bcd').
    :rtype: ``string``"""

    first, second = id.split("/")
    options = os.listdir(os.path.join(execution.location, "work", first))
    second = [option for option in options if option.startswith(second)]
    if not second: return
    return os.path.join(execution.location, "work", first, second[0])