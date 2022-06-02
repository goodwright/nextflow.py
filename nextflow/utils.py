import os
import re
from datetime import datetime

def parse_datetime(dt):
    """Gets a UNIX timestamp from a Nextflow datetime string."""

    return datetime.strptime(dt[:19], "%Y-%m-%d %H:%M:%S")


def parse_duration(duration):
    """Gets a duration in seconds from a Nextflow duration string."""

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
    escaped_id = id.replace('/', '\\/')
    return re.search(f"\\[{escaped_id}\\] Submitted process > (.+)", log)[1]


def get_process_start_from_log(log, id):
    escaped_id = id.replace('/', '\\/')
    return re.search(
        f"(...-\d\d \d\d:\d\d:\d\d\.\d+).+\\[{escaped_id}\\] Submitted process", log
    )[1]


def get_process_end_from_log(log, id):
    escaped_id = id.replace('/', '\\/')
    match = re.search(f"(...-\d\d \d\d:\d\d:\d\d\.\d+).+Task completed.+{escaped_id}", log)
    if not match: return datetime.now()
    return datetime.strptime(
        f"{str(datetime.now().year)}-{match[1]}",
        "%Y-%b-%d %H:%M:%S.%f"
    )


def get_process_status_from_log(log, id):
    escaped_id = id.replace('/', '\\/')
    match = re.search(f".+Task completed.+status: ([A-Z]+).+{escaped_id}", log)
    return match[1] if match else "-"


def get_process_stdout(execution, id):
    location = get_process_directory(execution, id)
    try:
        with open(os.path.join(location, ".command.out")) as f:
            return f.read()
    except: return "-"


def get_process_stderr(execution, id):
    location = get_process_directory(execution, id)
    try:
        with open(os.path.join(location, ".command.err")) as f:
            return f.read()
    except: return "-"


def get_process_returncode(execution, id):
    location = get_process_directory(execution, id)
    try:
        with open(os.path.join(location, ".exitcode")) as f:
            return f.read()
    except: return ""


def get_process_directory(execution, id):
    first, second = id.split("/")
    options = os.listdir(os.path.join(execution.location, "work", first))
    second = [option for option in options if option.startswith(second)][0]
    return os.path.join(execution.location, "work", first, second)