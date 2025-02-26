import re
from datetime import datetime

def get_started_from_log(log):
    """Gets the time the pipeline was started from the log file.
    
    :param str log: the contents of the log file.
    :rtype: ``datetime.datetime``"""

    if not log: return None
    lines = log.splitlines()
    if not lines: return None
    return get_datetime_from_line(lines[0])


def get_finished_from_log(log):
    """Gets the time the pipeline ended from the log file.
    
    :param str log: the contents of the log file.
    :rtype: ``datetime.datetime``"""

    if not log: return None
    lines = log.splitlines()
    if log_is_finished(log):
        for line in reversed(lines):
            dt = get_datetime_from_line(line)
            if dt: return dt


def log_is_finished(log):
    """Checks if the log file indicates the pipeline has finished.
    
    :param str log: the contents of the log file.
    :rtype: ``bool``"""

    if not log: return False
    lines = log.strip().splitlines()
    if lines[-1].endswith(" - > Execution complete -- Goodbye"): return True
    if lines[-1].startswith("    at ") or lines[-1].startswith("\tat "):
        last_unindented = [l for l in lines if not l.startswith("	at ")][-1]
        if "Exception" in last_unindented: return True
    return False


def get_identifier_from_log(log):
    """Gets the nextflow adjective_name identifier from the log file.
    
    :param str log: the contents of the log file.
    :rtype: ``str``"""

    if not log: return ""
    if (m := re.search(r"\[([a-z]+_[a-z]+)\]", log)): return m[1]
    return ""


def get_session_uuid_from_log(log):
    """Gets the session uuid from the log file.
    
    :param str log: the contents of the log file.
    :rtype: ``str``"""

    if not log: return ""
    if (m := re.search(r"Session UUID: ([\w-]+)", log)): return m[1]
    return ""


def get_datetime_from_line(line):
    """Gets the datetime from a line of the log file.
    
    :param str line: a line from the log file.
    :rtype: ``datetime.datetime``"""

    year = datetime.now().year
    if (m := re.search(r"[A-Z][a-z]{2}-\d{1,2} \d{2}:\d{2}:\d{2}\.\d{3}", line)):
        return datetime.strptime(f"{year}-{m.group(0)}", "%Y-%b-%d %H:%M:%S.%f")
    return None


def parse_cached_line(line):
    """Parses a line from the log file that indicates a process has been
    cached, to get its xx/yyyyyy identifier, name, and process.

    :param str line: a line from the log file.
    :rtype: ``tuple``"""

    log_pattern = (
        r"(?P<timestamp>\w{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[.*?\] INFO  "
        r"nextflow\.processor\.TaskProcessor - \[(?P<id>[\w/]+)\] "
        r"Cached process > (?P<name>.+)"
    )
    match = re.match(log_pattern, line)
    if not match: return "", "", ""
    identifier = match.group("id")
    name = match.group("name")
    process = name[:name.find("(") - 1] if "(" in name else name
    return identifier, name, process


def parse_submitted_line(line):
    """Parses a line from the log file that indicates a process has been
    submitted, to get its xx/yyyyyy identifier, name, process, and submission
    time.

    :param str line: a line from the log file.
    :rtype: ``tuple``"""

    log_pattern = (
        r"(?P<timestamp>\w{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[.*?\] INFO  "
        r"nextflow\.Session - \[(?P<id>[\w/]+)\] "
        r"Submitted process > (?P<name>.+)"
    )
    match = re.match(log_pattern, line)
    if not match: return "", "", "", None
    identifier = match.group("id")
    name = match.group("name")
    process = name[:name.find("(") - 1] if "(" in name else name
    year = datetime.now().year
    submitted = datetime.strptime(
        f"{year}-{match.group('timestamp')}", "%Y-%b-%d %H:%M:%S.%f"
    )
    return identifier, name, process, submitted


def parse_completed_line(line):
    """Parses a line from the log file that indicates a process has completed,
    to get its xx/yyyyyy identifier, finish time, return code, and status.

    :param str line: a line from the log file.
    :rtype: ``tuple``"""

    log_pattern = (
        r"(?P<timestamp>\w{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) .*?"
        r"Task completed > TaskHandler\[.*?"
        r"name: (?P<name>.+); status: (?P<status>\w+); "
        r"exit: (?P<exit_code>\d+); .*?workDir: .*?/work/(?P<id>[\w/]{9})"
    )
    match = re.match(log_pattern, line)
    if not match: return "", None, "", ""
    identifier = match.group("id")
    year = datetime.now().year
    finished = datetime.strptime(
        f"{year}-{match.group('timestamp')}", "%Y-%b-%d %H:%M:%S.%f"
    )
    exit_code = match.group("exit_code")
    status = match.group("status") or "-"
    if exit_code != "0": status = "FAILED"
    return identifier, finished, exit_code, status