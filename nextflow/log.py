import re
from datetime import datetime


def get_started_from_log(log):
    """Gets the time the pipeline was started from the log file.

    :param str log: the contents of the log file.
    :rtype: ``datetime.datetime``"""

    if not log:
        return None
    lines = log.splitlines()
    if not lines:
        return None
    return get_datetime_from_line(lines[0])


def get_finished_from_log(log):
    """Gets the time the pipeline ended from the log file.

    :param str log: the contents of the log file.
    :rtype: ``datetime.datetime``"""

    if not log:
        return None
    lines = log.splitlines()
    if log_is_finished(log):
        for line in reversed(lines):
            dt = get_datetime_from_line(line)
            if dt:
                return dt


def log_is_finished(log):
    """Checks if the log file indicates the pipeline has finished.

    :param str log: the contents of the log file.
    :rtype: ``bool``"""

    if not log:
        return False
    lines = log.strip().splitlines()
    if lines[-1].endswith(" - > Execution complete -- Goodbye"):
        return True
    if lines[-1].startswith("    at ") or lines[-1].startswith("\tat "):
        last_unindented = [l for l in lines if not l.startswith("	at ")][-1]
        if "Exception" in last_unindented:
            return True
    return False


def get_datetime_from_line(line):
    """Gets the datetime from a line of the log file.

    :param str line: a line from the log file.
    :rtype: ``datetime.datetime``"""

    year = datetime.now().year
    if m := re.search(r"[A-Z][a-z]{2}-\d{1,2} \d{2}:\d{2}:\d{2}\.\d{3}", line):
        return datetime.strptime(f"{year}-{m.group(0)}", "%Y-%b-%d %H:%M:%S.%f")
    return None


def get_process_name_from_log(log, process_id):
    """Gets a process's name from the .nextflow.log file, given its unique ID.

    :param str log: The text of the log file.
    :param str process_id: The process's ID (eg. '1a/234bcd').
    :rtype: ``str``"""

    escaped_id = process_id.replace("/", "\\/")
    match = re.search(f"\\[{escaped_id}\\] Submitted process > (.+)", log)
    if match:
        return match[1]


def get_process_start_from_log(log, process_id):
    """Gets a process's start time from the .nextflow.log file as a datetime,
    given its unique ID, if it has started.

    :param str log: The text of the log file.
    :param str process_id: The process's ID (eg. '1a/234bcd').
    :rtype: ``datetime``"""

    escaped_id = process_id.replace("/", "\\/")
    match = re.search(
        f"(...-\d+ \d+:\d+:\d+\.\d+).+\\[{escaped_id}\\] Submitted process", log
    )
    if not match:
        return
    year = datetime.now().year
    return datetime.strptime(f"{year}-{match[1]}", "%Y-%b-%d %H:%M:%S.%f")


def get_process_end_from_log(log, process_id):
    """Gets a process's end time from the .nextflow.log file as a datetime,
    given its unique ID, if it has finshed.

    :param str log: The text of the log file.
    :param str process_id: The process's ID (eg. '1a/234bcd').
    :rtype: ``datetime``"""

    escaped_id = process_id.replace("/", "\\/")
    match = re.search(f"(...-\d+ \d+:\d+:\d+\.\d+).+Task completed.+{escaped_id}", log)
    if not match:
        return
    year = datetime.now().year
    return datetime.strptime(f"{year}-{match[1]}", "%Y-%b-%d %H:%M:%S.%f")


def get_process_status_from_log(log, process_id):
    """Gets a process's status (COMPLETED, ERROR etc.) from the .nextflow.log
    file, given its unique ID. If it's still running, '-' is returned.

    :param str log: The text of the log file.
    :param str process_id: The process's ID (eg. '1a/234bcd').
    :rtype: ``string``"""

    escaped_id = process_id.replace("/", "\\/")
    match = re.search(
        f".+Task completed.+status: ([A-Z]+).+exit: (.+?);.+{escaped_id}", log
    )
    if not match:
        return "-"
    if match[2].isdigit() and match[2] != "0":
        return "FAILED"
    return match[1]


def collect_process_info_from_logs(log, process_ids):
    year = datetime.now().year
    process_info = {}
    for pid in process_ids:
        process_info[pid] = {}
        process_info[pid]["name"] = None
        process_info[pid]["start"] = None
        process_info[pid]["end"] = None
        process_info[pid]["status"] = "-"

    for line in log.splitlines():
        line = line.strip()

        if "Submitted process" in line:
            # Example logline:
            # Jan-17 17:01:48.846 [AWSBatch-executor-163] INFO  nextflow.Session - [f4/a0d5ad] Submitted process > SINGLETS (223)
            log_pattern = r"(?P<timestamp>\w{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[.*?\] INFO  nextflow\.Session - \[(?P<id>[\w/]+)\] Submitted process > (?P<name>.+)"
            match = re.match(log_pattern, line)
            if match:
                start = datetime.strptime(
                    f"{year}-{match.group('timestamp')}", "%Y-%b-%d %H:%M:%S.%f"
                )
                id = match.group("id")
                name = match.group("name")

                if id in process_info:
                    process_info[id]["name"] = name
                    process_info[id]["start"] = start

        elif "Task completed" in line:
            # Example logline:
            # Jan-17 17:01:49.519 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 938; name: COMPENSATE (228); status: COMPLETED; exit: 0; error: -; workDir: s3://ozette-temp/nextflow/work/5d/ce81938befec30ba97b2788592b055]
            log_pattern = (
                r"(?P<timestamp>\w{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) .*?"
                r"Task completed > TaskHandler\[.*?status: (?P<status>\w+); "
                r"exit: (?P<exit_code>\d+); .*?workDir: .*?/work/(?P<id>[\w/]{9})"
            )

            match = re.match(log_pattern, line)
            if match:
                # Extract the components
                end = datetime.strptime(
                    f"{year}-{match.group('timestamp')}", "%Y-%b-%d %H:%M:%S.%f"
                )
                status = match.group("status")
                exit_code = match.group("exit_code")
                id = match.group("id")
                if id in process_info:
                    process_info[id]["name"] = name
                    process_info[id]["end"] = end
                    if exit_code.isdigit() and exit_code != "0":
                        process_info[id]["status"] = "FAILED"
                    else:
                        process_info[id]["status"] = status

    return process_info
