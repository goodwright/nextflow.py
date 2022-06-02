import os
import re
from datetime import datetime
from nextflow.utils import *

class Execution:
    """The record of the running of a Nextflow script."""

    def __init__(self, location, id, stdout=None, stderr=None, returncode=None):
        self.location = location
        self.id = id
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.get_processes_from_log()
    

    @staticmethod
    def create_from_location(location, stdout, stderr, returncode):
        with open(os.path.join(location, ".nextflow.log")) as f:
            log_text = f.read()
        run_id = re.search(r"\[([a-z]+_[a-z]+)\]", log_text)[1]
        return Execution(
            location, run_id, stdout=stdout, stderr=stderr,
            returncode=returncode
        )
    

    def __repr__(self):
        return f"<Execution [{self.id}]>"
    

    @property
    def history_data(self):
        """Gets the execution's data line from .nextflow/history as a list of
        values."""

        with open(os.path.join(self.location, ".nextflow", "history")) as f:
            lines = f.readlines()
        for line in lines:
            values = line.split("\t")
            if values[2] == self.id: return values

    
    @property
    def started_string(self):
        """The datetime at which the execution started as a string."""

        data = self.history_data
        if data: return data[0]
    

    @property
    def started_dt(self):
        string = self.started_string
        if not string: return None
        return parse_datetime(string)
    

    @property
    def started(self):
        return datetime.timestamp(self.started_dt)
    

    @property
    def duration_string(self):
        """How long the execution took to complete."""

        data = self.history_data
        if data: return data[1]
    

    @property
    def duration(self):
        string = self.duration_string
        if not string: return None
        return parse_duration(self.duration_string)
    

    @property
    def status(self):
        """The Nextflow reported status of the execution."""

        data = self.history_data
        if data: return data[3]
    

    @property
    def command(self):
        """The command used at the terminal to run the pipeline."""

        data = self.history_data
        if data: return data[6]
    

    @property
    def log(self):
        """Gets the full text of the execution's log file."""

        for filename in os.listdir(self.location):
            if ".nextflow.log" in filename:
                with open(os.path.join(self.location, filename)) as f:
                    text = f.read()
                    if f"[{self.id}]" in text: return text
    

    def get_processes_from_log(self):
        """Populates the process_executions list from data in the execution's
        log file."""

        log_text = self.log
        self.process_executions = []
        for process_id in [match for match in re.findall(
            r"\[([a-f,0-9]{2}/[a-f,0-9]{6})\] Submitted process",
            log_text, flags=re.MULTILINE
        )]:
            fields = {
                "hash": process_id,
                "process": "",
                "name": get_process_name_from_log(log_text, process_id),
                "started_string": get_process_start_from_log(log_text, process_id),
                "started": None,
                "duration": 0,
                "status":  get_process_status_from_log(log_text, process_id),
                "stdout": get_process_stdout(self, process_id),
                "stderr": get_process_stderr(self, process_id),
                "returncode": get_process_returncode(self, process_id),
            }
            if "(" in fields["name"]:
                fields["process"] = fields["name"][:fields["name"].find("(") - 1]
            else:
                fields["process"] = fields["name"]
            fields["started"] = datetime.strptime(
                f"{str(datetime.now().year)}-{fields['started_string']}",
                "%Y-%b-%d %H:%M:%S.%f"
            )
            end_time = get_process_end_from_log(log_text, process_id)
            fields["duration"] = (end_time - fields["started"]).total_seconds()
            self.process_executions.append(
                ProcessExecution(execution=self, **fields)
            )

            

class ProcessExecution:

    def __init__(self, execution, hash, process, name, status, stdout, stderr, started_string, started, duration, returncode):
        self.execution = execution
        self.hash = hash
        self.process = process
        self.name = name
        self.status = status
        self.stdout = stdout
        self.stderr = stderr
        self.started_string = started_string
        self.started_dt = started
        self.duration = duration
        self.returncode = returncode
    

    def __repr__(self):
        return f"<ProcessExecution from {self.execution.id}: {self.name}>"
    

    @property
    def started(self):
        return datetime.timestamp(self.started_dt)