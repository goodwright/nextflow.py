import os
import re
import subprocess
from datetime import datetime

from nextflow.utils import parse_datetime, parse_duration

class Execution:
    """The record of the running of a Nextflow script."""

    def __init__(self, location, id, stdout=None, stderr=None, returncode=None, use_nextflow=True):
        self.location = location
        self.id = id
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        if use_nextflow:
            self.update_process_executions_with_nextflow()
        else:
            self.update_process_executions_without_nextflow()
    

    @staticmethod
    def create_from_location(location, stdout, stderr, returncode, use_nextflow):
        with open(os.path.join(location, ".nextflow.log")) as f:
            log_text = f.read()
        run_id = re.search(r"\[([a-z]+_[a-z]+)\]", log_text)[1]
        return Execution(
            location, run_id, stdout=stdout, stderr=stderr,
            returncode=returncode, use_nextflow=use_nextflow
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
    def datetime_string(self):
        """The datetime at which the execution started."""

        data = self.history_data
        if data: return data[0]
    

    @property
    def timestamp(self):
        string = self.datetime_string
        if not string: return None
        return parse_datetime(self.datetime_string)
    

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
    

    def get_available_fields(self):
        """Gets all the fields that Nextflow can report for this execution."""

        process = subprocess.run(
            f"nextflow log {self.id} -l",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd=self.location
        )
        return [field.strip() for field in
            process.stdout.splitlines() if field.strip()]
    

    def get_process_paths(self):
        """Gets the work directories of all the process executions in this
        execution."""

        return subprocess.run(
            f"nextflow log {self.id}",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd=self.location
        ).stdout.splitlines()


    def update_process_executions_with_nextflow(self):
        """Creates process execution objects for the execution by interrogating
        the log and work directories."""

        field_names = self.get_available_fields()
        field_names = [f"\\${f}" for f in field_names]
        template = ' XXXXXXXXX '.join(field_names)
        all_values = subprocess.run(
            f"nextflow log {self.id} -t \"{template}\"",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd=self.location
        ).stdout.strip().split(" XXXXXXXXX ")
        process_count = int(((len(all_values) - 1) / (len(field_names) - 1)))
        process_values = []
        for p in range(process_count):
            values = all_values[p * len(field_names): (p + 1) * len(field_names)]
            last_column_values = values[-1].split("\n")
            if len(last_column_values) == 2:
                real_last, actual_next_first = last_column_values
                values[-1] = real_last
                all_values.insert((p + 1) * len(field_names), actual_next_first)
            process_values.append(values)
        self.process_executions = []
        for values in process_values:
            keys = [f[2:] for f in field_names]
            fields = dict(zip(keys, values))
            self.process_executions.append(ProcessExecution(
                fields=fields, execution=self
            ))
    

    def update_process_executions_without_nextflow(self):
        """Creates process executions without calls the 'nextflow log' at the
        command line. This is useful when the pipeline is still running, though
        it relies of a particular formatting of the log file."""
        
        self.process_executions = []
        log_text = self.log
        for match in re.findall(
            r"^(.+?) \[Task submitter\].+?\[(..\/......)\] Submitted process > (.+)",
            log_text, flags=re.MULTILINE
        ):
            dir1, dir2 = match[1].split("/")
            dir1 = os.path.join(self.location, "work", dir1)
            dir2 = [d for d in os.listdir(dir1) if d.startswith(dir2)][0]
            proc_dir = os.path.join(dir1, dir2)
            fields = {"hash": match[1], "name": match[2]}
            fields["process"] = match[2][:match[2].find("(") - 1]
            start_dt = datetime.strptime(
                f"{str(datetime.now().year)}-{match[0]}", "%Y-%b-%d %H:%M:%S.%f"
            )
            fields["start"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            completed_line = re.search(
                r"^(.+?) \[.+?completed >.+?status: (.+?);.+?" + match[1],
                log_text, flags=re.MULTILINE
            )
            end_dt = datetime.now()
            if completed_line:
                fields["status"] = completed_line[2]
                if fields["status"] == "COMPLETED" and "exit: 0" not in completed_line[0]:
                    fields["status"] = "ERROR"
                end_dt = datetime.strptime(
                    f"{str(datetime.now().year)}-{completed_line[1]}",
                    "%Y-%b-%d %H:%M:%S.%f"
                )
            else:
                fields["status"] = "-"
            fields["duration"] = str((end_dt - start_dt).seconds) + "s"
            try:
                with open(os.path.join(proc_dir, ".command.out")) as f:
                    fields["stdout"] = f.read() or "-"
            except: fields["stdout"] = "-"
            try:
                with open(os.path.join(proc_dir, ".command.err")) as f:
                    fields["stderr"] = f.read() or "-"
            except: fields["stderr"] = "-"
            self.process_executions.append(ProcessExecution(
                fields=fields, execution=self
            ))



class ProcessExecution:

    def __init__(self, fields, execution):
        self.fields = fields
        self.execution = execution
    

    def __getattr__(self, key):
        try:
            return self.fields[key]
        except KeyError:
            raise AttributeError(f"ProcessExecution has no attribute '{key}'")
    

    def __repr__(self):
        return f"<ProcessExecution from {self.execution.id}: {self.name}>"