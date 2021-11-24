import os
import subprocess

class Execution:
    """The record of the running of a Nextflow script."""

    def __init__(self, location, id, process=None):
        self.location = location
        self.id = id
        self.process = process
        self.update_process_executions()
    

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
    def datetime(self):
        """The datetime at which the execution started."""

        data = self.history_data
        if data: return data[0]
    

    @property
    def duration(self):
        """How long the execution took to complete."""

        data = self.history_data
        if data: return data[1]
    

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


    def update_process_executions(self):
        """Creates process execution objects for the execution by interrogating
        the log and work directories."""
        
        field_names = self.get_available_fields()
        field_names = [f"\\${f}" for f in field_names]
        self.process_executions = []
        for path in self.get_process_paths():
            fields = {}
            keys = [f[2:] for f in field_names]
            template = ' XXXXXXXXX '.join(field_names)
            values = subprocess.run(
                f"nextflow log {self.id} -t \"{template}\" -F \"workdir == '{path}'\"",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, shell=True, cwd=self.location
            ).stdout.strip().split(" XXXXXXXXX ")
            fields = dict(zip(keys, values))
            self.process_executions.append(NextflowProcess(
                fields=fields, execution=self
            ))



class NextflowProcess:

    def __init__(self, fields, execution):
        self.fields = fields
        self.execution = execution
    

    def __getattr__(self, key):
        try:
            return self.fields[key]
        except KeyError:
            raise AttributeError(f"NextflowProcess has no attribute '{key}'")
    

    def __repr__(self):
        return f"<NextflowProcess from {self.execution.id}: {self.name}>"