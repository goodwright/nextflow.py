import os

class Execution:

    def __init__(self, location, id):
        self.location = location
        self.id = id
    

    def __repr__(self):
        return f"<Execution [{self.id}]>"
    

    @property
    def history_data(self):
        with open(f"{self.location}/.nextflow/history") as f:
            lines = f.readlines()
        for line in lines:
            values = line.split("\t")
            if values[2] == self.id: return values

    
    @property
    def datetime(self):
        data = self.history_data
        if data: return data[0]
    

    @property
    def duration(self):
        data = self.history_data
        if data: return data[1]
    

    @property
    def status(self):
        data = self.history_data
        if data: return data[3]
    

    @property
    def command(self):
        data = self.history_data
        if data: return data[6]
    

    @property
    def log(self):
        for filename in os.listdir(self.location):
            if ".nextflow.log" in filename:
                with open(f"{self.location}/{filename}") as f:
                    text = f.read()
                    if f"[{self.id}]" in text: return text



class Script:

    def __init__(self, path):
        self.path = path
    

    def __repr__(self):
        return f"<Script ({self.path})>"