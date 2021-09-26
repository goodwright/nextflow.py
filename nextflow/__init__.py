

class Execution:

    def __init__(self, location, id):
        self.location = location
        self.id = id
    

    @property
    def datetime(self):
        with open(f"{self.location}/.nextflow/history") as f:
            lines = f.readlines()
        for line in lines:
            values = line.split("\t")
            if values[2] == self.id: return values[0]
        