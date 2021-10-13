import json

class Pipeline:
    """A .nf file somewhere on the local filesystem."""

    def __init__(self, path, config=None, schema=None):
        self.path = path
        self.config = config
        self.schema = schema
    

    def __repr__(self):
        return f"<Pipeline ({self.path})>"
    

    @property
    def input_schema(self):
        if not self.schema: return None
        with open(self.schema) as f:
            schema = json.load(f)
        inputs = []
        for category in schema["definitions"].values():
            for property in category["properties"].values():
                inputs.append(property)
        return inputs