"""Functions for parsing Nextflow code."""

import re
from pathlib import Path

def get_imports(filestring):
    """Scans the contents of a .nf file and returns all the imports.
    
    :param str filestring: the .nf filestring.
    :rtype: ``dict``"""
    
    pattern = "include.*?{(.*?)}.*?from.*?['\"](.*?)['\"]"
    matches = list(re.finditer(pattern, filestring))
    nf = lambda s: s if s.endswith(".nf") else s + ".nf"
    return {m[1].strip(): nf(m[2].strip()) for m in matches}


def get_workflows(filestring):
    """Scans the contents of a .nf file and returns all the workflows.
    
    :param str filestring: the .nf filestring.
    :rtype: ``list``"""

    pattern = r"workflow\s*([A-Z0-9_]*)\s*{(.+?)}"
    matches = list(re.finditer(pattern, filestring, re.DOTALL))
    return [{"body": m[2].strip(), "name": m[1].strip()} for m in matches]


def get_module_paths(path, prefix=""):
    """Gets all modules used in a pipeline as a mapping of process names to
    module locations. Files are searched recursively, following all imports.
    
    :param str path: the location of the pipeline.
    :paam str prefix: the prefix to apply to all names.
    :rtype: ``dict``"""
    
    with open(path) as f: filestring = f.read()
    imports = get_imports(filestring)
    workflows = get_workflows(filestring)
    location = Path(path).parent
    names = {}
    for workflow in workflows:
        wf_prefix = workflow["name"] + ":" if workflow["name"] else ""
        for import_name, import_path in imports.items():
            full_name = f"{wf_prefix}{import_name}"
            if f"{import_name}(" in workflow["body"]:
                path = Path(f"{location}/{import_path}").resolve()
                imported_names = get_module_paths(path, prefix=wf_prefix)
                if imported_names:
                    names = {**names, **imported_names}
                else:
                    names[prefix + full_name] = str(path)
    return names