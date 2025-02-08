import re
import os
from nextflow.io import get_file_text

def get_pipeline_process_names(path):
    """Takes a path to a nextflow script and returns a list of the full process
    names in the pipeline, following imports where necessary.
    
    :param str path: the path to the nextflow script.
    :rtype: ``list``"""
    
    file_process_paths = get_file_process_paths(path)
    return list(file_process_paths.keys())


def get_file_process_paths(path):
    """Takes a path to a nextflow script and returns a mapping of the full 
    process names to the full paths of the corresponding module. Any imported
    files are also included in the mapping.

    :param str path: the path to the nextflow script.
    :rtype: ``dict``"""

    text = get_file_text(path)
    lines = text.splitlines()
    names_to_paths = get_import_names_to_paths(text, path)
    workflows = {}
    for name, subpath in names_to_paths.items():
        if sub := get_file_process_paths(subpath): workflows[name] = sub
    paths, workflow_name, pre_workflow = {}, None, True
    for line in lines:
        if line.lstrip().startswith("include"): continue
        match = re.match(r"workflow *(.+?) *\{", line.lstrip())
        if match:
            workflow_name = match.group(1)
            pre_workflow = False
        match = re.match(r"workflow *\{", line.lstrip())
        if match:
            workflow_name = None
            pre_workflow = False
        if pre_workflow: continue
        for name, path in names_to_paths.items():
            if name in line:
                full = f"{workflow_name}:{name}" if workflow_name else name
                if name in workflows:
                    for k, v in workflows[name].items():
                        if v["has_workflow_name"]: k = ":".join(k.split(":")[1:])
                        paths[f"{full}:{k}"] = {
                            "path": v["path"],
                            "has_workflow_name": bool(workflow_name)
                        }
                else:
                    paths[full] = {
                        "path": os.path.normpath(names_to_paths[name]),
                        "has_workflow_name": bool(workflow_name)
                    }
    return paths


def get_import_names_to_paths(text, path):
    """Finds all the import lines in a nextflow script and returns a mapping of
    the module names to the full paths of the modules.
    
    :param str text: the text of the nextflow script.
    :param str path: the path to the nextflow script.
    :rtype: ``dict``"""

    pattern = r'include\s+?\{(.*?)\}\s+?from\s+[\'"](.+?)[\'"]'
    imports = re.findall(pattern, text, re.DOTALL)
    names_to_paths = {}
    for imp in imports:
        module_name = imp[0].strip().split()[-1]
        module_path = imp[1].strip()
        if not module_path.endswith(".nf"): module_path += ".nf"
        full_module_path = os.path.join(os.path.dirname(path), module_path)
        names_to_paths[module_name] = full_module_path
    return names_to_paths