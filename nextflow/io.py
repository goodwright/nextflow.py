import os

def get_file_text(path):
    """Gets the contents of a text file, if it exists.
    
    :param str path: the location of the file.
    :rtype: ``str``"""

    try:
        with open(path, "r") as f: return f.read()
    except FileNotFoundError:
        return ""


def get_process_ids_to_paths(process_ids, execution_path):
    """Takes a list of nine character process IDs and maps them to the full
    directories they represent.
    
    :param list process_ids: a list of nine character process IDs.
    :param str execution_path: the path to the execution directory.
    :rtype: ``dict``"""

    process_ids_to_paths = {}
    path = os.path.join(execution_path, "work")
    subdirectories = []
    for root, dirs, _ in os.walk(path):
        for directory in dirs:
            subdirectories.append(os.path.join(root, directory))
    for subdirectory in subdirectories:
        sub = os.path.sep.join(subdirectory.split(os.path.sep)[-2:])
        for process_id in process_ids:
            if sub.startswith(process_id):
                process_ids_to_paths[process_id] = subdirectory
                break
    return process_ids_to_paths