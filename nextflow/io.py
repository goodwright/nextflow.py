import os
import glob
from zoneinfo import ZoneInfo
from datetime import datetime

def get_file_text(path, io=None):
    """Gets the contents of a text file, if it exists.
    
    :param str path: the location of the file.
    :param io: an optional custom io object to handle reading.
    :rtype: ``str``"""

    try:
        if io: return io.read(path)
        with open(path, "r") as f: return f.read()
    except FileNotFoundError:
        return ""


def get_file_creation_time(path, timezone=None, io=None):
    """Gets the creation time of a file.
    
    :param str path: the location of the file.
    :param str timezone: an optional timezone to convert the creation time to.
    :param io: an optional custom io object to handle file times.
    :rtype: ``datetime.datetime``"""

    try:
        if io:
            dt = io.ctime(path)
        else:
            dt = datetime.fromtimestamp(os.path.getctime(path))
        if timezone:
            tz = ZoneInfo(timezone)
            dt = dt.astimezone(tz)
            dt = dt.replace(tzinfo=None)
        return dt
    except FileNotFoundError:
        return None


def get_process_ids_to_paths(process_ids, execution_path, io=None):
    """Takes a list of nine character process IDs and maps them to the full
    directories they represent.
    
    :param list process_ids: a list of nine character process IDs.
    :param str execution_path: the path to the execution directory.
    :param io: an optional custom io object to handle globbing.
    :rtype: ``dict``"""

    process_ids_to_paths = {}
    path = os.path.join(execution_path, "work")
    if io:
        subdirectories = io.glob(os.path.join(path, "*", "*"))
    else:
        subdirectories = glob.glob(os.path.join(path, "*", "*"))
    for subdirectory in subdirectories:
        sub = os.path.sep.join(subdirectory.split(os.path.sep)[-2:])
        for process_id in process_ids:
            if sub.startswith(process_id):
                process_ids_to_paths[process_id] = subdirectory
                break
    return process_ids_to_paths