import os
import subprocess

def get_file_text(path):
    """Gets the contents of a text file, if it exists.
    
    :param str path: the location of the file.
    :rtype: ``str``"""

    try:
        with open(path, "r") as f: return f.read()
    except FileNotFoundError:
        return ""