import os
import subprocess

def get_file_text(path, remote):
    """Gets the contents of a text file, if it exists. The text file can be on
    the local machine or on a remote machine.
    
    :param str path: the location of the file.
    :param str remote: the ssh hostname the the path is for.
    :rtype: ``str``"""

    if remote:
        command = f"ssh {remote} 'cat {path}'"
        process = subprocess.run(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        return process.stdout.decode() if process.returncode == 0 else ""
    try:
        with open(path, "r") as f: return f.read()
    except FileNotFoundError:
        return ""


def get_directory_contents(path, remote):
    if remote:
        return filter(bool, subprocess.check_output(
            f"ssh {remote} 'ls {path}'",
            shell=True
        ).decode("utf-8").split("\n"))
    return os.listdir(path)