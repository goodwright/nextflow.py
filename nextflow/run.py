import os
import re
import time
from datetime import datetime
import subprocess

def run(pipeline_path, run_path=None, script_path=None, script_contents="", remote=None, shell=None, version=None, configs=None, params=None, profiles=None):
    """
    :param str run_location: the location to run the pipeline command from.
    """

    nextflow_command = make_nextflow_command(
        run_path, pipeline_path, version, configs, params, profiles
    )
    run_command = make_run_command(
        nextflow_command, remote, script_path, shell
    )
    if script_path:
        create_script(nextflow_command, script_contents, script_path, remote)
    subprocess.run(
        run_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, shell=True
    )
    exection = get_execution(run_path, remote)
    return exection


def make_nextflow_command(run_path, pipeline_path, version, configs, params, profiles):
    """Generates the `nextflow run` commmand.
    
    :param str run_path: the location to run the pipeline in.
    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param str version: the nextflow version to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :rtype: ``str``"""

    env = make_nextflow_command_env_string(version)
    if env: env += " "
    nf = "nextflow -Duser.country=US"
    configs = make_nextflow_command_config_string(configs)
    if configs: configs += " "
    params = make_nextflow_command_params_string(params)
    profiles = make_nextflow_command_profiles_string(profiles)
    command = f"{env}{nf} {configs}run {pipeline_path} {params} {profiles}"
    if run_path: command = f"cd {run_path}; {command}"
    command = command.rstrip() + " >stdout.txt 2>stderr.txt"
    return command


def make_nextflow_command_env_string(version):
    """Creates the environment variable setting portion of the nextflow run
    command string.
    
    :param list configs: any config files to be applied.
    :rtype: ``str``"""

    env = {"NXF_ANSI_LOG": "false"}
    if version: env["NXF_VER"] = version
    return " ".join([f"{k}={v}" for k, v in env.items()])


def make_nextflow_command_config_string(configs):
    """Creates the config setting portion of the nextflow run command string.
    Absolute paths are recommended.
    
    :param str version: the nextflow version to use.
    :rtype: ``str``"""

    if configs is None: configs = []
    return " ".join(f"-c \"{c}\"" for c in configs)


def make_nextflow_command_params_string(params):
    """Creates the parameter setting portion of the nextflow run command

    :param dict params: the parameters to pass.
    :rtype: ``str``"""

    if not params: return ""
    return " ".join([
        f"--{param[0]}='{param[1]}'" if param[1][0] not in "'\""
        else f"--{param[0]}={param[1]}" for param in params.items()
    ])


def make_nextflow_command_profiles_string(profiles):
    """Creates the profile setting portion of the nextflow run command string.
    
    :param list profiles: any profiles to be applied.
    :rtype: ``str``"""

    if not profiles: return ""
    return ("-profile " + ",".join(profiles))


def make_run_command(nextflow_command, remote, script_path="", shell=None):
    """Gnenerates the command that will actually be run with subprocess. It might
    be the nextflow command itself, or it might be wrapped in an ssh call, or it
    might just be a script call.
    
    :param str nextflow_command: the nextflow command to run.
    :param str remote: the ssh hostname to run the command on.
    :param str script_path: the path of the script which wraps the command.
    :param str shell: the shell to use to run any wrapper script.
    :rtype: ``str``"""

    if script_path:
        parent_dir = os.path.dirname(script_path)
        filename = os.path.basename(script_path)
        shell = shell or os.environ.get("SHELL", "/bin/bash")
        command = f"{shell} {filename}"
        if parent_dir: command = f"cd {parent_dir} && {shell} {filename}"
    else:
        command = nextflow_command
    if remote:
        command = command.replace('"', '\\"')
        command = f"ssh {remote} \"{command}\""
    return command


def create_script(nextflow_command, script_contents, script_path, remote=""):
    """Creates a script at the given path with the given command.
    
    :param str nextflow_command: the nextflow command to run.
    :param str script_contents: the script content before the command.
    :param str script_path: the path of the script which wraps the command.
    :param str remote: the ssh hostname to run the command on.
    :rtype: ``str``"""

    text = script_contents + "\n\n\n" + nextflow_command
    if remote:
        text = text.replace('"', '\\"')
        ssh_command = f'echo "{text}" | ssh {remote} "cat > {script_path}"'
        subprocess.run(
            ssh_command, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    else:
        with open(script_path, "w") as f:
            f.write(text)
    return script_path


def get_execution(execution_path, remote):
    log = get_file_text(os.path.join(execution_path, ".nextflow.log"), remote)
    identifier = m[1] if (m := re.search(r"\[([a-z]+_[a-z]+)\]", log)) else ""
    stdout = get_file_text(os.path.join(execution_path, "stdout.txt"), remote)
    stderr = get_file_text(os.path.join(execution_path, "stderr.txt"), remote)
    started = get_started_from_log(log)
    finished = get_finished_from_log(log)
    return {
        "identifier": identifier,
        "stdout": stdout[:20],
        "stderr": stderr[:20],
        "started": started,
        "finished": finished,
    }


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


def get_started_from_log(log):
    """Gets the time the pipeline was started from the log file.
    
    :param str log: the contents of the log file.
    :rtype: ``datetime.datetime``"""

    if not log: return None
    lines = log.splitlines()
    if not lines: return None
    return get_datetime_from_line(lines[0])


def get_finished_from_log(log):
    """Gets the time the pipeline ended from the log file.
    
    :param str log: the contents of the log file.
    :rtype: ``datetime.datetime``"""

    if not log: return None
    lines = log.splitlines()
    if log_is_finished(log):
        for line in reversed(lines):
            dt = get_datetime_from_line(line)
            if dt: return dt


def log_is_finished(log):
    """Checks if the log file indicates the pipeline has finished.
    
    :param str log: the contents of the log file.
    :rtype: ``bool``"""

    if not log: return False
    lines = log.strip().splitlines()
    if lines[-1].endswith(" - > Execution complete -- Goodbye"): return True
    if lines[-1].startswith("    at ") or lines[-1].startswith("\tat "):
        last_unindented = [l for l in lines if not l.startswith("	at ")][-1]
        if "Exception" in last_unindented: return True
    return False


def get_datetime_from_line(line):
    """Gets the datetime from a line of the log file.
    
    :param str line: a line from the log file.
    :rtype: ``datetime.datetime``"""

    year = datetime.now().year
    if (m := re.search(r"[A-Z][a-z]{2}-\d{1,2} \d{2}:\d{2}:\d{2}\.\d{3}", line)):
        return datetime.strptime(f"{year}-{m.group(0)}", "%Y-%b-%d %H:%M:%S.%f")
    return None