import os
import re
import time
import subprocess
from nextflow.io import get_file_text, get_process_ids_to_paths
from nextflow.models import Execution, ProcessExecution
from nextflow.log import (
    get_started_from_log,
    get_finished_from_log,
    get_process_name_from_log,
    get_process_start_from_log,
    get_process_end_from_log,
    get_process_status_from_log
)

def run(*args, **kwargs):
    return list(_run(*args, poll=False, **kwargs))[0]


def run_and_poll(*args, **kwargs):
    for execution in _run(*args, poll=True, **kwargs):
        yield execution


def _run(pipeline_path, poll=False, run_path=None, version=None, configs=None, params=None, profiles=None, sleep=2):
    """
    :param str run_location: the location to run the pipeline command from.
    """

    nextflow_command = make_nextflow_command(
        run_path, pipeline_path, version, configs, params, profiles
    )
    subprocess.Popen(
        nextflow_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, shell=True,
    )
    execution = None
    while True:
        time.sleep(sleep)
        execution = get_execution(run_path, nextflow_command)
        if execution and poll: yield execution
        if execution and execution.finished: break
    yield execution


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
    command = command.rstrip() + " >stdout.txt 2>stderr.txt; echo $? >rc.txt"
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


def get_execution(execution_path, nextflow_command):
    log = get_file_text(os.path.join(execution_path, ".nextflow.log"))
    identifier = m[1] if (m := re.search(r"\[([a-z]+_[a-z]+)\]", log)) else ""
    stdout = get_file_text(os.path.join(execution_path, "stdout.txt"))
    stderr = get_file_text(os.path.join(execution_path, "stderr.txt"))
    return_code = get_file_text(os.path.join(execution_path, "rc.txt"))
    started = get_started_from_log(log)
    finished = get_finished_from_log(log)
    process_executions = get_process_executions(log, execution_path)
    command = sorted(nextflow_command.split(";"), key=len)[-1].replace(
        ">stdout.txt 2>stderr.txt", ""
    ).strip()
    execution = Execution(
        identifier=identifier,
        stdout=stdout,
        stderr=stderr,
        return_code=return_code.strip(),
        started=started,
        finished=finished,
        command=command,
        log=log,
        path=execution_path,
        process_executions=process_executions,
    )
    for process_execution in execution.process_executions:
        process_execution.execution = execution
    return execution


def get_process_executions(log, execution_path):
    process_ids = re.findall(
        r"\[([a-f,0-9]{2}/[a-f,0-9]{6})\] Submitted process",
        log, flags=re.MULTILINE
    )
    process_executions = []
    process_ids_to_paths = get_process_ids_to_paths(process_ids, execution_path)
    for process_id in process_ids:
        path = process_ids_to_paths.get(process_id, "")
        process_executions.append(get_process_execution(
            process_id, path, log, execution_path
        ))
    return process_executions


def get_process_execution(process_id, path, log, execution_path):
    stdout, stderr, returncode, bash = "", "", "", ""
    if path:
        full_path = os.path.join(execution_path, "work", path)
        stdout = get_file_text(os.path.join(full_path, ".command.out"))
        stderr = get_file_text(os.path.join(full_path, ".command.err"))
        returncode = get_file_text(os.path.join(full_path, ".exitcode"))
        bash = get_file_text(os.path.join(full_path, ".command.sh"))
    name = get_process_name_from_log(log, process_id)
    return ProcessExecution(
        identifier=process_id,
        name=name,
        process=name[:name.find("(") - 1] if "(" in name else name,
        path=path,
        stdout=stdout,
        stderr=stderr,
        return_code=returncode,
        bash=bash,
        started=get_process_start_from_log(log, process_id),
        finished=get_process_end_from_log(log, process_id),
        status=get_process_status_from_log(log, process_id),
    )