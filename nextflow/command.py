import os
import re
import time
import subprocess
from datetime import datetime
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
    """Runs a pipeline and returns the execution.
    
    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param str run_path: the location to run the pipeline in.
    :param str output_path: the location to store the output in.
    :param function runner: a function to run the pipeline command.
    :param str version: the nextflow version to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :param str timezone: the timezone to use for the log.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :rtype: ``nextflow.models.Execution``"""

    return list(_run(*args, poll=False, **kwargs))[0]


def run_and_poll(*args, **kwargs):
    """Runs a pipeline and polls it for updates. Yields the execution after each
    update.
    
    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param str run_path: the location to run the pipeline in.
    :param str output_path: the location to store the output in.
    :param function runner: a function to run the pipeline command.
    :param str version: the nextflow version to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :param str timezone: the timezone to use for the log.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :param int sleep: the number of seconds to wait between polls.
    :rtype: ``nextflow.models.Execution``"""

    for execution in _run(*args, poll=True, **kwargs):
        yield execution


def _run(
        pipeline_path, poll=False, run_path=None, output_path=None, runner=None,
        version=None, configs=None, params=None, profiles=None, timezone=None,
        report=None, timeline=None, dag=None, sleep=1
):
    if not run_path: run_path = os.path.abspath(".")
    nextflow_command = make_nextflow_command(
        run_path, output_path, pipeline_path, version, configs, params,
        profiles, timezone, report, timeline, dag
    )
    if runner:
        process = None
        runner(nextflow_command)
    else:
        process = subprocess.Popen(
            nextflow_command, universal_newlines=True, shell=True        
        )
    execution = None
    while True:
        time.sleep(sleep)
        execution = get_execution(output_path or run_path, nextflow_command)
        if execution and poll: yield execution
        process_finished = not process or process.poll() is not None
        if execution and execution.return_code and process_finished:
            if not poll: yield execution
            break


def make_nextflow_command(run_path, output_path, pipeline_path, version, configs, params, profiles, timezone, report, timeline, dag):
    """Generates the `nextflow run` commmand.
    
    :param str run_path: the location to run the pipeline in.
    :param str output_path: the location to store the output in.
    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param str version: the nextflow version to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :param str timezone: the timezone to use.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :rtype: ``str``"""

    env = make_nextflow_command_env_string(version, timezone, output_path)
    if env: env += " "
    nf = "nextflow -Duser.country=US"
    log = make_nextflow_command_log_string(output_path)
    if log: log += " "
    configs = make_nextflow_command_config_string(configs)
    if configs: configs += " "
    params = make_nextflow_command_params_string(params)
    profiles = make_nextflow_command_profiles_string(profiles)
    reports = make_reports_string(output_path, report, timeline, dag)
    command = f"{env}{nf} {log}{configs}run {pipeline_path} {params} {profiles} {reports}"
    if run_path: command = f"cd {run_path}; {command}"
    prefix = (str(output_path) + os.path.sep) if output_path else ""
    command = command.rstrip() + f" >{prefix}"
    command += f"stdout.txt 2>{prefix}"
    command += f"stderr.txt; echo $? >{prefix}rc.txt"
    return command


def make_nextflow_command_env_string(version, timezone, output_path):
    """Creates the environment variable setting portion of the nextflow run
    command string.
    
    :param str version: the nextflow version to use.
    :param str timezone: the timezone to use.
    :param str output_path: the location to store the output in.
    :rtype: ``str``"""

    env = {"NXF_ANSI_LOG": "false"}
    if version: env["NXF_VER"] = version
    if timezone: env["TZ"] = timezone
    if output_path: env["NXF_WORK"] = os.path.join(output_path, "work")
    return " ".join([f"{k}={v}" for k, v in env.items()])


def make_nextflow_command_log_string(output_path):
    """Creates the log setting portion of the nextflow run command string.
    
    :param str output_path: the location to store the output in.
    :rtype: ``str``"""

    if not output_path: return ""
    return f"-log '{os.path.join(output_path, '.nextflow.log')}'"


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
    param_list = []
    for key, value in params.items():
        if not value:
            param_list.append(f"--{key}=")
        elif value[0] in "'\"": 
            param_list.append(f"--{key}={value}")
        else:
            param_list.append(f"--{key}='{value}'")
    return " ".join(param_list)


def make_nextflow_command_profiles_string(profiles):
    """Creates the profile setting portion of the nextflow run command string.
    
    :param list profiles: any profiles to be applied.
    :rtype: ``str``"""

    if not profiles: return ""
    return ("-profile " + ",".join(profiles))


def make_reports_string(output_path, report, timeline, dag):
    """Creates the report setting portion of the nextflow run command string.
    
    :param str output_path: the location to store the output in.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :rtype: ``str``"""

    params = []
    if report: params.append(f"-with-report {report}")
    if timeline: params.append(f"-with-timeline {timeline}")
    if dag: params.append(f"-with-dag {dag}")
    if output_path:
        for i, param in enumerate(params):
            words = param.split(" ")
            words[1] = os.path.join(output_path, words[1])
            params[i] = " ".join(words)
    return " ".join(params)


def get_execution(execution_path, nextflow_command):
    """Creates an execution object from a location.
    
    :param str execution_path: the location of the execution.
    :param str nextflow_command: the command used to run the pipeline.
    :rtype: ``nextflow.models.Execution``"""

    log = get_file_text(os.path.join(execution_path, ".nextflow.log"))
    if not log: return
    lines = log.splitlines()
    execution = make_execution_from_log(log, execution_path, nextflow_command)
    process_executions = get_initial_process_executions_from_log(lines, execution)
    process_ids_to_paths = get_process_ids_to_paths(list(process_executions.keys()), execution_path)
    for process_id, path in process_ids_to_paths.items():
        process_executions[process_id].path = path
    for process_execution in process_executions.values():
        update_process_execution_from_path(process_execution, execution_path)
    execution.process_executions = list(process_executions.values())
    return execution


def make_execution_from_log(log, execution_path, nextflow_command):
    identifier = m[1] if (m := re.search(r"\[([a-z]+_[a-z]+)\]", log)) else ""
    stdout = get_file_text(os.path.join(execution_path, "stdout.txt"))
    stderr = get_file_text(os.path.join(execution_path, "stderr.txt"))
    return_code = get_file_text(os.path.join(execution_path, "rc.txt"))
    started = get_started_from_log(log)
    finished = get_finished_from_log(log)
    command = sorted(nextflow_command.split(";"), key=len)[-1]
    command = re.sub(r">[a-zA-Z0-9\/-]+?stdout\.txt", "", command)
    command = re.sub(r"2>[a-zA-Z0-9\/-]+?stderr\.txt", "", command).strip()
    return Execution(
        identifier=identifier, stdout=stdout, stderr=stderr,
        return_code=return_code.strip(), started=started, finished=finished,
        command=command, log=log, path=execution_path,
        process_executions=[],
    )


def get_initial_process_executions_from_log(lines, execution):
    process_executions = {}
    for line in lines:
        if "Submitted process" in line:
            proc_ex = create_process_execution_from_line(line)
            if not proc_ex: continue
            proc_ex.execution = execution
            process_executions[proc_ex.identifier] = proc_ex
        elif "Task completed" in line:
            update_process_execution_from_line(process_executions, line)
    return process_executions


def create_process_execution_from_line(line):
    # Parse line
    log_pattern = r"(?P<timestamp>\w{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[.*?\] INFO  nextflow\.Session - \[(?P<id>[\w/]+)\] Submitted process > (?P<name>.+)"
    match = re.match(log_pattern, line)
    if not match: return

    # Get identifier
    identifier = match.group("id")

    # Get name and process
    name = match.group("name")
    process = name[:name.find("(") - 1] if "(" in name else name

    # Get started
    year = datetime.now().year
    started = datetime.strptime(
        f"{year}-{match.group('timestamp')}", "%Y-%b-%d %H:%M:%S.%f"
    )

    # Create process execution
    return ProcessExecution(
        identifier=identifier,
        name=name,
        process=process,
        path="",
        stdout="",
        stderr="",
        return_code=None,
        bash="",
        started=started,
        finished=None,
        status="-",
    )


def update_process_execution_from_line(process_executions, line):
    # Parse line
    log_pattern = r"(?P<timestamp>\w{3}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) .*?" r"Task completed > TaskHandler\[.*?name: (?P<name>.+); status: (?P<status>\w+); " r"exit: (?P<exit_code>\d+); .*?workDir: .*?/work/(?P<id>[\w/]{9})"
    match = re.match(log_pattern, line)
    if not match: return

    # Get identifier
    identifier = match.group("id")

    # Get process execution
    process_execution = process_executions.get(identifier)
    if not process_execution: return

    # Get finished
    year = datetime.now().year
    process_execution.finished = datetime.strptime(
        f"{year}-{match.group('timestamp')}", "%Y-%b-%d %H:%M:%S.%f"
    )

    # Get return code
    process_execution.return_code = match.group("exit_code")

    # Get status
    status = match.group("status") or "-"
    if process_execution.return_code == "1": status = "FAILED"
    process_execution.status = status


def update_process_execution_from_path(process_execution, execution_path):
    if not process_execution.path: return
    full_path = os.path.join(execution_path, "work", process_execution.path)
    process_execution.stdout = get_file_text(os.path.join(full_path, ".command.out"))
    process_execution.stderr = get_file_text(os.path.join(full_path, ".command.err"))
    process_execution.bash = get_file_text(os.path.join(full_path, ".command.sh"))








def get_process_executions(log, execution_path):
    """Creates a list of process executions from a log.
    
    :param str log: the log text.
    :param str execution_path: the location of the execution.
    :rtype: ``list`` of ``nextflow.models.ProcessExecution``"""

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
    """Creates a process execution from a log and its ID.
    
    :param str process_id: the ID of the process.
    :param str path: the path of the process.
    :param str log: the log text.
    :param str execution_path: the location of the execution.
    :rtype: ``nextflow.models.ProcessExecution``"""
    
    stdout, stderr, returncode, bash = "", "", "", ""
    if path:
        full_path = os.path.join(execution_path, "work", path)
        stdout = get_file_text(os.path.join(full_path, ".command.out"))
        stderr = get_file_text(os.path.join(full_path, ".command.err"))
        returncode = get_file_text(os.path.join(full_path, ".exitcode"))
        bash = get_file_text(os.path.join(full_path, ".command.sh"))
    name = get_process_name_from_log(log, process_id)
    return ProcessExecution(
        identifier=process_id, name=name,
        process=name[:name.find("(") - 1] if "(" in name else name,
        path=path, stdout=stdout, stderr=stderr, return_code=returncode,
        bash=bash,
        started=get_process_start_from_log(log, process_id),
        finished=get_process_end_from_log(log, process_id),
        status=get_process_status_from_log(log, process_id),
    )