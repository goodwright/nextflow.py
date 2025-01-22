import os
import re
import time
import subprocess
from nextflow.io import get_file_text, get_process_ids_to_paths
from nextflow.models import Execution, ProcessExecution
from nextflow.log import (
    get_started_from_log,
    get_finished_from_log,
    get_identifier_from_log,
    parse_submitted_line,
    parse_completed_line,
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
    execution, log_start = None, 0
    while True:
        time.sleep(sleep)
        execution, diff = get_execution(
            output_path or run_path, nextflow_command, execution, log_start
        )
        log_start += diff
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


def get_execution(execution_path, nextflow_command, execution=None, log_start=0):
    """Creates an execution object from a location. If you are polling, you can
    pass in the previous execution to update it with new information.
    
    :param str execution_path: the location of the execution.
    :param str nextflow_command: the command used to run the pipeline.
    :rtype: ``nextflow.models.Execution``"""

    log = get_file_text(os.path.join(execution_path, ".nextflow.log"))
    if not log: return None, 0
    log = log[log_start:]
    execution = make_or_update_execution(log, execution_path, nextflow_command, execution)
    process_executions, changed = get_initial_process_executions(log, execution)
    to_check = [k for k, v in process_executions.items() if not v.path]
    process_ids_to_paths = get_process_ids_to_paths(to_check, execution_path)
    for process_id, path in process_ids_to_paths.items():
        process_executions[process_id].path = path
    for process_execution in process_executions.values():
        if not process_execution.finished or process_execution.identifier in changed:
            update_process_execution_from_path(process_execution, execution_path)
    execution.process_executions = list(process_executions.values())
    return execution, len(log)


def make_or_update_execution(log, execution_path, nextflow_command, execution):
    """Creates an Execution object from a log file, or updates an existing one
    from a previous poll.

    :param str log: a section of the log file.
    :param str execution_path: the location of the execution.
    :param str nextflow_command: the command used to run the pipeline.
    :param nextflow.models.Execution execution: the existing execution.
    :rtype: ``nextflow.models.Execution``"""

    if not execution:
        command = sorted(nextflow_command.split(";"), key=len)[-1]
        command = re.sub(r">[a-zA-Z0-9\/-]+?stdout\.txt", "", command)
        command = re.sub(r"2>[a-zA-Z0-9\/-]+?stderr\.txt", "", command).strip()
        execution = Execution(
            identifier="", stdout="", stderr="", return_code="",
            started=None, finished=None, command=command, log="",
            path=execution_path, process_executions=[],
        )
    if not execution.identifier: execution.identifier = get_identifier_from_log(log)
    if not execution.started: execution.started = get_started_from_log(log)
    if not execution.finished: execution.finished = get_finished_from_log(log)
    execution.log += log
    execution.stdout = get_file_text(os.path.join(execution_path, "stdout.txt"))
    execution.stderr = get_file_text(os.path.join(execution_path, "stderr.txt"))
    execution.return_code = get_file_text(os.path.join(execution_path, "rc.txt")).rstrip()
    return execution


def get_initial_process_executions(log, execution):
    """Parses a section of a log file and looks for new process executions not
    currently in the list, or uncompleted ones which can now be completed. Some
    attributes are not yet filled in.

    The identifiers of the proccess executions seen are returned.
    
    :param str log: a section of the log file.
    :param nextflow.models.Execution execution: the containing execution.
    :rtype: ``tuple``"""

    lines = log.splitlines()
    process_executions = {p.identifier: p for p in execution.process_executions}
    just_updated= []
    for line in lines:
        if "Submitted process" in line:
            proc_ex = create_process_execution_from_line(line)
            if not proc_ex: continue
            proc_ex.execution = execution
            process_executions[proc_ex.identifier] = proc_ex
            just_updated.append(proc_ex.identifier)
        elif "Task completed" in line:
            just_updated.append(
                update_process_execution_from_line(process_executions, line)
            )
    return process_executions, just_updated


def create_process_execution_from_line(line):
    """Creates a process execution from a line of the log file in which its
    submission is reported.
    
    :param str line: a line from the log file.
    :rtype: ``nextflow.models.ProcessExecution``"""

    identifier, name, process, started = parse_submitted_line(line)
    if not identifier: return
    return ProcessExecution(
        identifier=identifier, name=name, process=process, started=started,
        path="", stdout="", stderr="", return_code="", bash="", finished=None,
        status="-",
    )


def update_process_execution_from_line(process_executions, line):
    """Updates a process execution with information from a line of the log file
    in which its completion is reported. The identifier of the process execution
    is returned.
    
    :param dict process_executions: a dictionary of process executions.
    :param str line: a line from the log file.
    :rtype: ``str``"""

    identifier, finished, return_code, status = parse_completed_line(line)
    if not identifier: return
    process_execution = process_executions.get(identifier)
    if not process_execution: return
    process_execution.finished = finished
    process_execution.return_code = return_code
    process_execution.status = status
    return identifier


def update_process_execution_from_path(process_execution, execution_path):
    """Some attributes of a process execution need to be obtained from files on
    disk. This function updates the process execution with these values.
    
    :param nextflow.models.ProcessExecution process_execution: the process execution.
    :param str execution_path: the location of the containing execution."""

    if not process_execution.path: return
    full_path = os.path.join(execution_path, "work", process_execution.path)
    process_execution.stdout = get_file_text(os.path.join(full_path, ".command.out"))
    process_execution.stderr = get_file_text(os.path.join(full_path, ".command.err"))
    if not process_execution.bash:
        process_execution.bash = get_file_text(os.path.join(full_path, ".command.sh"))
    if process_execution.execution.finished and not process_execution.return_code:
        process_execution.return_code = process_execution.execution.return_code