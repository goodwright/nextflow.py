import os
import re
import time
import weakref
import subprocess
from datetime import datetime
from nextflow.io import get_file_text, get_process_ids_to_paths, get_file_creation_time
from nextflow.models import Execution, ProcessExecution, ExecutionSubmission
from nextflow.log import (
    get_started_from_log,
    get_finished_from_log,
    get_identifier_from_log,
    get_session_uuid_from_log,
    parse_cached_line,
    parse_submitted_line,
    parse_completed_line,
)

def run(*args, **kwargs):
    """Runs a pipeline and returns the execution.

    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param str run_path: the location to run the pipeline in (if not current directory).
    :param str output_path: the location to store the output in (if not run path).
    :param str log_path: the location to store the log in (if not output path).
    :param resume: whether to resume an existing execution.
    :param function runner: a function to run the pipeline command.
    :param str version: the nextflow version to use.
    :param str java_home: the path to the Java installation to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :param str timezone: the timezone to use for the log.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :param str trace: the filename to use for the trace report.
    :rtype: ``nextflow.models.Execution``"""

    return list(_run(*args, poll=False, **kwargs))[0]


def run_and_poll(*args, **kwargs):
    """Runs a pipeline and polls it for updates. Yields the execution after each
    update.

    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param str run_path: the location to run the pipeline in (if not current directory).
    :param str output_path: the location to store the output in (if not run path).
    :param str log_path: the location to store the log in (if not output path).
    :param resume: whether to resume an existing execution.
    :param function runner: a function to run the pipeline command.
    :param io: an optional custom io object to handle file operations.
    :param str version: the nextflow version to use.
    :param str java_home: the path to the Java installation to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :param str timezone: the timezone to use for the log.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :param str trace: the filename to use for the trace report.
    :param int sleep: the number of seconds to wait between polls.
    :rtype: ``nextflow.models.Execution``"""

    for execution in _run(*args, poll=True, **kwargs):
        yield execution


def _run(
        pipeline_path, resume=False, poll=False, run_path=None, output_path=None,
        log_path=None, runner=None, io=None, java_home=None,
        version=None, configs=None, params=None, profiles=None, timezone=None,
        report=None, timeline=None, dag=None, trace=None, sleep=1
):
    submission = submit_execution(
        pipeline_path=pipeline_path,
        resume=resume,
        run_path=run_path,
        output_path=output_path,
        log_path=log_path,
        runner=runner,
        io=io,
        version=version,
        java_home=java_home,
        configs=configs,
        dag=dag,
        trace=trace,
        timeline=timeline,
        report=report,
        profiles=profiles,
        timezone=timezone,
        params=params
    )

    execution, log_start = None, 0
    while True:
        time.sleep(sleep)
        execution, diff = get_execution(
            submission.output_path, submission.log_path, submission.nextflow_command, execution, log_start, timezone, io
        )
        log_start += diff
        if execution and poll: yield execution
        if execution and execution.return_code and execution.finished:
            if not poll: yield execution
            break


def submit_execution(
        pipeline_path,
        resume=False,
        run_path=None,
        output_path=None,
        log_path=None,
        runner=None,
        io=None,
        version=None,
        java_home=None,
        configs=None,
        params=None,
        profiles=None,
        timezone=None,
        report=None,
        timeline=None,
        dag=None,
        trace=None,
):
    """Submits an execution and returns information about that submission as an
    `ExecutionSubmission` object.

    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param str run_path: the location to run the pipeline in (if not current directory).
    :param str output_path: the location to store the output in (if not run path).
    :param str log_path: the location to store the log in (if not output path).
    :param resume: whether to resume an existing execution.
    :param function runner: a function to run the pipeline command.
    :param io: an optional custom io object to handle file operations.
    :param str version: the nextflow version to use.
    :param str java_home: the path to the Java installation to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :param str timezone: the timezone to use for the log.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :param str trace: the filename to use for the trace report.
    :rtype: ``nextflow.models.ExecutionSubmission``"""

    if not run_path and not io: run_path = os.path.abspath(".")
    if not run_path and io: run_path = io.abspath(".")
    if not output_path: output_path = run_path
    if not log_path: log_path = output_path
    nextflow_command = make_nextflow_command(
        run_path, output_path, log_path, pipeline_path, resume, version, java_home,
        configs, params, profiles, timezone, report, timeline, dag, trace, io
    )
    start = datetime.now()
    if runner:
        runner(nextflow_command)
    else:
        process = subprocess.Popen(
            nextflow_command, universal_newlines=True, shell=True
        )
        process._child_created = False  # disables warning machinery
        weakref.finalize(process, lambda: None)
    submission = ExecutionSubmission(
        pipeline_path, run_path, output_path, log_path, nextflow_command, timezone
    )
    if resume:
        wait_for_log_creation(submission.log_path, start, io)
    return submission


def make_nextflow_command(run_path, output_path, log_path, pipeline_path, resume, version, java_home, configs, params, profiles, timezone, report, timeline, dag, trace, io):
    """Generates the `nextflow run` commmand.

    :param str run_path: the location to run the pipeline in.
    :param str output_path: the location to store the output in.
    :param str log_path: the location to store the log in.
    :param str pipeline_path: the absolute path to the pipeline .nf file.
    :param bool resume: whether to resume an existing execution.
    :param str version: the nextflow version to use.
    :param str java_home: the path to the Java installation to use.
    :param list configs: any config files to be applied.
    :param dict params: the parameters to pass.
    :param list profiles: any profiles to be applied.
    :param str timezone: the timezone to use.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :param str trace: the filename to use for the trace report.
    :param io: an optional custom io object to handle file operations.
    :rtype: ``str``"""

    env = make_nextflow_command_env_string(version, timezone, output_path, run_path, java_home)
    if env: env += " "
    nf = "nextflow -Duser.country=US"
    log = make_nextflow_command_log_string(log_path, run_path)
    if log: log += " "
    configs = make_nextflow_command_config_string(configs)
    if configs: configs += " "
    resume = make_nextflow_command_resume_string(resume)
    if resume: resume = f"{resume} "
    params = make_nextflow_command_params_string(params)
    profiles = make_nextflow_command_profiles_string(profiles)
    reports = make_reports_string(output_path, report, timeline, dag, trace)
    command = f"{env}{nf} {log}{configs}run {pipeline_path} {resume}{params} {profiles} {reports}"
    abspath = io.abspath if io else os.path.abspath
    if run_path != abspath("."): command = f"cd {run_path}; {command}"
    prefix = (str(output_path) + os.path.sep) if output_path != run_path else ""
    command = command.rstrip() + f" >{prefix}"
    command += f"stdout.txt 2>{prefix}"
    command += f"stderr.txt; echo $? >{prefix}rc.txt"
    return command


def make_nextflow_command_env_string(version, timezone, output_path, run_path, java_home):
    """Creates the environment variable setting portion of the nextflow run
    command string.

    :param str version: the nextflow version to use.
    :param str timezone: the timezone to use.
    :param str output_path: the location to store the output in.
    :param str run_path: the location to run the pipeline in.
    :param str java_home: the path to the Java installation to use.
    :rtype: ``str``"""

    env = {"NXF_ANSI_LOG": "false"}
    if version: env["NXF_VER"] = version
    if timezone: env["TZ"] = timezone
    if output_path != run_path: env["NXF_WORK"] = os.path.join(output_path, "work")
    if java_home: env["JAVA_HOME"] = java_home
    return " ".join([f"{k}={v}" for k, v in env.items()])


def make_nextflow_command_log_string(log_path, run_path):
    """Creates the log setting portion of the nextflow run command string.

    :param str log_path: the location to store the log file in.
    :rtype: ``str``"""

    if log_path == run_path: return ""
    return f"-log '{os.path.join(log_path, '.nextflow.log')}'"


def make_nextflow_command_config_string(configs):
    """Creates the config setting portion of the nextflow run command string.
    Absolute paths are recommended.

    :param str version: the nextflow version to use.
    :rtype: ``str``"""

    if configs is None: configs = []
    return " ".join(f"-c \"{c}\"" for c in configs)


def make_nextflow_command_resume_string(resume):
    """Creates the resume setting portion of the nextflow run command string.

    :param resume: whether to resume an existing execution.
    :rtype: ``str``"""

    if not resume: return ""
    if isinstance(resume, str): return f"-resume {resume}"
    return "-resume"


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


def make_reports_string(output_path, report, timeline, dag, trace):
    """Creates the report setting portion of the nextflow run command string.

    :param str output_path: the location to store the output in.
    :param str report: the filename to use for the execution report.
    :param str timeline: the filename to use for the timeline report.
    :param str dag: the filename to use for the DAG report.
    :param str trace: the filename to use for the trace report.
    :rtype: ``str``"""

    params = []
    if report: params.append(f"-with-report {report}")
    if timeline: params.append(f"-with-timeline {timeline}")
    if dag: params.append(f"-with-dag {dag}")
    if trace: params.append(f"-with-trace {trace}")
    if output_path:
        for i, param in enumerate(params):
            words = param.split(" ")
            words[1] = os.path.join(output_path, words[1])
            params[i] = " ".join(words)
    return " ".join(params)


def wait_for_log_creation(output_path, start, io):
    """Waits for a log file for this execution to be created.

    :param str output_path: the location to store the output in.
    :param datetime start: the start time.
    :param io: an optional custom io object to handle file operations."""

    while True:
        created = get_file_creation_time(os.path.join(output_path, ".nextflow.log"), io=io)
        if created and created > start: break
        time.sleep(0.1)


def get_execution(execution_path, log_path, nextflow_command, execution=None, log_start=0, timezone=None, io=None):
    """Creates an execution object from a location. If you are polling, you can
    pass in the previous execution to update it with new information.

    :param str execution_path: the location of the execution.
    :param str log_path: the location of the log.
    :param str nextflow_command: the command used to run the pipeline.
    :param nextflow.models.Execution execution: the existing execution, if any.
    :param int log_start: the number of lines already read from the log.
    :param str timezone: the timezone to use for the log.
    :param io: an optional custom io object to handle file operations.
    :rtype: ``nextflow.models.Execution``"""

    log = get_file_text(os.path.join(log_path, ".nextflow.log"), io)
    if not log: return None, 0
    log = log[log_start:]
    execution = make_or_update_execution(log, execution_path, nextflow_command, execution, io)
    process_executions, changed = get_initial_process_executions(log, execution, io)
    no_path = [k for k, v in process_executions.items() if not v.path]
    process_ids_to_paths = get_process_ids_to_paths(no_path, execution_path, io)
    for process_id, path in process_ids_to_paths.items():
        process_executions[process_id].path = path
    for process_execution in process_executions.values():
        if not process_execution.finished or not process_execution.started or \
         process_execution.identifier in changed:
            update_process_execution_from_path(process_execution, execution_path, timezone, io)
    execution.process_executions = list(process_executions.values())
    return execution, len(log)


def make_or_update_execution(log, execution_path, nextflow_command, execution, io):
    """Creates an Execution object from a log file, or updates an existing one
    from a previous poll.

    :param str log: a section of the log file.
    :param str execution_path: the location of the execution.
    :param str nextflow_command: the command used to run the pipeline.
    :param nextflow.models.Execution execution: the existing execution.
    :param io: an optional custom io object to handle file operations.
    :rtype: ``nextflow.models.Execution``"""

    if not execution:
        command = sorted(nextflow_command.split(";"), key=len)[-1]
        command = re.sub(r">[a-zA-Z0-9\/-]+?stdout\.txt", "", command)
        command = re.sub(r"2>[a-zA-Z0-9\/-]+?stderr\.txt", "", command).strip()
        execution = Execution(
            identifier="", stdout="", stderr="", return_code="",
            started=None, finished=None, command=command, log="",
            session_uuid="", path=execution_path, process_executions=[],
        )
    if not execution.identifier: execution.identifier = get_identifier_from_log(log)
    if not execution.started: execution.started = get_started_from_log(log)
    if not execution.finished: execution.finished = get_finished_from_log(log)
    if not execution.session_uuid: execution.session_uuid = get_session_uuid_from_log(log)
    execution.log += log
    execution.stdout = get_file_text(os.path.join(execution_path, "stdout.txt"), io)
    execution.stderr = get_file_text(os.path.join(execution_path, "stderr.txt"), io)
    execution.return_code = get_file_text(os.path.join(execution_path, "rc.txt"), io).rstrip()
    return execution


def get_initial_process_executions(log, execution, io):
    """Parses a section of a log file and looks for new process executions not
    currently in the list, or uncompleted ones which can now be completed. Some
    attributes are not yet filled in.

    The identifiers of the process executions seen are returned.

    :param str log: a section of the log file.
    :param nextflow.models.Execution execution: the containing execution.
    :param io: an optional custom io object to handle file operations.
    :rtype: ``tuple``"""

    lines = log.splitlines()
    process_executions = {p.identifier: p for p in execution.process_executions}
    just_updated= []
    for line in lines:
        if "Submitted process" in line or "Cached process" in line:
            is_cached = "Cached process" in line
            proc_ex = create_process_execution_from_line(line, is_cached, io)
            if not proc_ex: continue
            proc_ex.execution = execution
            process_executions[proc_ex.identifier] = proc_ex
            just_updated.append(proc_ex.identifier)
        elif "Task completed" in line:
            just_updated.append(
                update_process_execution_from_line(process_executions, line)
            )
    return process_executions, just_updated


def create_process_execution_from_line(line, cached=False, io=None):
    """Creates a process execution from a line of the log file in which its
    submission (or previous caching) is reported.

    :param str line: a line from the log file.
    :param bool cached: whether the process is cached.
    :param io: an optional custom io object to handle file operations.
    :rtype: ``nextflow.models.ProcessExecution``"""

    if cached:
        identifier, name, process = parse_cached_line(line)
        submitted = None
    else:
        identifier, name, process, submitted = parse_submitted_line(line)
    if not identifier: return
    return ProcessExecution(
        identifier=identifier, name=name, process=process, submitted=submitted,
        path="", stdout="", stderr="", bash="", started=None, finished=None,
        return_code="0" if cached else "",
        status="COMPLETED" if cached else "-",
        cached=cached, io=io
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


def update_process_execution_from_path(process_execution, execution_path, timezone=None, io=None):
    """Some attributes of a process execution need to be obtained from files on
    disk. This function updates the process execution with these values.

    :param nextflow.models.ProcessExecution process_execution: the process execution.
    :param str execution_path: the location of the containing execution.
    :param str timezone: the timezone to use for the log.
    :param io: an optional custom io object to handle file operations."""

    if not process_execution.path: return
    full_path = os.path.join(execution_path, "work", process_execution.path)
    process_execution.stdout = get_file_text(os.path.join(full_path, ".command.out"), io)
    process_execution.stderr = get_file_text(os.path.join(full_path, ".command.err"), io)
    if not process_execution.started and not process_execution.cached:
        process_execution.started = get_file_creation_time(os.path.join(full_path, ".command.begin"), timezone, io)
    if not process_execution.bash:
        process_execution.bash = get_file_text(os.path.join(full_path, ".command.sh"), io)
    if process_execution.execution.finished and not process_execution.return_code:
        process_execution.return_code = process_execution.execution.return_code