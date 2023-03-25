import os
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
    return command.strip()


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
        text = text.replace("'", "\\'")
        ssh_command = f"echo '{text}' | ssh {remote} 'cat > {script_path}'"
        subprocess.run(
            ssh_command, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    else:
        with open(script_path, "w") as f:
            f.write(text)
    return script_path