from unittest import TestCase
from unittest.mock import patch, PropertyMock
from nextflow.run import *

class RunTests(TestCase):

    @patch("nextflow.run.make_nextflow_command")
    @patch("nextflow.run.make_run_command")
    def test_can_run(self, mock_rc, mock_nc):
        run(
            "main.nf", run_path="/exdir", script_path="/run.sh", remote="user@host",
            shell="bash", version="21.10", configs=["conf1"], params={"param": "2"},
            profiles=["docker"]
        )
        mock_nc.assert_called_with("/exdir", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"])
        mock_rc.assert_called_with(mock_nc.return_value, "user@host", "/run.sh", "bash")



class NextflowCommandTests(TestCase):

    @patch("nextflow.run.make_nextflow_command_env_string")
    @patch("nextflow.run.make_nextflow_command_config_string")
    @patch("nextflow.run.make_nextflow_command_params_string")
    @patch("nextflow.run.make_nextflow_command_profiles_string")
    def test_can_get_full_nextflow_command(self, mock_prof, mock_params, mock_conf, mock_env):
        mock_env.return_value = "A=B C=D"
        mock_conf.return_value = "-c conf1 -c conf2"
        mock_params.return_value = "--p1=10 --p2=20"
        mock_prof.return_value = "-profile docker,test"
        command = make_nextflow_command("/exdir", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"])
        mock_env.assert_called_with("21.10")
        mock_conf.assert_called_with(["conf1"])
        mock_params.assert_called_with({"param": "2"})
        mock_prof.assert_called_with(["docker"])
        self.assertEqual(command, "cd /exdir; A=B C=D nextflow -Duser.country=US -c conf1 -c conf2 run main.nf --p1=10 --p2=20 -profile docker,test")
    

    @patch("nextflow.run.make_nextflow_command_env_string")
    @patch("nextflow.run.make_nextflow_command_config_string")
    @patch("nextflow.run.make_nextflow_command_params_string")
    @patch("nextflow.run.make_nextflow_command_profiles_string")
    def test_can_get_minimal_nextflow_command(self, mock_prof, mock_params, mock_conf, mock_env):
        mock_env.return_value = ""
        mock_conf.return_value = ""
        mock_params.return_value = ""
        mock_prof.return_value = ""
        command = make_nextflow_command("/exdir", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"])
        mock_env.assert_called_with("21.10")
        mock_conf.assert_called_with(["conf1"])
        mock_params.assert_called_with({"param": "2"})
        mock_prof.assert_called_with(["docker"])
        self.assertEqual(command, "cd /exdir; nextflow -Duser.country=US run main.nf")



class EnvStringTests(TestCase):

    def test_can_get_env_without_version(self):
        self.assertEqual(make_nextflow_command_env_string(None), "NXF_ANSI_LOG=false")


    def test_can_get_env_with_version(self):
        self.assertEqual(make_nextflow_command_env_string("22.1"), "NXF_ANSI_LOG=false NXF_VER=22.1")



class ConfigStringTests(TestCase):

    def test_can_handle_no_config(self):
        self.assertEqual(make_nextflow_command_config_string(None), "")
        self.assertEqual(make_nextflow_command_config_string([]), "")
    

    def test_can_handle_config(self):
        self.assertEqual(make_nextflow_command_config_string(["conf1", "conf2"]), '-c "conf1" -c "conf2"')



class ParamsStringTests(TestCase):

    def test_can_handle_no_params(self):
        self.assertEqual(make_nextflow_command_params_string(None), "")
        self.assertEqual(make_nextflow_command_params_string({}), "")
    

    def test_can_handle_params(self):
        self.assertEqual(make_nextflow_command_params_string(
            {"param": "2", "param2": "'3'", "param3": '"7"'}
        ), "--param='2' --param2='3' --param3=\"7\""
    )



class ProfilesStringTests(TestCase):

    def test_can_handle_no_profiles(self):
        self.assertEqual(make_nextflow_command_profiles_string(None), "")
        self.assertEqual(make_nextflow_command_profiles_string([]), "")
    

    def test_can_handle_profiles(self):
        self.assertEqual(make_nextflow_command_profiles_string(["docker", "test"]), "-profile docker,test")



class RunCommandTests(TestCase):

    def test_can_get_nextflow_run_command(self):
        self.assertEqual(make_run_command("nextflow run", ""), "nextflow run")
    

    def test_can_get_ssh_nextflow_run_command(self):
        self.assertEqual(
            make_run_command('nextflow run --param="1"', "user@host"),
            'ssh user@host "nextflow run --param=\\"1\\""'
        )
    

    def test_can_get_get_local_script_run(self):
        self.assertEqual(
            make_run_command("nextflow run", "", "/path/script.sh", "/zsh"),
            "cd /path && /zsh script.sh"
        )
    

    def test_can_get_get_local_script_run_with_no_parent(self):
        self.assertEqual(
            make_run_command("nextflow run", "", "script.sh", "/zsh"),
            "/zsh script.sh"
        )
    

    @patch.dict(os.environ, {"SHELL": "/zsh"}, clear=True)
    def test_can_get_get_local_script_run_with_default_shell(self):
        self.assertEqual(
            make_run_command("nextflow run", "", "/path/script.sh", ""),
            "cd /path && /zsh script.sh"
        )
    

    @patch.dict(os.environ, {}, clear=True)
    def test_can_get_get_local_script_run_with_bash_shell(self):
        self.assertEqual(
            make_run_command("nextflow run", "", "/path/script.sh", ""),
            "cd /path && /bin/bash script.sh"
        )
    

    def test_can_get_remote_script_command(self):
        self.assertEqual(
            make_run_command("nextflow run", "user@host", "/path/script.sh", "/zsh"),
            'ssh user@host "cd /path && /zsh script.sh"'
        )