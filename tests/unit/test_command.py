from unittest import TestCase
from unittest.mock import patch, PropertyMock
from nextflow.command import *
from nextflow.command import _run

class RunTests(TestCase):

    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("nextflow.command.get_execution")
    def test_can_run(self, mock_ex, mock_run, mock_nc):
        executions = list(_run(
            "main.nf", run_path="/exdir", version="21.10", configs=["conf1"],
            params={"param": "2"}, profiles=["docker"]
        ))
        mock_nc.assert_called_with("/exdir", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"])
        mock_run.assert_called_with(
            mock_nc.return_value, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True
        )
        mock_ex.assert_called_with("/exdir", "user@host", mock_nc.return_value)
        self.assertEqual(executions, [mock_ex.return_value])



class NextflowCommandTests(TestCase):

    @patch("nextflow.command.make_nextflow_command_env_string")
    @patch("nextflow.command.make_nextflow_command_config_string")
    @patch("nextflow.command.make_nextflow_command_params_string")
    @patch("nextflow.command.make_nextflow_command_profiles_string")
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
        self.assertEqual(command, "cd /exdir; A=B C=D nextflow -Duser.country=US -c conf1 -c conf2 run main.nf --p1=10 --p2=20 -profile docker,test >stdout.txt 2>stderr.txt; echo $? >rc.txt")
    

    @patch("nextflow.command.make_nextflow_command_env_string")
    @patch("nextflow.command.make_nextflow_command_config_string")
    @patch("nextflow.command.make_nextflow_command_params_string")
    @patch("nextflow.command.make_nextflow_command_profiles_string")
    def test_can_get_minimal_nextflow_command(self, mock_prof, mock_params, mock_conf, mock_env):
        mock_env.return_value = ""
        mock_conf.return_value = ""
        mock_params.return_value = ""
        mock_prof.return_value = ""
        command = make_nextflow_command(None, "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"])
        mock_env.assert_called_with("21.10")
        mock_conf.assert_called_with(["conf1"])
        mock_params.assert_called_with({"param": "2"})
        mock_prof.assert_called_with(["docker"])
        self.assertEqual(command, "nextflow -Duser.country=US run main.nf >stdout.txt 2>stderr.txt; echo $? >rc.txt")



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



class ProcessIdsToPathsTest(TestCase):

    @patch("subprocess.run")
    def test_can_get_local_paths(self, mock_run):
        process_ids = ["ab/123456", "cd/7890123"]
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "/ex/work/xx\n/ex/work/xx/yyyyyyy\n/ex/work/cd\n/ex/work/cd/789012345678"
        paths = get_process_ids_to_paths(process_ids, "/ex")
        self.assertEqual(paths, {"cd/7890123": "cd/789012345678"})
        mock_run.assert_called_with(
            f"find {os.path.join('/ex', 'work')} -type d",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
        )
    

    @patch("subprocess.run")
    def test_can_handle_command_fail(self, mock_run):
        process_ids = ["ab/123456", "cd/7890123"]
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = "/ex/work/xx\n/ex/work/xx/yyyyyyy\n/ex/work/cd\n/ex/work/cd/789012345678"
        paths = get_process_ids_to_paths(process_ids, "/ex")
        self.assertEqual(paths, {})
        mock_run.assert_called_with(
            f"find {os.path.join('/ex', 'work')} -type d",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
        )



