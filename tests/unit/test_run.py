from unittest import TestCase
from unittest.mock import patch, PropertyMock
from nextflow.run import *

class RunTests(TestCase):

    @patch("nextflow.run.make_nextflow_command")
    @patch("nextflow.run.make_run_command")
    @patch("nextflow.run.create_script")
    @patch("subprocess.run")
    @patch("nextflow.run.get_execution")
    def test_can_run(self, mock_ex, mock_run, mock_script, mock_rc, mock_nc):
        execution = run(
            "main.nf", run_path="/exdir", script_path="/run.sh", script_contents="line1",
            remote="user@host", shell="bash", version="21.10", configs=["conf1"],
            params={"param": "2"}, profiles=["docker"]
        )
        mock_nc.assert_called_with("/exdir", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"])
        mock_rc.assert_called_with(mock_nc.return_value, "user@host", "/run.sh", "bash")
        mock_script.assert_called_with(mock_nc.return_value, "line1", "/run.sh", "user@host")
        mock_run.assert_called_with(
            mock_rc.return_value, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True
        )
        mock_ex.assert_called_with("/exdir", "user@host")
        self.assertEqual(execution, mock_ex.return_value)



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
        self.assertEqual(command, "cd /exdir; A=B C=D nextflow -Duser.country=US -c conf1 -c conf2 run main.nf --p1=10 --p2=20 -profile docker,test >stdout.txt 2>stderr.txt; echo $? >rc.txt")
    

    @patch("nextflow.run.make_nextflow_command_env_string")
    @patch("nextflow.run.make_nextflow_command_config_string")
    @patch("nextflow.run.make_nextflow_command_params_string")
    @patch("nextflow.run.make_nextflow_command_profiles_string")
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



class CreateScriptTests(TestCase):

    @patch("builtins.open")
    def test_can_create_local_script(self, mock_open):
        create_script("nextflow run", "command1\ncommand2", "/path/script.sh")
        mock_open.assert_called_with("/path/script.sh", "w")
        mock_open.return_value.__enter__.return_value.write.assert_called_with("command1\ncommand2\n\n\nnextflow run")
    

    @patch("subprocess.run")
    def test_can_create_remote_script(self, mock_run):
        create_script("nextflow run", "command1\n\"command2\"", "/path/script.sh", "user@host")
        mock_run.assert_called_with(
            'echo "command1\n\\"command2\\"\n\n\nnextflow run" | ssh user@host "cat > /path/script.sh"',
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )



class FileTextTests(TestCase):

    @patch("builtins.open")
    def test_can_get_local_log_text(self, mock_open):
        mock_open.return_value.__enter__.return_value.read.return_value = "line1\nline2"
        self.assertEqual(get_file_text("/ex/file.txt", ""), "line1\nline2")
        mock_open.assert_called_with("/ex/file.txt", "r")
    

    @patch("builtins.open")
    def test_can_can_handle_no_log_text(self, mock_open):
        mock_open.side_effect = FileNotFoundError
        self.assertEqual(get_file_text("/ex/file.txt", ""), "")
        mock_open.assert_called_with("/ex/file.txt", "r")
    

    @patch("subprocess.run")
    def test_can_get_remote_log_text(self, mock_run):
        mock_run.return_value.stdout = b"line1\nline2"
        mock_run.return_value.returncode = 0
        self.assertEqual(get_file_text("/ex/file.txt", "user@host"), "line1\nline2")
        mock_run.assert_called_with(
            "ssh user@host 'cat /ex/file.txt'",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    

    @patch("subprocess.run")
    def test_can_handle_error_getting_remote_log_text(self, mock_run):
        mock_run.return_value.returncode = 1
        self.assertEqual(get_file_text("/ex/file.txt", "user@host"), "")
        mock_run.assert_called_with(
            "ssh user@host 'cat /ex/file.txt'",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )



class LogStartedTests(TestCase):

    def test_can_handle_no_log_text(self):
        self.assertFalse(get_started_from_log(""))
        

    @patch("nextflow.run.get_datetime_from_line")
    def test_can_get_started_datetime(self, mock_datetime):
        mock_datetime.return_value = datetime(2020, 1, 1, 1, 1, 1)
        self.assertEqual(get_started_from_log("line1\nline2"), datetime(2020, 1, 1, 1, 1, 1))
        mock_datetime.assert_called_with("line1")



class LogFinishedTests(TestCase):

    def test_can_handle_no_log_text(self):
        self.assertFalse(get_finished_from_log(""))
    

    @patch("nextflow.run.log_is_finished")
    def test_can_handle_no_nextflow_finished_text(self, mock_finished):
        mock_finished.return_value = False
        self.assertFalse(get_finished_from_log("line1\nline2"))
        mock_finished.assert_called_with("line1\nline2")
    

    @patch("nextflow.run.log_is_finished")
    @patch("nextflow.run.get_datetime_from_line")
    def test_can_get_finished_datetime(self, mock_datetime, mock_finished):
        mock_finished.return_value = True
        mock_datetime.side_effect = [None, datetime(2020, 1, 1, 1, 1, 1)]
        self.assertEqual(get_finished_from_log("line1\nline2\nline3"), datetime(2020, 1, 1, 1, 1, 1))
        mock_finished.assert_called_with("line1\nline2\nline3")
        self.assertEqual(mock_datetime.call_count, 2)
        mock_datetime.assert_any_call("line3")
        mock_datetime.assert_any_call("line2")


class LogIsFinishedTests(TestCase):

    def test_can_handle_no_log_text(self):
        self.assertFalse(log_is_finished(""))
    

    def test_can_handle_no_nextflow_finished_text(self):
        self.assertFalse(log_is_finished("line1\nline2"))
    

    def test_can_handle_nextflow_finished_text(self):
        self.assertTrue(log_is_finished("line1\nline2\ - > Execution complete -- Goodbye\n"))
    

    def test_can_handle_java_error_with_spaces(self):
        text = "line1\nline2\njava.nio.file.NoSuchFileException: /media/\n	at 1"
        self.assertTrue(log_is_finished(text))
    

    def test_can_handle_java_error_with_tabs(self):
        text = "line1\nline2\njava.nio.file.NoSuchFileException: /media/\n\tat 1"
        self.assertTrue(log_is_finished(text))



class DatetimeFromLineTests(TestCase):

    def test_can_get_datetime_from_line(self):
        self.assertEqual(
            get_datetime_from_line("Mar-29 02:48:56.642 [main] DEBUG"),
            datetime(datetime.now().year, 3, 29, 2, 48, 56, 642000)
        )
    

    def test_can_get_datetime_from_line_with_no_datetime(self):
        self.assertEqual(get_datetime_from_line("DEBUG"), None)



class ProcessIdsToPathsTest(TestCase):

    @patch("subprocess.run")
    def test_can_get_local_paths(self, mock_run):
        process_ids = ["ab/123456", "cd/7890123"]
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "/ex/work/xx\n/ex/work/xx/yyyyyyy\n/ex/work/cd\n/ex/work/cd/789012345678"
        paths = get_process_ids_to_paths(process_ids, "/ex", "")
        self.assertEqual(paths, {"cd/7890123": "cd/789012345678"})
        mock_run.assert_called_with(
            f"find {os.path.join('/ex', 'work')} -type d",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
        )
    

    @patch("subprocess.run")
    def test_can_get_remote_paths(self, mock_run):
        process_ids = ["ab/123456", "cd/7890123"]
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "/ex/work/xx\n/ex/work/xx/yyyyyyy\n/ex/work/cd\n/ex/work/cd/789012345678"
        paths = get_process_ids_to_paths(process_ids, "/ex", "user@host")
        self.assertEqual(paths, {"cd/7890123": "cd/789012345678"})
        mock_run.assert_called_with(
            f"ssh user@host \"find {os.path.join('/ex', 'work')} -type d\"",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
        )
    

    @patch("subprocess.run")
    def test_can_handle_command_fail(self, mock_run):
        process_ids = ["ab/123456", "cd/7890123"]
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = "/ex/work/xx\n/ex/work/xx/yyyyyyy\n/ex/work/cd\n/ex/work/cd/789012345678"
        paths = get_process_ids_to_paths(process_ids, "/ex", "")
        self.assertEqual(paths, {})
        mock_run.assert_called_with(
            f"find {os.path.join('/ex', 'work')} -type d",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True
        )



class ProcessNameFromLogTests(TestCase):

    def setUp(self):
        self.log = (
            "Jun-02 19:39:54.493 [main] DEBUG nextflow.cli.CmdRun -\n"
            "Jun-01 16:45:57.048 [Task submitter] INFO  nextflow.Session - [d6/31d530] Submitted process > DEMULTIPLEX:CSV_TO_BARCODE (file.csv)\n"
            "Jun-01 16:46:08.965 [Task submitter] INFO  nextflow.Session - [99/6165a9] Submitted process > DEMULTIPLEX:FASTQC (sample)\n"
            "Jun-01 16:46:13.434 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye"
        )

    def test_can_get_process_name(self):
        self.assertEqual(
            get_process_name_from_log(self.log, "99/6165a9"),
            "DEMULTIPLEX:FASTQC (sample)"
        )
    

    def test_can_get_no_process_name(self):
        self.assertIsNone(get_process_name_from_log(self.log, "88/6165a9"))