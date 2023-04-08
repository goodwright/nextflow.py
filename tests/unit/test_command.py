from unittest import TestCase
from unittest.mock import patch, Mock, MagicMock
from nextflow.command import *
from nextflow.command import _run

class RunTests(TestCase):

    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_with_default_values(self, mock_ex, mock_sleep, mock_run, mock_nc):
        executions = list(_run("main.nf"))
        mock_nc.assert_called_with(os.path.abspath("."), "main.nf", None, None, None, None)
        mock_run.assert_called_with(
            mock_nc.return_value,
            universal_newlines=True, shell=True
        )
        mock_sleep.assert_called_with(1)
        mock_ex.assert_called_with(os.path.abspath("."), mock_nc.return_value)
        self.assertEqual(executions, [mock_ex.return_value])
    

    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_with_custom_values(self, mock_ex, mock_sleep, mock_run, mock_nc):
        mock_executions = [Mock(return_code=""), Mock(return_code="0")]
        mock_ex.side_effect = [None, *mock_executions]
        executions = list(_run(
            "main.nf", run_path="/exdir", version="21.10", configs=["conf1"],
            params={"param": "2"}, profiles=["docker"], sleep=4
        ))
        mock_nc.assert_called_with("/exdir", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"])
        mock_run.assert_called_with(
            mock_nc.return_value,
            universal_newlines=True, shell=True
        )
        mock_sleep.assert_called_with(4)
        self.assertEqual(mock_sleep.call_count, 3)
        mock_ex.assert_called_with("/exdir", mock_nc.return_value)
        self.assertEqual(mock_ex.call_count, 3)
        self.assertEqual(executions, [mock_executions[1]])


    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_and_poll(self, mock_ex, mock_sleep, mock_run, mock_nc):
        mock_run.return_value.poll.side_effect = [None, None, 1]
        mock_executions = [Mock(finished=False), Mock(finished=True)]
        mock_ex.side_effect = [None, *mock_executions]
        executions = list(_run("main.nf", poll=True))
        mock_nc.assert_called_with(os.path.abspath("."), "main.nf", None, None, None, None)
        mock_run.assert_called_with(
            mock_nc.return_value,
            universal_newlines=True, shell=True
        )
        mock_sleep.assert_called_with(1)
        self.assertEqual(mock_sleep.call_count, 3)
        mock_ex.assert_called_with(os.path.abspath("."), mock_nc.return_value)
        self.assertEqual(mock_ex.call_count, 3)
        self.assertEqual(executions, mock_executions)
    

    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_with_custom_runner(self, mock_ex, mock_sleep, mock_run, mock_nc):
        runner = MagicMock()
        executions = list(_run("main.nf", runner=runner))
        mock_nc.assert_called_with(os.path.abspath("."), "main.nf", None, None, None, None)
        self.assertFalse(mock_run.called)
        runner.assert_called_with(mock_nc.return_value)
        mock_sleep.assert_called_with(1)
        mock_ex.assert_called_with(os.path.abspath("."), mock_nc.return_value)
        self.assertEqual(executions, [mock_ex.return_value])
    

    @patch("nextflow.command._run")
    def test_can_run_without_poll(self, mock_run):
        mock_run.return_value = [Mock(finished=True)]
        execution = run(1, 2, a=3)
        self.assertEqual(execution, mock_run.return_value[0])
        mock_run.assert_called_with(1, 2, a=3, poll=False)
    

    @patch("nextflow.command._run")
    def test_can_run_with_poll(self, mock_run):
        mock_run.return_value = [Mock(finished=False), Mock(finished=True)]
        executions = list(run_and_poll(1, 2, a=3))
        self.assertEqual(executions, mock_run.return_value)
        mock_run.assert_called_with(1, 2, a=3, poll=True)



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



class GetExecutionTests(TestCase):

    @patch("nextflow.command.get_file_text")
    @patch("nextflow.command.get_started_from_log")
    @patch("nextflow.command.get_finished_from_log")
    @patch("nextflow.command.get_process_executions")
    @patch("nextflow.command.Execution")
    def test_can_get_execution(self, mock_ex, mock_proc, mock_fin, mock_start, mock_file):
        mock_file.side_effect = [
            "log text [xx_yy]", "ok", "bad", "9"
        ]
        mock_ex.return_value = Mock(process_executions=[Mock(execution=None), Mock(execution=None)])
        execution = get_execution("/ex", "a=b; nf run >stdout.txt 2>stderr.txt")
        self.assertEqual([c[0] for c in mock_file.call_args_list], [
            (os.path.join("/ex", ".nextflow.log"),),
            (os.path.join("/ex", "stdout.txt"),),
            (os.path.join("/ex", "stderr.txt"),),
            (os.path.join("/ex", "rc.txt"),),
        ])
        mock_start.assert_called_with("log text [xx_yy]")
        mock_fin.assert_called_with("log text [xx_yy]")
        mock_proc.assert_called_with("log text [xx_yy]", "/ex")
        mock_ex.assert_called_with(
            identifier="xx_yy",
            stdout="ok",
            stderr="bad",
            return_code="9",
            started=mock_start.return_value,
            finished=mock_fin.return_value,
            command="nf run",
            log="log text [xx_yy]",
            path="/ex",
            process_executions=mock_proc.return_value,
        )
        self.assertEqual(execution, mock_ex.return_value)
        for proc in mock_ex.return_value.process_executions:
            self.assertIs(proc.execution, mock_ex.return_value)
    

    @patch("nextflow.command.get_file_text")
    @patch("nextflow.command.get_started_from_log")
    @patch("nextflow.command.get_finished_from_log")
    @patch("nextflow.command.get_process_executions")
    @patch("nextflow.command.Execution")
    def test_can_get_no_execution(self, mock_ex, mock_proc, mock_fin, mock_start, mock_file):
        mock_file.return_value = None
       
        self.assertIsNone(get_execution("/ex", "nf run >stdout.txt 2>stderr.txt"))
        self.assertEqual([c[0] for c in mock_file.call_args_list], [
            (os.path.join("/ex", ".nextflow.log"),),
        ])
        self.assertFalse(mock_start.called)
        self.assertFalse(mock_fin.called)
        self.assertFalse(mock_proc.called)
        self.assertFalse(mock_ex.called)



class GetProcessExecutionsTests(TestCase):

    @patch("nextflow.command.get_process_ids_to_paths")
    @patch("nextflow.command.get_process_execution")
    def test_can_get_process_executions(self, mock_proc, mock_ids):
        log = "line1\nSubmitted process a/bb\n..[ab/123456] Submitted process\n[cd/789012] Submitted process"
        mock_ids.return_value = {
            "ab/123456": "/ex/ab/123456aa",
            "cd/789012": "/ex/cd/789012bb",
        }
        process_executions = get_process_executions(log, "/ex")
        mock_ids.assert_called_with(["ab/123456", "cd/789012"], "/ex")
        self.assertEqual([c[0] for c in mock_proc.call_args_list], [
            ("ab/123456", "/ex/ab/123456aa", log, "/ex"),
            ("cd/789012", "/ex/cd/789012bb", log, "/ex"),
        ])
        self.assertEqual(process_executions, [mock_proc.return_value, mock_proc.return_value])



class GetProcessExecutionTests(TestCase):

    @patch("nextflow.command.get_file_text")
    @patch("nextflow.command.get_process_name_from_log")
    @patch("nextflow.command.get_process_start_from_log")
    @patch("nextflow.command.get_process_end_from_log")
    @patch("nextflow.command.get_process_status_from_log")
    @patch("nextflow.command.ProcessExecution")
    def test_can_get_process_execution(self, mock_proc, mock_status, mock_end, mock_start, mock_name, mock_file):
        mock_name.return_value = "PROC (123)"
        mock_file.side_effect = [
            "ok", "bad", "9", "$"
        ]
        process_execution = get_process_execution("xx_yy", "aa/bb", "log text", "/ex")
        self.assertEqual([c[0] for c in mock_file.call_args_list], [
            (os.path.join("/ex", "work", "aa/bb", ".command.out"),),
            (os.path.join("/ex", "work", "aa/bb", ".command.err"),),
            (os.path.join("/ex", "work", "aa/bb", ".exitcode"),),
            (os.path.join("/ex", "work", "aa/bb", ".command.sh"),),
        ])
        mock_name.assert_called_with("log text", "xx_yy")
        mock_proc.assert_called_with(
            identifier="xx_yy",
            name="PROC (123)",
            process="PROC",
            path="aa/bb",
            stdout="ok",
            stderr="bad",
            return_code="9",
            bash="$",
            started=mock_start.return_value,
            finished=mock_end.return_value,
            status=mock_status.return_value,
        )
        self.assertEqual(process_execution, mock_proc.return_value)
    

    @patch("nextflow.command.get_file_text")
    @patch("nextflow.command.get_process_name_from_log")
    @patch("nextflow.command.get_process_start_from_log")
    @patch("nextflow.command.get_process_end_from_log")
    @patch("nextflow.command.get_process_status_from_log")
    @patch("nextflow.command.ProcessExecution")
    def test_can_get_process_execution_without_path(self, mock_proc, mock_status, mock_end, mock_start, mock_name, mock_file):
        mock_name.return_value = "PROC 1"
        mock_file.side_effect = [
            "ok", "bad", "9", "$"
        ]
        process_execution = get_process_execution("xx_yy", "", "log text", "/ex")
        self.assertEqual([c[0] for c in mock_file.call_args_list], [])
        mock_name.assert_called_with("log text", "xx_yy")
        mock_proc.assert_called_with(
            identifier="xx_yy",
            name="PROC 1",
            process="PROC 1",
            path="",
            stdout="",
            stderr="",
            return_code="",
            bash="",
            started=mock_start.return_value,
            finished=mock_end.return_value,
            status=mock_status.return_value,
        )
        self.assertEqual(process_execution, mock_proc.return_value)
        
        
       
