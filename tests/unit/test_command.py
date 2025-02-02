from unittest import TestCase
from unittest.mock import patch, Mock, MagicMock, call
from nextflow.command import *
from nextflow.command import _run

class RunTests(TestCase):

    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_with_default_values(self, mock_ex, mock_sleep, mock_run, mock_nc):
        execution = Mock()
        mock_ex.return_value = execution, 20
        executions = list(_run("main.nf"))
        mock_nc.assert_called_with(os.path.abspath("."), None, "main.nf", None, None, None, None, None, None, None, None, None)
        mock_run.assert_called_with(
            mock_nc.return_value,
            universal_newlines=True, shell=True
        )
        mock_sleep.assert_called_with(1)
        mock_ex.assert_called_with(os.path.abspath("."), mock_nc.return_value, None, 0)
        self.assertEqual(executions, [execution])
    

    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_with_custom_values(self, mock_ex, mock_sleep, mock_run, mock_nc):
        mock_executions = [Mock(return_code=""), Mock(return_code="0")]
        mock_ex.side_effect = [[None, 0], [mock_executions[0], 40], [mock_executions[1], 20]]
        executions = list(_run(
            "main.nf", run_path="/exdir", output_path="/out", version="21.10", configs=["conf1"],
            params={"param": "2"}, profiles=["docker"], timezone="UTC", report="report.html",
            timeline="time.html", dag="dag.html", trace="trace.html", sleep=4
        ))
        mock_nc.assert_called_with("/exdir", "/out", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"], "UTC", "report.html", "time.html", "dag.html", "trace.html")
        mock_run.assert_called_with(
            mock_nc.return_value,
            universal_newlines=True, shell=True
        )
        mock_sleep.assert_called_with(4)
        self.assertEqual(mock_sleep.call_count, 3)
        mock_ex.assert_called_with("/out", mock_nc.return_value, mock_executions[0], 40)
        self.assertEqual(mock_ex.call_count, 3)
        self.assertEqual(executions, [mock_executions[1]])


    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_and_poll(self, mock_ex, mock_sleep, mock_run, mock_nc):
        mock_run.return_value.poll.side_effect = [None, None, 1]
        mock_executions = [Mock(finished=False), Mock(finished=True)]
        mock_ex.side_effect = [[None, 20], [mock_executions[0], 40], [mock_executions[1], 20]]
        executions = list(_run("main.nf", poll=True))
        mock_nc.assert_called_with(os.path.abspath("."), None, "main.nf", None, None, None, None, None, None, None, None, None)
        mock_run.assert_called_with(
            mock_nc.return_value,
            universal_newlines=True, shell=True
        )
        mock_sleep.assert_called_with(1)
        self.assertEqual(mock_sleep.call_count, 3)
        mock_ex.assert_called_with(os.path.abspath("."), mock_nc.return_value, mock_executions[0], 60)
        self.assertEqual(mock_ex.call_count, 3)
        self.assertEqual(executions, mock_executions)
    

    @patch("nextflow.command.make_nextflow_command")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("nextflow.command.get_execution")
    def test_can_run_with_custom_runner(self, mock_ex, mock_sleep, mock_run, mock_nc):
        runner = MagicMock()
        mock_ex.return_value = [Mock(finished=True), 0]
        executions = list(_run("main.nf", runner=runner))
        mock_nc.assert_called_with(os.path.abspath("."), None, "main.nf", None, None, None, None, None, None, None, None, None)
        self.assertFalse(mock_run.called)
        runner.assert_called_with(mock_nc.return_value)
        mock_sleep.assert_called_with(1)
        mock_ex.assert_called_with(os.path.abspath("."), mock_nc.return_value, None, 0)
        self.assertEqual(executions, [mock_ex.return_value[0]])
    

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
    @patch("nextflow.command.make_nextflow_command_log_string")
    @patch("nextflow.command.make_nextflow_command_config_string")
    @patch("nextflow.command.make_nextflow_command_params_string")
    @patch("nextflow.command.make_nextflow_command_profiles_string")
    @patch("nextflow.command.make_reports_string")
    def test_can_get_full_nextflow_command(self, mock_report, mock_prof, mock_params, mock_conf, mock_log, mock_env):
        mock_env.return_value = "A=B C=D"
        mock_log.return_value = "-log '.nextflow.log'"
        mock_conf.return_value = "-c conf1 -c conf2"
        mock_params.return_value = "--p1=10 --p2=20"
        mock_prof.return_value = "-profile docker,test"
        mock_report.return_value = "--dag.html"
        command = make_nextflow_command("/exdir", "/out", "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"], "UTC", "report.html", "time.html", "dag.html", "trace.html")
        mock_env.assert_called_with("21.10", "UTC", "/out")
        mock_conf.assert_called_with(["conf1"])
        mock_params.assert_called_with({"param": "2"})
        mock_prof.assert_called_with(["docker"])
        mock_report.assert_called_with("/out", "report.html", "time.html", "dag.html", "trace.html")
        self.assertEqual(command, "cd /exdir; A=B C=D nextflow -Duser.country=US -log '.nextflow.log' -c conf1 -c conf2 run main.nf --p1=10 --p2=20 -profile docker,test --dag.html >/out/stdout.txt 2>/out/stderr.txt; echo $? >/out/rc.txt")
    

    @patch("nextflow.command.make_nextflow_command_env_string")
    @patch("nextflow.command.make_nextflow_command_log_string")
    @patch("nextflow.command.make_nextflow_command_config_string")
    @patch("nextflow.command.make_nextflow_command_params_string")
    @patch("nextflow.command.make_nextflow_command_profiles_string")
    @patch("nextflow.command.make_reports_string")
    def test_can_get_minimal_nextflow_command(self, mock_report, mock_prof, mock_params, mock_conf, mock_log, mock_env):
        mock_env.return_value = ""
        mock_log.return_value = ""
        mock_conf.return_value = ""
        mock_params.return_value = ""
        mock_prof.return_value = ""
        mock_report.return_value = ""
        command = make_nextflow_command(None, None, "main.nf", "21.10", ["conf1"], {"param": "2"}, ["docker"], None, None, None, None, None)
        mock_env.assert_called_with("21.10", None, None)
        mock_conf.assert_called_with(["conf1"])
        mock_params.assert_called_with({"param": "2"})
        mock_prof.assert_called_with(["docker"])
        mock_report.assert_called_with(None, None, None, None, None)
        self.assertEqual(command, "nextflow -Duser.country=US run main.nf >stdout.txt 2>stderr.txt; echo $? >rc.txt")



class EnvStringTests(TestCase):

    def test_can_get_env_without_args(self):
        self.assertEqual(make_nextflow_command_env_string(None, None, None), "NXF_ANSI_LOG=false")


    def test_can_get_env_with_version(self):
        self.assertEqual(make_nextflow_command_env_string("22.1", None, None), "NXF_ANSI_LOG=false NXF_VER=22.1")
    

    def test_can_get_env_with_timezone(self):
        self.assertEqual(make_nextflow_command_env_string(None, "UTC", None), "NXF_ANSI_LOG=false TZ=UTC")
    

    def test_can_get_env_with_work_location(self):
        self.assertEqual(make_nextflow_command_env_string(None, None, "/out"), "NXF_ANSI_LOG=false NXF_WORK=/out/work")



class LogStringTests(TestCase):

    def test_can_handle_no_directory(self):
        self.assertEqual(make_nextflow_command_log_string(None), "")
    

    def test_can_handle_directory(self):
        self.assertEqual(make_nextflow_command_log_string("/out"), "-log '/out/.nextflow.log'")



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
        

    def test_can_handle_params_with_empty_values(self):
        self.assertEqual(make_nextflow_command_params_string(
            {"param": "2", "param2": "",}
        ), "--param='2' --param2="
    )



class ProfilesStringTests(TestCase):

    def test_can_handle_no_profiles(self):
        self.assertEqual(make_nextflow_command_profiles_string(None), "")
        self.assertEqual(make_nextflow_command_profiles_string([]), "")
    

    def test_can_handle_profiles(self):
        self.assertEqual(make_nextflow_command_profiles_string(["docker", "test"]), "-profile docker,test")



class ReportsStringTests(TestCase):

    def test_can_handle_no_reports(self):
        self.assertEqual(make_reports_string(None, None, None, None, None), "")
    

    def test_can_make_execution_report(self):
        self.assertEqual(make_reports_string(None, "report.html", None, None, None), "-with-report report.html")
    

    def test_can_make_timeline_report(self):
        self.assertEqual(make_reports_string(None, None, "time.html", None, None), "-with-timeline time.html")
    

    def test_can_make_dag_report(self):
        self.assertEqual(make_reports_string(None, None, None, "dag.html", None), "-with-dag dag.html")
    

    def test_can_make_trace_file(self):
        self.assertEqual(make_reports_string(None, None, None, None, "trace.txt"), "-with-trace trace.txt")
    

    def test_can_make_full_report(self):
        self.assertEqual(
            make_reports_string(None, "report.html", "time.html", "dag.html", "trace.txt"),
            "-with-report report.html -with-timeline time.html -with-dag dag.html -with-trace trace.txt"
        )
    

    def test_can_make_full_report_with_custom_location(self):
        self.assertEqual(
            make_reports_string("/out", "report.html", "time.html", "dag.html", "trace.txt"),
            "-with-report /out/report.html -with-timeline /out/time.html -with-dag /out/dag.html -with-trace /out/trace.txt"
        )



class GetExecutionTests(TestCase):

    @patch("nextflow.command.get_file_text")
    @patch("nextflow.command.make_or_update_execution")
    @patch("nextflow.command.get_initial_process_executions")
    @patch("nextflow.command.get_process_ids_to_paths")
    @patch("nextflow.command.update_process_execution_from_path")
    def test_can_get_first_execution(self, mock_update, mock_paths, mock_init, mock_make, mock_text):
        mock_text.return_value = "LOG"
        mock_execution = Mock()
        mock_make.return_value = mock_execution
        process_executions = {
            "aa/bb": Mock(identifier="aa/bb", path="/ex/aa/bb", finished=None),
            "cc/dd": Mock(identifier="cc/dd", path=None, finished="Y"),
            "ee/ff": Mock(identifier="ee/ff", path="/ex/ee/ff", finished=None),
            "gg/hh": Mock(identifier="gg/hh", path=None, finished="Y"),
        }
        mock_init.return_value = (process_executions, ["ee/ff", "gg/hh"])
        mock_paths.return_value = {
            "cc/dd": "/ex/cc/dd",
            "gg/hh": "/ex/gg/hh",
        }
        execution, size = get_execution("/ex", "nf run")
        self.assertEqual(execution, mock_execution)
        self.assertEqual(size, 3)
        mock_text.assert_called_with(os.path.join("/ex", ".nextflow.log"))
        mock_make.assert_called_with("LOG", "/ex", "nf run", None)
        mock_init.assert_called_with("LOG", mock_execution)
        mock_paths.assert_called_with(["cc/dd","gg/hh"], "/ex")
        self.assertEqual([c[0] for c in mock_update.call_args_list], [
            (process_executions["aa/bb"], "/ex"),
            (process_executions["ee/ff"], "/ex"),
            (process_executions["gg/hh"], "/ex"),
        ])
        self.assertEqual(execution.process_executions, [
            process_executions["aa/bb"],
            process_executions["cc/dd"],
            process_executions["ee/ff"],
            process_executions["gg/hh"],
        ])
    

    @patch("nextflow.command.get_file_text")
    @patch("nextflow.command.make_or_update_execution")
    @patch("nextflow.command.get_initial_process_executions")
    @patch("nextflow.command.get_process_ids_to_paths")
    @patch("nextflow.command.update_process_execution_from_path")
    def test_can_get_subsequent_execution(self, mock_update, mock_paths, mock_init, mock_make, mock_text):
        mock_text.return_value = "LAG_LOG"
        mock_execution = Mock()
        mock_make.return_value = mock_execution
        process_executions = {
            "aa/bb": Mock(identifier="aa/bb", path="/ex/aa/bb", finished=None),
            "cc/dd": Mock(identifier="cc/dd", path=None, finished="Y"),
            "ee/ff": Mock(identifier="ee/ff", path="/ex/ee/ff", finished=None),
            "gg/hh": Mock(identifier="gg/hh", path=None, finished="Y"),
        }
        mock_init.return_value = (process_executions, ["ee/ff", "gg/hh"])
        mock_paths.return_value = {
            "cc/dd": "/ex/cc/dd",
            "gg/hh": "/ex/gg/hh",
        }
        execution, size = get_execution("/ex", "nf run", mock_execution, 4)
        self.assertEqual(execution, mock_execution)
        self.assertEqual(size, 3)
        mock_text.assert_called_with(os.path.join("/ex", ".nextflow.log"))
        mock_make.assert_called_with("LOG", "/ex", "nf run", mock_execution)
        mock_init.assert_called_with("LOG", mock_execution)
        mock_paths.assert_called_with(["cc/dd","gg/hh"], "/ex")
        self.assertEqual([c[0] for c in mock_update.call_args_list], [
            (process_executions["aa/bb"], "/ex"),
            (process_executions["ee/ff"], "/ex"),
            (process_executions["gg/hh"], "/ex"),
        ])
        self.assertEqual(execution.process_executions, [
            process_executions["aa/bb"],
            process_executions["cc/dd"],
            process_executions["ee/ff"],
            process_executions["gg/hh"],
        ])
    

    @patch("nextflow.command.get_file_text")
    def test_can_handle_no_log_yet(self, mock_text):
        mock_text.return_value = ""
        execution, size = get_execution("/ex", "nf run")
        self.assertIsNone(execution)
        self.assertEqual(size, 0)
        mock_text.assert_called_with(os.path.join("/ex", ".nextflow.log"))



class MakeOrUpdateExecutionTests(TestCase):

    @patch("nextflow.command.get_identifier_from_log")
    @patch("nextflow.command.get_started_from_log")
    @patch("nextflow.command.get_finished_from_log")
    @patch("nextflow.command.get_file_text")
    def test_can_create_execution(self, mock_text, mock_fin, mock_start, mock_id):
        command = "nf run >stdout.txt 2>stderr.txt"
        mock_text.side_effect = ["ok", "bad", "9"]
        execution = make_or_update_execution("LOG", "/path", command, None)
        self.assertEqual(execution.identifier, mock_id.return_value)
        self.assertEqual(execution.stdout, "ok")
        self.assertEqual(execution.stderr, "bad")
        self.assertEqual(execution.return_code, "9")
        self.assertEqual(execution.started, mock_start.return_value)
        self.assertEqual(execution.finished, mock_fin.return_value)
        self.assertEqual(execution.command, command)
        self.assertEqual(execution.log, "LOG")
        self.assertEqual(execution.path, "/path")
        self.assertEqual(execution.process_executions, [])
        mock_id.assert_called_with("LOG")
        mock_start.assert_called_with("LOG")
        mock_fin.assert_called_with("LOG")
        self.assertEqual([c[0] for c in mock_text.call_args_list], [
            (os.path.join("/path", "stdout.txt"),),
            (os.path.join("/path", "stderr.txt"),),
            (os.path.join("/path", "rc.txt"),),
        ])
    

    @patch("nextflow.command.get_identifier_from_log")
    @patch("nextflow.command.get_started_from_log")
    @patch("nextflow.command.get_finished_from_log")
    @patch("nextflow.command.get_file_text")
    def test_can_update_execution_with_values(self, mock_text, mock_fin, mock_start, mock_id):
        old_execution = Mock(identifier="xx/yy", started="MON", finished="TUE", log="LOG1", stdout=".", stderr=".", return_code="", command="nf")
        command = "nf run >stdout.txt 2>stderr.txt"
        mock_text.side_effect = ["ok", "bad", "9"]
        execution = make_or_update_execution("LOG", "/path", command, old_execution)
        self.assertIs(old_execution, execution)
        self.assertEqual(execution.identifier, "xx/yy")
        self.assertEqual(execution.stdout, "ok")
        self.assertEqual(execution.stderr, "bad")
        self.assertEqual(execution.return_code, "9")
        self.assertEqual(execution.started, "MON")
        self.assertEqual(execution.finished, "TUE")
        self.assertEqual(execution.command, "nf")
        self.assertEqual(execution.log, "LOG1LOG")
        self.assertFalse(mock_id.called)
        self.assertFalse(mock_start.called)
        self.assertFalse(mock_fin.called)
        self.assertEqual([c[0] for c in mock_text.call_args_list], [
            (os.path.join("/path", "stdout.txt"),),
            (os.path.join("/path", "stderr.txt"),),
            (os.path.join("/path", "rc.txt"),),
        ])



class InitialProcessExecutionTests(TestCase):

    @patch("nextflow.command.create_process_execution_from_line")
    @patch("nextflow.command.update_process_execution_from_line")
    def test_can_create_first_pass(self, mock_update, mock_create):
        execution = Mock(process_executions=[])
        p1, p2 = Mock(identifier="aa/bb"), Mock(identifier="xx/yy")
        mock_create.side_effect = [p1, p2, None]
        mock_update.return_value = "cc/dd"
        log = "line1\nSubmitted process a/bb\n..[ab/123456]\n[cd/789012] Submitted process\nTask completed\nSubmitted process"
        process_executions, updated = get_initial_process_executions(log, execution)
        self.assertEqual(process_executions, {"aa/bb": p1, "xx/yy": p2})
        self.assertEqual(updated, ["aa/bb", "xx/yy", "cc/dd"])
        self.assertEqual([c[0] for c in mock_create.call_args_list], [
            ("Submitted process a/bb",),
            ("[cd/789012] Submitted process",),
            ("Submitted process",),
        ])
        mock_update.assert_called_with({"aa/bb": p1, "xx/yy": p2}, "Task completed")
    

    @patch("nextflow.command.create_process_execution_from_line")
    @patch("nextflow.command.update_process_execution_from_line")
    def test_can_update_existing(self, mock_update, mock_create):
        p1, p2 = Mock(identifier="aa/bb"), Mock(identifier="xx/yy")
        p3, p4 = Mock(identifier="cc/dd"), Mock(identifier="aa/bb")
        execution = Mock(process_executions=[p3, p4])
        mock_create.side_effect = [p1, p2, None]
        mock_update.return_value = "cc/dd"
        log = "line1\nSubmitted process a/bb\n..[ab/123456]\n[cd/789012] Submitted process\nTask completed\nSubmitted process"
        process_executions, updated = get_initial_process_executions(log, execution)
        self.assertEqual(process_executions, {"aa/bb": p1, "cc/dd": p3, "xx/yy": p2})
        self.assertEqual(updated, ["aa/bb", "xx/yy", "cc/dd"])
        self.assertEqual([c[0] for c in mock_create.call_args_list], [
            ("Submitted process a/bb",),
            ("[cd/789012] Submitted process",),
            ("Submitted process",),
        ])
        mock_update.assert_called_with({"aa/bb": p1, "cc/dd": p3, "xx/yy": p2}, "Task completed")



class CreateProcessExecutionFromLineTests(TestCase):

    @patch("nextflow.command.parse_submitted_line")
    def test_can_create_process_execution(self, mock_parse):
        mock_parse.return_value = ("aa/bb", "PROC (123)", "PROC", "NOW")
        proc_ex = create_process_execution_from_line("line1")
        self.assertEqual(proc_ex.identifier, "aa/bb")
        self.assertEqual(proc_ex.name, "PROC (123)")
        self.assertEqual(proc_ex.process, "PROC")
        self.assertEqual(proc_ex.started, "NOW")
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.return_code, "")
        self.assertEqual(proc_ex.bash, "")
        self.assertEqual(proc_ex.finished, None)
        self.assertEqual(proc_ex.status, "-")
        self.assertEqual(proc_ex.path, "")
    

    @patch("nextflow.command.parse_submitted_line")
    def test_can_handle_no_identifier(self, mock_parse):
        mock_parse.return_value = ("", "PROC (123)", "PROC", "NOW")
        proc_ex = create_process_execution_from_line("line1")
        self.assertIsNone(proc_ex)



class UpdateProcessExecutionFromLine(TestCase):

    @patch("nextflow.command.parse_completed_line")
    def test_can_update_values(self, mock_parse):
        mock_parse.return_value = ("aa/bb", "NOW", "1", "FAIL")
        process_executions = {
            "aa/bb": Mock(finished=None, return_code="", path="aa/bb", execution=Mock(finished=None)),
            "cc/dd": Mock(finished=None, return_code="", path="cc/dd", execution=Mock(finished=None)),
        }
        identifier = update_process_execution_from_line(process_executions, "line1")
        self.assertEqual(process_executions["aa/bb"].finished, "NOW")
        self.assertEqual(process_executions["aa/bb"].return_code, "1")
        self.assertEqual(process_executions["aa/bb"].status, "FAIL")
        self.assertEqual(identifier, "aa/bb")
    

    @patch("nextflow.command.parse_completed_line")
    def test_can_handle_no_process_execution(self, mock_parse):
        mock_parse.return_value = ("aa/bb", "NOW", "1", "FAIL")
        process_executions = {
            "cc/dd": Mock(finished=None, return_code="", path="cc/dd", status="", execution=Mock(finished=None)),
        }
        identifier = update_process_execution_from_line(process_executions, "line1")
        self.assertEqual(process_executions["cc/dd"].finished, None)
        self.assertEqual(process_executions["cc/dd"].return_code, "")
        self.assertEqual(process_executions["cc/dd"].status, "")
        self.assertEqual(identifier, None)
    

    @patch("nextflow.command.parse_completed_line")
    def test_can_handle_no_identifier(self, mock_parse):
        mock_parse.return_value = ("", "NOW", "1", "FAIL")
        process_executions = {
            "aa/bb": Mock(finished=None, return_code="", path="aa/bb", status="", execution=Mock(finished=None)),
        }
        identifier = update_process_execution_from_line(process_executions, "line1")
        self.assertEqual(process_executions["aa/bb"].finished, None)
        self.assertEqual(process_executions["aa/bb"].return_code, "")
        self.assertEqual(process_executions["aa/bb"].status, "")
        self.assertEqual(identifier, None)



class UpdateProcessExecutionFromPathTests(TestCase):

    @patch("nextflow.command.get_file_text")
    def test_can_update_values(self, mock_text):
        proc_ex = Mock(stdout=".", stderr=".", bash=".", finished=None, return_code="", path="aa/bb", execution=Mock(finished=None))
        mock_text.side_effect = ["ok", "bad"]
        update_process_execution_from_path(proc_ex, "/ex")
        self.assertEqual(proc_ex.stdout, "ok")
        self.assertEqual(proc_ex.stderr, "bad")
        self.assertEqual(proc_ex.bash, ".")
        self.assertEqual(proc_ex.return_code, "")
        self.assertEqual(mock_text.call_args_list, [
            call(os.path.join("/ex", "work", "aa/bb", ".command.out"),),
            call(os.path.join("/ex", "work", "aa/bb", ".command.err"),),
        ])
    

    @patch("nextflow.command.get_file_text")
    def test_can_update_values_with_bash(self, mock_text):
        proc_ex = Mock(stdout=".", stderr=".", bash="", return_code="0", path="aa/bb", execution=Mock(finished="FFF"))
        mock_text.side_effect = ["ok", "bad", "$$"]
        update_process_execution_from_path(proc_ex, "/ex")
        self.assertEqual(proc_ex.stdout, "ok")
        self.assertEqual(proc_ex.stderr, "bad")
        self.assertEqual(proc_ex.bash, "$$")
        self.assertEqual(proc_ex.return_code, "0")
        self.assertEqual(mock_text.call_args_list, [
            call(os.path.join("/ex", "work", "aa/bb", ".command.out"),),
            call(os.path.join("/ex", "work", "aa/bb", ".command.err"),),
            call(os.path.join("/ex", "work", "aa/bb", ".command.sh"),),
        ])
    

    @patch("nextflow.command.get_file_text")
    def test_can_add_exit_code(self, mock_text):
        proc_ex = Mock(stdout=".", stderr=".", bash=".", finished=None, return_code="", path="aa/bb", execution=Mock(finished="FFF", return_code="9"))
        mock_text.side_effect = ["ok", "bad"]
        update_process_execution_from_path(proc_ex, "/ex")
        self.assertEqual(proc_ex.stdout, "ok")
        self.assertEqual(proc_ex.stderr, "bad")
        self.assertEqual(proc_ex.bash, ".")
        self.assertEqual(proc_ex.return_code, "9")
        self.assertEqual(mock_text.call_args_list, [
            call(os.path.join("/ex", "work", "aa/bb", ".command.out"),),
            call(os.path.join("/ex", "work", "aa/bb", ".command.err"),),
        ])
    

    def test_can_handle_no_path(self):
        proc_ex = Mock(stdout=".", stderr=".", bash=".", path="", finished=None, return_code="", execution=Mock(finished="FFF", return_code="9"))
        update_process_execution_from_path(proc_ex, "/ex")
        self.assertEqual(proc_ex.stdout, ".")
        self.assertEqual(proc_ex.stderr, ".")
        self.assertEqual(proc_ex.bash, ".")
        self.assertEqual(proc_ex.return_code, "")