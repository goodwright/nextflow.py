from os import pipe
from unittest import TestCase
from unittest.mock import PropertyMock, mock_open, patch, Mock, MagicMock
from nextflow.pipeline import *

class ExecutionTest(TestCase):

    def setUp(self):
        self.patch1 = patch("nextflow.execution.Execution.update_process_executions")
        self.mock_update = self.patch1.start()
    

    def tearDown(self):
        self.patch1.stop()



class ExecutionCreationTests(ExecutionTest):

    def test_can_create_execution(self):
        execution = Execution("/location", "xxx_yyy")
        self.assertEqual(execution.location, "/location")
        self.assertEqual(execution.id, "xxx_yyy")
        self.assertIsNone(execution.stdout)
        self.assertIsNone(execution.stderr)
        self.assertIsNone(execution.returncode)
        self.assertEqual(str(execution), "<Execution [xxx_yyy]>")
        self.mock_update.assert_called_once_with()
    

    def test_can_create_execution_with_process(self):
        execution = Execution("/location", "xxx_yyy", stdout="ok", stderr="bad", returncode=1)
        self.assertEqual(execution.location, "/location")
        self.assertEqual(execution.id, "xxx_yyy")
        self.assertEqual(execution.stdout, "ok")
        self.assertEqual(execution.stderr, "bad")
        self.assertEqual(execution.returncode, 1)
        self.assertEqual(str(execution), "<Execution [xxx_yyy]>")
        self.mock_update.assert_called_once_with()



class ExecutionFromLocationTests(TestCase):

    @patch("builtins.open")
    @patch("nextflow.execution.Execution")
    def test_can_create_execution_from_location(self, mock_Ex, mock_open):
        open_return = MagicMock()
        mock_file = Mock()
        open_return.__enter__.return_value = mock_file
        mock_file.read.return_value = "abc [xx_yy] def"
        mock_open.return_value = open_return
        ex = Execution.create_from_location(
            "/path/to/execution", "ok", "bad", 1
        )
        mock_open.assert_called_with("/path/to/execution/.nextflow.log")
        mock_Ex.assert_called_with(
            "/path/to/execution", "xx_yy", stdout="ok", stderr="bad", returncode=1
        )
        self.assertIs(ex, mock_Ex.return_value)



class HistoryDataTests(ExecutionTest):

    @patch("builtins.open", new_callable=mock_open)
    def test_can_get_history_data(self, mock_open):
        mock_open.return_value.__enter__().readlines.return_value = [
            "2021-10-09 18:54:49\t-\taaa_bbb\t-\t1dcf6dbff4\tc2e4c5df-a8ae-4d3a\tnextflow run main.nf",
            "2021-10-09 19:54:49\t-\tccc_ddd\t-\t2dcf6dbff4\tc2e4c5df-a8ae-4d3a\tnextflow run main.nf",
            "2021-10-09 20:54:49\t-\teee_fff\t-\t3dcf6dbff4\tc2e4c5df-a8ae-4d3a\tnextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd")
        self.assertEqual(execution.history_data, [
            "2021-10-09 19:54:49", "-", "ccc_ddd", "-", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ])
        mock_open.assert_called_with(os.path.join("/location", ".nextflow", "history"))
    

    @patch("builtins.open", new_callable=mock_open)
    def test_can_get_no_history_data(self, mock_open):
        mock_open.return_value.__enter__().readlines.return_value = [
            "2021-10-09 18:54:49\t-\taaa_bbb\t-\t1dcf6dbff4\tc2e4c5df-a8ae-4d3a\tnextflow run main.nf",
            "2021-10-09 19:54:49\t-\tccc_ddd\t-\t2dcf6dbff4\tc2e4c5df-a8ae-4d3a\tnextflow run main.nf",
            "2021-10-09 20:54:49\t-\teee_fff\t-\t3dcf6dbff4\tc2e4c5df-a8ae-4d3a\tnextflow run main.nf"
        ]
        execution = Execution("/location", "xxx_yyy")
        self.assertIsNone(execution.history_data)
        mock_open.assert_called_with(os.path.join("/location", ".nextflow", "history"))



class ExecutionDatetimeTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_datetime(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd")
        self.assertEqual(execution.datetime, "2021-10-09 19:54:49")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_datetime(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd")
        self.assertIsNone(execution.datetime)



class ExecutionDurationTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_duration(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd")
        self.assertEqual(execution.duration, "4s")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_duration(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd")
        self.assertIsNone(execution.duration)



class ExecutionStatusTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_status(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd")
        self.assertEqual(execution.status, "OK")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_status(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd")
        self.assertIsNone(execution.status)



class ExecutionCommandTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_command(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd")
        self.assertEqual(execution.command, "nextflow run main.nf")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_command(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd")
        self.assertIsNone(execution.command)



class ExecutionLogTests(ExecutionTest):

    @patch("os.listdir")
    @patch("builtins.open")
    def test_can_get_log(self, mock_open, mock_dir):
        mock_dir.return_value = ["file1", ".nextflow.log", ".nextflow.log1", ".nextflow.log3"]
        mock_open.return_value.__enter__().read.side_effect = [
            "aaa [aaa_bbb] ...", "aaa [ccc_ddd] ...", "aaa [eee_fff] ..."
        ]
        execution = Execution("/location", "ccc_ddd")
        self.assertEqual(execution.log, "aaa [ccc_ddd] ...")
        mock_dir.assert_called_with("/location")
        mock_open.assert_any_call(os.path.join("/location", ".nextflow.log"))
        mock_open.assert_any_call(os.path.join("/location", ".nextflow.log1"))



class AvailableFieldsTests(ExecutionTest):

    @patch("subprocess.run")
    def test_can_get_available_fields(self, mock_run):
        mock_run.return_value.stdout = "f1\n f2 \n\n"
        execution = Execution("/location", "ccc_ddd")
        fields = execution.get_available_fields()
        self.assertEqual(fields, ["f1", "f2"])
        mock_run.assert_called_with(
            "nextflow log ccc_ddd -l",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="/location"
        )



class ProcessPathTests(ExecutionTest):

    @patch("subprocess.run")
    def test_can_get_process_paths(self, mock_run):
        mock_run.return_value.stdout = "path1\npath2\n"
        execution = Execution("/location", "ccc_ddd")
        fields = execution.get_process_paths()
        self.assertEqual(fields, ["path1", "path2"])
        mock_run.assert_called_with(
            "nextflow log ccc_ddd",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="/location"
        )



class ProcessExecutionUpdatingTests(TestCase):

    @patch("nextflow.execution.Execution.get_available_fields")
    @patch("nextflow.execution.Execution.get_process_paths")
    @patch("subprocess.run")
    @patch("nextflow.execution.ProcessExecution")
    def test_can_get_process_executions(self, mock_procex, mock_run, mock_paths, mock_fields):
        mock_fields.return_value = ["field1", "field2"]
        mock_paths.return_value = ["path1", "path2"]
        mock_run.side_effect = [Mock(stdout="  1 XXXXXXXXX 2  "), Mock(stdout="  3 XXXXXXXXX 4  ")]
        execution = Execution("/location", "ccc_ddd")
        mock_fields.assert_called_once_with()
        mock_paths.assert_called_once_with()
        mock_run.assert_any_call(
            "nextflow log ccc_ddd -t \"\\$field1 XXXXXXXXX \\$field2\" -F \"workdir == 'path1'\"",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="/location"
        )
        mock_run.assert_any_call(
            "nextflow log ccc_ddd -t \"\\$field1 XXXXXXXXX \\$field2\" -F \"workdir == 'path2'\"",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="/location"
        )
        mock_procex.assert_any_call(fields={"field1": "1", "field2": "2"}, execution=execution)
        mock_procex.assert_any_call(fields={"field1": "3", "field2": "4"}, execution=execution)
        self.assertEqual(execution.process_executions, [mock_procex.return_value, mock_procex.return_value])
