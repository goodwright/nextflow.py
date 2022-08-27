from datetime import datetime
from unittest import TestCase
from unittest.mock import PropertyMock, mock_open, patch, Mock, MagicMock
from nextflow.execution import *

class ExecutionTest(TestCase):

    def setUp(self):
        self.patch = patch("nextflow.execution.Execution.get_processes_from_log")
        self.mock_get_processes = self.patch.start()
        self.pipeline = Mock()
    

    def tearDown(self):
        self.patch.stop()



class ExecutionCreationTests(ExecutionTest):

    def test_can_create_execution(self):
        execution = Execution("/location", "xxx_yyy", self.pipeline)
        self.assertEqual(execution.location, "/location")
        self.assertEqual(execution.id, "xxx_yyy")
        self.assertEqual(execution.pipeline, self.pipeline)
        self.assertIsNone(execution.stdout)
        self.assertIsNone(execution.stderr)
        self.assertIsNone(execution.returncode)
        self.assertEqual(str(execution), "<Execution [xxx_yyy]>")
        self.mock_get_processes.assert_called_once_with()
    

    def test_can_create_execution_with_process(self):
        execution = Execution("/location", "xxx_yyy", self.pipeline, stdout="ok", stderr="bad", returncode=1)
        self.assertEqual(execution.location, "/location")
        self.assertEqual(execution.id, "xxx_yyy")
        self.assertEqual(execution.pipeline, self.pipeline)
        self.assertEqual(execution.stdout, "ok")
        self.assertEqual(execution.stderr, "bad")
        self.assertEqual(execution.returncode, 1)
        self.assertEqual(str(execution), "<Execution [xxx_yyy]>")
        self.mock_get_processes.assert_called_once_with()



class ExecutionFromLocationTests(ExecutionTest):

    @patch("nextflow.execution.get_directory_id")
    @patch("nextflow.execution.Execution")
    def test_can_create_execution_from_location(self, mock_Ex, mock_id):
        mock_id.return_value = "xx_yy"
        ex = Execution.create_from_location(
            "/path/to/execution", self.pipeline, "ok", "bad", 1
        )
        mock_id.assert_called_with("/path/to/execution")
        mock_Ex.assert_called_with(
            "/path/to/execution", "xx_yy", self.pipeline, stdout="ok", stderr="bad", returncode=1
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
        execution = Execution("/location", "ccc_ddd", self.pipeline)
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
        execution = Execution("/location", "xxx_yyy", self.pipeline)
        self.assertIsNone(execution.history_data)
        mock_open.assert_called_with(os.path.join("/location", ".nextflow", "history"))



class ExecutionStartedStringTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_started_string(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.started_string, "2021-10-09 19:54:49")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_started_string(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertIsNone(execution.started_string)



class ExecutionStartedDtTests(ExecutionTest):

    @patch("nextflow.execution.Execution.started_string", new_callable=PropertyMock)
    @patch("nextflow.execution.parse_datetime")
    def test_can_get_started_dt(self, mock_parse_datetime, mock_started_string):
        mock_started_string.return_value = "2022"
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.started_dt, mock_parse_datetime.return_value)
        mock_parse_datetime.assert_called_with("2022")
    

    @patch("nextflow.execution.Execution.started_string", new_callable=PropertyMock)
    @patch("nextflow.execution.parse_datetime")
    def test_can_get_no_started_dt(self, mock_parse_datetime, mock_started_string):
        mock_started_string.return_value = None
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.started_dt, None)
        self.assertFalse(mock_parse_datetime.called)



class ExecutionStartedTests(ExecutionTest):

    @patch("nextflow.execution.Execution.started_dt", new_callable=PropertyMock)
    def test_can_get_started(self, mock_started_dt):
        mock_started_dt.return_value = datetime(2021, 1, 2, 3, 4, 5)
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.started, 1609556645)
    

    @patch("nextflow.execution.Execution.started_dt", new_callable=PropertyMock)
    def test_can_get_no_started(self, mock_started_dt):
        mock_started_dt.return_value = None
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.started, None)



class ExecutionDurationStringTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_duration_string(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.duration_string, "4s")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_duration_string(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertIsNone(execution.duration_string)



class ExecutionDurationTests(ExecutionTest):

    @patch("nextflow.execution.Execution.duration_string", new_callable=PropertyMock)
    @patch("nextflow.execution.parse_duration")
    def test_can_get_duration(self, mock_parse_duration, mock_duration_string):
        mock_duration_string.return_value = "5s"
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.duration, mock_parse_duration.return_value)
        mock_parse_duration.assert_called_with("5s")
    

    @patch("nextflow.execution.Execution.duration_string", new_callable=PropertyMock)
    @patch("nextflow.execution.parse_duration")
    def test_can_get_no_duration(self, mock_parse_duration, mock_duration_string):
        mock_duration_string.return_value = None
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.duration, None)
        self.assertFalse(mock_parse_duration.called)



class ExecutionStatusTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_status(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.status, "OK")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_status(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertIsNone(execution.status)



class ExecutionCommandTests(ExecutionTest):

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_command(self, mock_history):
        mock_history.return_value = [
            "2021-10-09 19:54:49", "4s", "ccc_ddd", "OK", "2dcf6dbff4", "c2e4c5df-a8ae-4d3a", "nextflow run main.nf"
        ]
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.command, "nextflow run main.nf")
    

    @patch("nextflow.execution.Execution.history_data", new_callable=PropertyMock)
    def test_can_get_no_command(self, mock_history):
        mock_history.return_value = None
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertIsNone(execution.command)



class ExecutionLogTests(ExecutionTest):

    @patch("os.listdir")
    @patch("builtins.open")
    def test_can_get_log(self, mock_open, mock_dir):
        mock_dir.return_value = ["file1", ".nextflow.log", ".nextflow.log1", ".nextflow.log3"]
        mock_open.return_value.__enter__().read.side_effect = [
            "aaa [aaa_bbb] ...", "aaa [ccc_ddd] ...", "aaa [eee_fff] ..."
        ]
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.assertEqual(execution.log, "aaa [ccc_ddd] ...")
        mock_dir.assert_called_with("/location")
        mock_open.assert_any_call(os.path.join("/location", ".nextflow.log"))
        mock_open.assert_any_call(os.path.join("/location", ".nextflow.log1"))



class ProcessesFromLogTests(ExecutionTest):

    @patch("nextflow.execution.Execution.log", new_callable=PropertyMock)
    @patch("nextflow.execution.get_process_name_from_log")
    @patch("nextflow.execution.get_process_start_from_log")
    @patch("nextflow.execution.get_process_status_from_log")
    @patch("nextflow.execution.get_process_stdout")
    @patch("nextflow.execution.get_process_stderr")
    @patch("nextflow.execution.get_process_returncode")
    @patch("nextflow.execution.get_process_end_from_log")
    @patch("nextflow.execution.ProcessExecution")
    def test_can_get_processes_from_log(self, *mocks):
        mocks[-1].return_value = (
            "Jun-02 19:39:54.493 [main] DEBUG nextflow.cli.CmdRun -\n"
            "Jun-01 16:45:57.048 [Task submitter] INFO  nextflow.Session - [d6/31d530] Submitted process > DEMULTIPLEX:CSV_TO_BARCODE (file.csv)\n"
            "Jun-01 16:46:08.965 [Task submitter] INFO  nextflow.Session - [99/6165a9] Submitted process > DEMULTIPLEX:FASTQC\n"
            "Jun-01 16:46:13.434 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye"
        )
        mocks[-2].side_effect = ["CSV_TO_BARCODE (file.csv)", "FASTQC"]
        mocks[-3].side_effect = ["Jun-01 16:45:57.148", "Jul-07 16:45:57.148"]
        mocks[-4].side_effect = ["COMPLETED", "ERROR"]
        mocks[-5].side_effect = ["out1", "out2"]
        mocks[-6].side_effect = ["err1", "err2"]
        mocks[-7].side_effect = ["0", "1"]
        mocks[-8].side_effect = [
            datetime(datetime.now().year, 6, 1, 17, 45, 57, 148000),
            datetime(datetime.now().year, 7, 7, 17, 45, 57, 148000),
        ]
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.patch.stop()
        execution.get_processes_from_log()
        for mock in mocks[-2:-5] + tuple(mocks[-8:-9]):
            mock.assert_any_call(mocks[-1].return_value, "d6/31d530")
            mock.assert_any_call(mocks[-1].return_value, "99/6165a9")
        for mock in mocks[-5:-8]:
            mock.assert_any_call(execution, "d6/31d530")
            mock.assert_any_call(execution, "99/6165a9")
        mocks[-9].assert_any_call(
            execution=execution,
            hash="d6/31d530",
            process="CSV_TO_BARCODE",
            name="CSV_TO_BARCODE (file.csv)",
            started_string="Jun-01 16:45:57.148",
            started=datetime(datetime.now().year, 6, 1, 16, 45, 57, 148000),
            duration=3600,
            status="COMPLETED",
            stdout="out1",
            stderr="err1",
            returncode="0"
        )
        mocks[-9].assert_any_call(
            execution=execution,
            hash="99/6165a9",
            process="FASTQC",
            name="FASTQC",
            started_string="Jul-07 16:45:57.148",
            started=datetime(datetime.now().year, 7, 7, 16, 45, 57, 148000),
            duration=3600,
            status="ERROR",
            stdout="out2",
            stderr="err2",
            returncode="1"
        )
    

    @patch("nextflow.execution.Execution.log", new_callable=PropertyMock)
    def test_can_get_processes_from_log(self, mock_log):
        mock_log.return_value = ""
        execution = Execution("/location", "ccc_ddd", self.pipeline)
        self.patch.stop()
        execution.get_processes_from_log()
        self.assertEqual(execution.process_executions, [])