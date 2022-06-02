from unittest import TestCase
from datetime import datetime
from unittest.mock import Mock, patch
from nextflow.utils import *

class DatetimeParsingTests(TestCase):

    def test_can_parse_datetime(self):
        self.assertEqual(
            parse_datetime("2020-01-01 18:00:02"),
            datetime(2020, 1, 1, 18, 0, 2)
        )
    

    def test_can_parse_datetime_with_extra_stuff(self):
        self.assertEqual(
            parse_datetime("2020-01-01 18:00:00.234234"),
            datetime(2020, 1, 1, 18, 0, 0, 234234)
        )



class DurationParsingTests(TestCase):

    def test_can_get_no_duration(self):
        self.assertEqual(parse_duration("-"), 0)
    

    def test_can_get_millisecond_duration(self):
        self.assertEqual(parse_duration("100ms"), 0.1)
    

    def test_can_get_second_duration(self):
        self.assertEqual(parse_duration("100s"), 100)
    

    def test_can_get_minute_duration(self):
        self.assertEqual(parse_duration("2m"), 120)
        self.assertEqual(parse_duration("2m 3s"), 123)
    

    def test_can_get_minute_duration(self):
        self.assertEqual(parse_duration("1h"), 3600)
        self.assertEqual(parse_duration("4h 2m 3s"), 14523)



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



class ProcessStartFromLogTests(TestCase):
    
    def setUp(self):
        self.log = (
            "Jun-02 19:39:54.493 [main] DEBUG nextflow.cli.CmdRun -\n"
            "Jun-01 16:45:57.048 [Task submitter] INFO  nextflow.Session - [d6/31d530] Submitted process > DEMULTIPLEX:CSV_TO_BARCODE (file.csv)\n"
            "Jun-01 16:46:08.965 [Task submitter] INFO  nextflow.Session - [99/6165a9] Submitted process > DEMULTIPLEX:FASTQC (sample)\n"
            "Jun-01 16:46:13.434 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye"
        )
    

    def test_can_get_process_start_from_log(self):
        self.assertEqual(
            get_process_start_from_log(self.log, "99/6165a9"),
            "Jun-01 16:46:08.965"
        )
    

    def test_can_get_no_process_start(self):
        self.assertIsNone(get_process_start_from_log(self.log, "88/6165a9"))



class ProcessEndFromLogTests(TestCase):
    
    def setUp(self):
        self.log = (
            "Jun-02 19:39:54.493 [main] DEBUG nextflow.cli.CmdRun -\n"
            "Jun-01 16:46:00.365 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 1; name: DEMULTIPLEX:CSV_TO_BARCODE (file.csv); status: COMPLETED; exit: 0; error: -; workDir: /work/d6/31d530a65ef23d1cb302940a782909]\n"
            "Jun-01 16:46:08.878 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 2; name: DEMULTIPLEX:ULTRAPLEX (file.fastq); status: COMPLETED; exit: 0; error: -; workDir: /work/8a/c2a4dc996d54cad136abeb4e4e309a]\n"
            "Jun-01 16:46:13.434 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye"
        )
    

    def test_can_get_process_end_from_log(self):
        self.assertEqual(
            get_process_end_from_log(self.log, "8a/c2a4dc"),
            datetime(2022, 6, 1, 16, 46, 8, 878000)
        )
    

    def test_can_get_no_process_end_from_log(self):
        self.assertLessEqual(abs(
            datetime.now() - get_process_end_from_log(self.log, "1a/c2a4dc")
        ).total_seconds(), 0.1)



class ProcessStatusFromLogTests(TestCase):
    
    def setUp(self):
        self.log = (
            "Jun-02 19:39:54.493 [main] DEBUG nextflow.cli.CmdRun -\n"
            "Jun-01 16:46:00.365 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 1; name: DEMULTIPLEX:CSV_TO_BARCODE (file.csv); status: COMPLETED; exit: 0; error: -; workDir: /work/d6/31d530a65ef23d1cb302940a782909]\n"
            "Jun-01 16:46:08.878 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 2; name: DEMULTIPLEX:ULTRAPLEX (file.fastq); status: COMPLETED; exit: 0; error: -; workDir: /work/8a/c2a4dc996d54cad136abeb4e4e309a]\n"
            "Jun-01 16:46:13.434 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye"
        )
    
    
    def test_can_get_process_status_from_log(self):
        self.assertEqual(
            get_process_status_from_log(self.log, "8a/c2a4dc"), "COMPLETED"
        )
    

    def test_can_get_no_process_status_from_log(self):
        self.assertEqual(
            get_process_status_from_log(self.log, "1a/c2a4dc"), "-"
        )



class ProcessStdoutTests(TestCase):
    
    @patch("nextflow.utils.get_process_directory")
    @patch("builtins.open")
    def test_can_get_stdout(self, mock_open, mock_dir):
        mock_dir.return_value = "/work/1a/234bcd454353452"
        mock_open.return_value.__enter__.return_value.read.return_value = "good"
        self.assertEqual(get_process_stdout("execution", "1a/234bcd"), "good")
        mock_dir.assert_called_with("execution", "1a/234bcd")
        mock_open.assert_called_with(
            os.path.join("/work/1a/234bcd454353452", ".command.out")
        )
    

    @patch("nextflow.utils.get_process_directory")
    @patch("builtins.open")
    def test_can_get_no_stdout(self, mock_open, mock_dir):
        mock_dir.return_value = "/work/1a/234bcd454353452"
        mock_open.side_effect = FileNotFoundError
        self.assertEqual(get_process_stdout("execution", "1a/234bcd"), "-")
        mock_dir.assert_called_with("execution", "1a/234bcd")
        mock_open.assert_called_with(
            os.path.join("/work/1a/234bcd454353452", ".command.out")
        )



class ProcessStderrTests(TestCase):
    
    @patch("nextflow.utils.get_process_directory")
    @patch("builtins.open")
    def test_can_get_stderr(self, mock_open, mock_dir):
        mock_dir.return_value = "/work/1a/234bcd454353452"
        mock_open.return_value.__enter__.return_value.read.return_value = "good"
        self.assertEqual(get_process_stderr("execution", "1a/234bcd"), "good")
        mock_dir.assert_called_with("execution", "1a/234bcd")
        mock_open.assert_called_with(
            os.path.join("/work/1a/234bcd454353452", ".command.err")
        )
    

    @patch("nextflow.utils.get_process_directory")
    @patch("builtins.open")
    def test_can_get_no_stderr(self, mock_open, mock_dir):
        mock_dir.return_value = "/work/1a/234bcd454353452"
        mock_open.side_effect = FileNotFoundError
        self.assertEqual(get_process_stderr("execution", "1a/234bcd"), "-")
        mock_dir.assert_called_with("execution", "1a/234bcd")
        mock_open.assert_called_with(
            os.path.join("/work/1a/234bcd454353452", ".command.err")
        )



class ProcessReturnCodeTests(TestCase):
    
    @patch("nextflow.utils.get_process_directory")
    @patch("builtins.open")
    def test_can_get_returncode(self, mock_open, mock_dir):
        mock_dir.return_value = "/work/1a/234bcd454353452"
        mock_open.return_value.__enter__.return_value.read.return_value = "1"
        self.assertEqual(get_process_returncode("execution", "1a/234bcd"), "1")
        mock_dir.assert_called_with("execution", "1a/234bcd")
        mock_open.assert_called_with(
            os.path.join("/work/1a/234bcd454353452", ".exitcode")
        )
    

    @patch("nextflow.utils.get_process_directory")
    @patch("builtins.open")
    def test_can_get_no_returncode(self, mock_open, mock_dir):
        mock_dir.return_value = "/work/1a/234bcd454353452"
        mock_open.side_effect = FileNotFoundError
        self.assertEqual(get_process_returncode("execution", "1a/234bcd"), "")
        mock_dir.assert_called_with("execution", "1a/234bcd")
        mock_open.assert_called_with(
            os.path.join("/work/1a/234bcd454353452", ".exitcode")
        )



class ProcessDirectoryTests(TestCase):
    
    @patch("os.listdir")
    def test_can_get_directory(self, mock_listdir):
        execution = Mock(location="/path")
        mock_listdir.return_value = [
            "3ca2234325d7eea72a38587",
            "234bcd99999e9ffa46a",
            "323423f43234234"
        ]
        directory = get_process_directory(execution, "1a/234bcd")
        self.assertEqual(directory, os.path.join(
            "/path", "work", "1a", "234bcd99999e9ffa46a"
        ))
        mock_listdir.assert_called_with(os.path.join("/path", "work", "1a"))
    

    @patch("os.listdir")
    def test_can_get_handle_no_directory(self, mock_listdir):
        execution = Mock(location="/path")
        mock_listdir.return_value = [
            "3ca2234325d7eea72a38587",
            "x234bcd99999e9ffa46a",
            "323423f43234234"
        ]
        self.assertIsNone(get_process_directory(execution, "1a/234bcd"))
        mock_listdir.assert_called_with(os.path.join("/path", "work", "1a"))