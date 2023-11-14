from unittest import TestCase
from unittest.mock import patch
from nextflow.log import *

class LogStartedTests(TestCase):

    def test_can_handle_no_log_text(self):
        self.assertFalse(get_started_from_log(""))
        

    @patch("nextflow.log.get_datetime_from_line")
    def test_can_get_started_datetime(self, mock_datetime):
        mock_datetime.return_value = datetime(2020, 1, 1, 1, 1, 1)
        self.assertEqual(get_started_from_log("line1\nline2"), datetime(2020, 1, 1, 1, 1, 1))
        mock_datetime.assert_called_with("line1")



class LogFinishedTests(TestCase):

    def test_can_handle_no_log_text(self):
        self.assertFalse(get_finished_from_log(""))
    

    @patch("nextflow.log.log_is_finished")
    def test_can_handle_no_nextflow_finished_text(self, mock_finished):
        mock_finished.return_value = False
        self.assertFalse(get_finished_from_log("line1\nline2"))
        mock_finished.assert_called_with("line1\nline2")
    

    @patch("nextflow.log.log_is_finished")
    @patch("nextflow.log.get_datetime_from_line")
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
            datetime(datetime.now().year, 6, 1, 16, 46, 8, 965000)
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
            datetime(datetime.now().year, 6, 1, 16, 46, 8, 878000)
        )
    

    def test_can_get_no_process_end_from_log(self):
        self.assertIsNone(get_process_end_from_log(self.log, "1a/c2a4dc"))



class ProcessStatusFromLogTests(TestCase):
    
    def setUp(self):
        self.log = (
            "Jun-02 19:39:54.493 [main] DEBUG nextflow.cli.CmdRun -\n"
            "Jun-01 16:46:00.365 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 1; name: DEMULTIPLEX:CSV_TO_BARCODE (file.csv); status: COMPLETED; exit: 0; error: -; workDir: /work/d6/31d530a65ef23d1cb302940a782909]\n"
            "Jun-01 16:46:08.878 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 2; name: DEMULTIPLEX:ULTRAPLEX (file.fastq); status: COMPLETED; exit: 0; error: -; workDir: /work/8a/c2a4dc996d54cad136abeb4e4e309a]\n"
            "Jun-01 16:46:08.878 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 2; name: DEMULTIPLEX:ULTRAPLEX (file2.fastq); status: COMPLETED; exit: 1; error: -; workDir: /work/4b/302940a782909c996d54cad31d53d45]\n"
            "Nov-14 14:09:42.634 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 20; name: NFCORE_FETCHNGS:SRA:SRA_TO_SAMPLESHEET (ERX1234253_ERR1160846); status: COMPLETED; exit: -; error: -; workDir: /Users/sam/Dropbox/Code/flow-api/local/executions/123951903246975190/work/c8/dfda38334147580b403fbf9da01d25]\n"
            "Jun-01 16:46:13.434 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye"
        )
    
    
    def test_can_get_completed_process_status_from_log(self):
        self.assertEqual(
            get_process_status_from_log(self.log, "8a/c2a4dc"), "COMPLETED"
        )
    

    def test_can_get_fail_process_status_from_log(self):
        self.assertEqual(
            get_process_status_from_log(self.log, "4b/302940"), "FAILED"
        )
    

    def test_can_handle_missing_exit_code(self):
        self.assertEqual(
            get_process_status_from_log(self.log, "c8/dfda38"), "COMPLETED"
        )
    

    def test_can_get_no_process_status_from_log(self):
        self.assertEqual(
            get_process_status_from_log(self.log, "1a/c2a4dc"), "-"
        )