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



class LogIdentifierTests(TestCase):

    def test_can_handle_no_log_text(self):
        self.assertEqual(get_identifier_from_log(""), "")
    

    def test_can_handle_no_identifier(self):
        self.assertEqual(get_identifier_from_log("log text\nok\nbad\n9"), "")
    

    def test_can_get_identifier_from_log(self):
        self.assertEqual(
            get_identifier_from_log("log text [xx_yy]\nok\nbad\n9"),
            "xx_yy"
        )



class DatetimeFromLineTests(TestCase):

    def test_can_get_datetime_from_line(self):
        self.assertEqual(
            get_datetime_from_line("Mar-29 02:48:56.642 [main] DEBUG"),
            datetime(datetime.now().year, 3, 29, 2, 48, 56, 642000)
        )
    

    def test_can_get_datetime_from_line_with_no_datetime(self):
        self.assertEqual(get_datetime_from_line("DEBUG"), None)



class SubmittedLineTests(TestCase):

    def test_can_parse_line(self):
        line = "Jun-01 16:45:57.048 [Task submitter] INFO  nextflow.Session - [d6/31d530] Submitted process > DEMULTIPLEX:CSV_TO_BARCODE (file.csv)"
        identifier, name, process, started = parse_submitted_line(line)
        self.assertEqual(identifier, "d6/31d530")
        self.assertEqual(name, "DEMULTIPLEX:CSV_TO_BARCODE (file.csv)")
        self.assertEqual(process, "DEMULTIPLEX:CSV_TO_BARCODE")
        self.assertEqual(started, datetime(datetime.now().year, 6, 1, 16, 45, 57, 48000))
    

    def test_can_parse_line_with_no_process_arg(self):
        line = "Jun-01 16:45:57.048 [Task submitter] INFO  nextflow.Session - [d6/31d530] Submitted process > DEMULTIPLEX:CSV_TO_BARCODE"
        identifier, name, process, started = parse_submitted_line(line)
        self.assertEqual(identifier, "d6/31d530")
        self.assertEqual(name, "DEMULTIPLEX:CSV_TO_BARCODE")
        self.assertEqual(process, "DEMULTIPLEX:CSV_TO_BARCODE")
        self.assertEqual(started, datetime(datetime.now().year, 6, 1, 16, 45, 57, 48000))
    

    def test_can_handle_no_match(self):
        line = "Jun-01 16:45:57.048 [Task submitter] INFO  nextflow.Session - [d63Z1d530 Submitted process > DEMULTIPLEX:CSV_TO_BARCODE"
        identifier, name, process, started = parse_submitted_line(line)
        self.assertEqual(identifier, "")
        self.assertEqual(name, "")
        self.assertEqual(process, "")
        self.assertIsNone(started)



class CompletedLineTests(TestCase):

    def test_can_parse_line(self):
        line = "Jun-01 16:46:00.365 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 1; name: DEMULTIPLEX:CSV_TO_BARCODE (file.csv); status: COMPLETED; exit: 0; error: -; workDir: /work/d6/31d530a65ef23d1cb302940a782909]"
        identifier, finished, return_code, status = parse_completed_line(line)
        self.assertEqual(identifier, "d6/31d530")
        self.assertEqual(finished, datetime(datetime.now().year, 6, 1, 16, 46, 0, 365000))
        self.assertEqual(return_code, "0")
        self.assertEqual(status, "COMPLETED")
    

    def test_can_parse_failing_line(self):
        line = "Jun-01 16:46:08.878 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 2; name: DEMULTIPLEX:ULTRAPLEX (file.fastq); status: FAILED; exit: 1; error: -; workDir: /work/8a/c2a4dc996d54cad136abeb4e4e309a]"
        identifier, finished, return_code, status = parse_completed_line(line)
        self.assertEqual(identifier, "8a/c2a4dc")
        self.assertEqual(finished, datetime(datetime.now().year, 6, 1, 16, 46, 8, 878000))
        self.assertEqual(return_code, "1")
        self.assertEqual(status, "FAILED")
    

    def test_return_code_non_zero_always_failure(self):
        line = "Jun-01 16:46:08.878 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 2; name: DEMULTIPLEX:ULTRAPLEX (file2.fastq); status: COMPLETED; exit: 25; error: -; workDir: /work/4b/302940a782909c996d54cad31d53d45]"
        identifier, finished, return_code, status = parse_completed_line(line)
        self.assertEqual(identifier, "4b/302940")
        self.assertEqual(finished, datetime(datetime.now().year, 6, 1, 16, 46, 8, 878000))
        self.assertEqual(return_code, "25")
        self.assertEqual(status, "FAILED")
    

    def test_can_handle_no_match(self):
        line = "Jun-01 16:46:08.878 [Task monitor] DEBUG n.processor.TaskPollingMonitor - Task completed > TaskHandler[id: 2; name: DEMULTIPLEX:ULTRAPLEX (file2.fastq); status: COMPLETED; exit: 1; error: -; wrkDir: /work/4b/302940a782909c996d54cad31d53d45"
        identifier, finished, return_code, status = parse_completed_line(line)
        self.assertEqual(identifier, "")
        self.assertIsNone(finished)
        self.assertEqual(return_code, "")
        self.assertEqual(status, "")