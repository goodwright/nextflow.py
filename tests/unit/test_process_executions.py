from datetime import datetime
from unittest import TestCase
from unittest.mock import PropertyMock, mock_open, patch, Mock, MagicMock
from nextflow.execution import *

class ProcessExecutionCreationTests(TestCase):

    def test_can_create_execution(self):
        execution = Mock(id="xxx_yyy")
        process_execution = ProcessExecution(
            execution, "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "Jul-06", datetime(2021, 7, 6), 1.2, "0"
        )
        self.assertEqual(process_execution.execution, execution)
        self.assertEqual(process_execution.hash, "12/3456")
        self.assertEqual(process_execution.process, "FASTQC")
        self.assertEqual(process_execution.name, "FASTQC (1)")
        self.assertEqual(process_execution.status, "COMPLETED")
        self.assertEqual(process_execution.stdout, "good")
        self.assertEqual(process_execution.stderr, "bad")
        self.assertEqual(process_execution.started_string, "Jul-06")
        self.assertEqual(process_execution.started_dt, datetime(2021, 7, 6))
        self.assertEqual(process_execution.duration, 1.2)
        self.assertEqual(process_execution.returncode, "0")
        self.assertEqual(str(process_execution), "<ProcessExecution from xxx_yyy: FASTQC (1)>")



class ProcessExecutionStartedTests(TestCase):

    def test_can_get_started(self):
        process_execution = ProcessExecution(
             Mock(), "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "Jul-06", datetime(2021, 1, 6, 1, 2, 3), 1.2, "0"
        )
        self.assertEqual(process_execution.started, 1609894923)



class InputDataTests(TestCase):

    def setUp(self):
        self.process_execution = ProcessExecution(
             Mock(), "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "Jul-06", datetime(2021, 1, 6, 1, 2, 3), 1.2, "0"
        )

    @patch("nextflow.execution.get_process_directory")
    @patch("os.listdir")
    @patch("os.path.islink")
    @patch("os.path.realpath")
    def test_can_get_input_data(self, mock_path, mock_link, mock_dir, mock_pd):
        mock_dir.return_value = ["file1", "file2", "file3", "file4"]
        mock_link.side_effect = [True, True, False, False]
        mock_path.side_effect = ["path1", "path2"]
        
        self.assertEqual(self.process_execution.input_data(), ["path1", "path2"])
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_dir.assert_called_with(mock_pd.return_value)
    

    @patch("nextflow.execution.get_process_directory")
    @patch("os.listdir")
    @patch("os.path.islink")
    def test_can_get_input_data_filenames(self, mock_link, mock_dir, mock_pd):
        mock_dir.return_value = ["file1", "file2", "file3", "file4"]
        mock_link.side_effect = [True, True, False, False]
        self.assertEqual(self.process_execution.input_data(include_path=False), ["file1", "file2"])
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_dir.assert_called_with(mock_pd.return_value)



class AllOutputDataTests(TestCase):

    def setUp(self):
        self.process_execution = ProcessExecution(
             Mock(), "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "Jul-06", datetime(2021, 1, 6, 1, 2, 3), 1.2, "0"
        )
    

    @patch("nextflow.execution.get_process_directory")
    @patch("os.listdir")
    @patch("os.path.islink")
    def test_can_get_all_output_data(self, mock_link, mock_dir, mock_pd):
        mock_pd.return_value = "/loc"
        mock_dir.return_value = ["file1", "file2", ".command.run", ".exitcode", "file3"]
        mock_link.side_effect = [False, True, False]
        self.assertEqual(
            self.process_execution.all_output_data(),
            [str(Path("/loc/file1")), str(Path("/loc/file3"))]
        )
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_dir.assert_called_with(mock_pd.return_value)
        mock_link.assert_any_call(Path("/loc/file1"))
        mock_link.assert_any_call(Path("/loc/file3"))
    

    @patch("nextflow.execution.get_process_directory")
    @patch("os.listdir")
    @patch("os.path.islink")
    def test_can_get_all_output_filenames(self, mock_link, mock_dir, mock_pd):
        mock_pd.return_value = "/loc"
        mock_dir.return_value = ["file1", "file2", ".command.run", ".exitcode", "file3"]
        mock_link.side_effect = [False, True, False]
        self.assertEqual(
            self.process_execution.all_output_data(include_path=False),
            ["file1", "file3"]
        )
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_dir.assert_called_with(mock_pd.return_value)
        mock_link.assert_any_call(Path("/loc/file1"))
        mock_link.assert_any_call(Path("/loc/file3"))