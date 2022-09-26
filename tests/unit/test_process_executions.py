from datetime import datetime
from unittest import TestCase
from unittest.mock import PropertyMock, mock_open, patch, Mock, MagicMock
from nextflow.execution import *

class ProcessExecutionCreationTests(TestCase):

    def test_can_create_execution(self):
        execution = Mock(id="xxx_yyy")
        process_execution = ProcessExecution(
            execution, "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "$", "Jul-06", datetime(2021, 7, 6), 1.2, "0"
        )
        self.assertEqual(process_execution.execution, execution)
        self.assertEqual(process_execution.hash, "12/3456")
        self.assertEqual(process_execution.process, "FASTQC")
        self.assertEqual(process_execution.name, "FASTQC (1)")
        self.assertEqual(process_execution.status, "COMPLETED")
        self.assertEqual(process_execution.stdout, "good")
        self.assertEqual(process_execution.stderr, "bad")
        self.assertEqual(process_execution.bash, "$")
        self.assertEqual(process_execution.started_string, "Jul-06")
        self.assertEqual(process_execution.started_dt, datetime(2021, 7, 6))
        self.assertEqual(process_execution.duration, 1.2)
        self.assertEqual(process_execution.returncode, "0")
        self.assertEqual(str(process_execution), "<ProcessExecution from xxx_yyy: FASTQC (1)>")



class ProcessExecutionStartedTests(TestCase):

    def test_can_get_started(self):
        process_execution = ProcessExecution(
             Mock(), "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "$", "Jul-06", datetime(2021, 1, 6, 1, 2, 3), 1.2, "0"
        )
        self.assertEqual(process_execution.started, 1609894923)



class InputDataTests(TestCase):

    def setUp(self):
        self.process_execution = ProcessExecution(
             Mock(), "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "$", "Jul-06", datetime(2021, 1, 6, 1, 2, 3), 1.2, "0"
        )
        self.text = """
        nxf_launch() {
            /usr/bin/env python /work/
        }

        nxf_stage() {
            true
            # stage input files
            rm -f suffix_lowered_duplicated_abc.dat
            rm -f suffix_lowered_duplicated_xyz.dat
            ln -s /work/25/7eaa7786ca/file1.dat file1.dat
            ln -s /work/fe/3b80569ba5/file2.dat file2.dat
        }

        nxf_unstage() {
            true
        """

    @patch("nextflow.execution.get_process_directory")
    @patch("builtins.open")
    def test_can_get_input_data(self, mock_open, mock_pd):
        mock_pd.return_value = "/loc"
        mock_open.return_value.__enter__.return_value.read.return_value = self.text
        self.assertEqual(
            self.process_execution.input_data(),
            ["/work/25/7eaa7786ca/file1.dat", "/work/fe/3b80569ba5/file2.dat"]
        )
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_open.assert_called_with(Path(f"/loc/.command.run"))
    

    @patch("nextflow.execution.get_process_directory")
    @patch("builtins.open")
    def test_can_get_input_data_filenames(self, mock_open, mock_pd):
        mock_pd.return_value = "/loc"
        mock_open.return_value.__enter__.return_value.read.return_value = self.text
        self.assertEqual(
            self.process_execution.input_data(include_path=False),
            ["file1.dat", "file2.dat"]
        )
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_open.assert_called_with(Path(f"/loc/.command.run"))
    

    @patch("nextflow.execution.get_process_directory")
    @patch("builtins.open")
    def test_can_handle_no_command_run_file(self, mock_open, mock_pd):
        mock_pd.return_value = "/loc"
        mock_open.side_effect = FileNotFoundError
        self.assertEqual(self.process_execution.input_data(), [])
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_open.assert_called_with(Path(f"/loc/.command.run"))
    

    @patch("nextflow.execution.get_process_directory")
    @patch("builtins.open")
    def test_can_handle_no_staging(self, mock_open, mock_pd):
        mock_pd.return_value = "/loc"
        mock_open.return_value.__enter__.return_value.read.return_value = "xxx"
        self.assertEqual(self.process_execution.input_data(), [])
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_open.assert_called_with(Path(f"/loc/.command.run"))



class AllOutputDataTests(TestCase):

    def setUp(self):
        self.process_execution = ProcessExecution(
             Mock(), "12/3456", "FASTQC", "FASTQC (1)", "COMPLETED",
            "good", "bad", "$", "Jul-06", datetime(2021, 1, 6, 1, 2, 3), 1.2, "0"
        )
    

    @patch("nextflow.execution.get_process_directory")
    @patch("nextflow.execution.ProcessExecution.input_data")
    @patch("os.listdir")
    def test_can_get_all_output_data(self, mock_dir, mock_input, mock_pd):
        mock_pd.return_value = "/loc"
        mock_dir.return_value = ["file1", "file2", ".command.run", ".exitcode", "file3"]
        mock_input.return_value = ["file2"]
        self.assertEqual(
            self.process_execution.all_output_data(),
            [str(Path("/loc/file1")), str(Path("/loc/file3"))]
        )
        mock_input.assert_called_with(include_path=False)
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_dir.assert_called_with(mock_pd.return_value)
    

    @patch("nextflow.execution.get_process_directory")
    @patch("nextflow.execution.ProcessExecution.input_data")
    @patch("os.listdir")
    def test_can_get_all_output_filenames(self, mock_dir, mock_input, mock_pd):
        mock_pd.return_value = "/loc"
        mock_dir.return_value = ["file1", "file2", ".command.run", ".exitcode", "file3"]
        mock_input.return_value = ["file2"]
        self.assertEqual(
            self.process_execution.all_output_data(include_path=False),
            ["file1", "file3"]
        )
        mock_input.assert_called_with(include_path=False)
        mock_pd.assert_called_with(self.process_execution.execution, "12/3456")
        mock_dir.assert_called_with(mock_pd.return_value)