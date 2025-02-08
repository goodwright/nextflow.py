from datetime import datetime, timedelta
from unittest import TestCase
from pathlib import Path
from unittest.mock import PropertyMock, mock_open, patch, Mock, MagicMock
from nextflow.models import ProcessExecution

class ProcessExecutionTest(TestCase):

    def make_process_execution(self, **kwargs):
        kwargs = {
            "identifier": "12/3456", "name": "FASTQC (1)", "submitted": datetime(2021, 7, 4),
            "process": "FASTQC", "path": "12/34567890", "stdout": "good", "stderr": "bad",
            "return_code": "0", "bash": "$", "started": datetime(2021, 7, 5),
            "finished": datetime(2021, 7, 6), "status": "COMPLETED",  **kwargs
        }
        return ProcessExecution(**kwargs)



class ProcessExecutionCreationTests(TestCase):

    def test_can_make_process_execution(self):
        process_execution = ProcessExecution(
            identifier="12/3456", name="FASTQC (1)", submitted=datetime(2021, 7, 4),
            process="FASTQC", path="12/34567890", stdout="good", stderr="bad",
            return_code="0", bash="$", started=datetime(2021, 7, 5),
            finished=datetime(2021, 7, 6), status="COMPLETED"
        )
        self.assertEqual(process_execution.identifier, "12/3456")
        self.assertEqual(process_execution.name, "FASTQC (1)")
        self.assertEqual(process_execution.process, "FASTQC")
        self.assertEqual(process_execution.path, "12/34567890")
        self.assertEqual(process_execution.stdout, "good")
        self.assertEqual(process_execution.stderr, "bad")
        self.assertEqual(process_execution.return_code, "0")
        self.assertEqual(process_execution.bash, "$")
        self.assertEqual(process_execution.submitted, datetime(2021, 7, 4))
        self.assertEqual(process_execution.started, datetime(2021, 7, 5))
        self.assertEqual(process_execution.finished, datetime(2021, 7, 6))
        self.assertEqual(process_execution.status, "COMPLETED")
        self.assertEqual(str(process_execution), "<ProcessExecution: 12/3456>")



class ExecutionDurationTests(ProcessExecutionTest):

    def test_can_get_duration(self):
        process_execution = self.make_process_execution(started=datetime(2020, 1, 1, 12, 2, 1), finished=datetime(2020, 1, 2, 12, 8, 10))
        self.assertEqual(process_execution.duration, timedelta(days=1, seconds=369))
    

    def test_can_handle_not_started(self):
        process_execution = self.make_process_execution(started=None, finished=datetime(2020, 1, 2, 12, 8, 10))
        self.assertIsNone(process_execution.duration)
    

    def test_can_handle_not_finished(self):
        process_execution = self.make_process_execution(started=datetime(2020, 1, 1, 12, 2, 1), finished=None)
        self.assertIsNone(process_execution.duration)



class ProcessExecutionFullPathTests(ProcessExecutionTest):

    def test_can_get_full_path_from_no_path(self):
        process_execution = self.make_process_execution(path="")
        self.assertEqual(process_execution.full_path, None)
    

    def test_can_get_full_path_from_path(self):
        process_execution = self.make_process_execution(path="12/34567890")
        execution = Mock(path="/ex/1")
        process_execution.execution = execution
        self.assertEqual(process_execution.full_path, Path("/ex/1", "work", "12/34567890"))



class InputDataTests(ProcessExecutionTest):

    def setUp(self):
        self.process_execution = self.make_process_execution()
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

    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("nextflow.models.get_file_text")
    def test_can_get_input_data(self, mock_text, mock_path):
        mock_text.return_value = self.text
        mock_path.return_value = Path("/loc")
        self.assertEqual(
            self.process_execution.input_data(),
            ["/work/25/7eaa7786ca/file1.dat", "/work/fe/3b80569ba5/file2.dat"]
        )
        mock_text.assert_called_with(Path("/loc/.command.run"))
    

    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("nextflow.models.get_file_text")
    def test_can_get_input_data_filenames(self, mock_text, mock_path):
        mock_text.return_value = self.text
        mock_path.return_value = Path("/loc")
        self.assertEqual(
            self.process_execution.input_data(include_path=False),
            ["file1.dat", "file2.dat"]
        )
        mock_text.assert_called_with(Path("/loc/.command.run"))
    

    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("nextflow.models.get_file_text")
    def test_can_handle_no_command_run_file(self, mock_text, mock_path):
        mock_text.return_value = ""
        mock_path.return_value = Path("/loc")
        self.assertEqual(
            self.process_execution.input_data(include_path=False), []
        )
        mock_text.assert_called_with(Path("/loc/.command.run"))
    

    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("nextflow.models.get_file_text")
    def test_can_handle_no_staging(self, mock_text, mock_path):
        mock_text.return_value = "xxx"
        mock_path.return_value = Path("/loc")
        self.assertEqual(
            self.process_execution.input_data(include_path=False), []
        )
        mock_text.assert_called_with(Path("/loc/.command.run"))
    

    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    def test_can_handle_no_path(self, mock_path):
        mock_path.return_value = None
        self.assertEqual(
            self.make_process_execution(path="").input_data(), []
        )
    

    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("nextflow.models.get_file_text")
    def test_can_handle_staging_by_copying(self, mock_text, mock_path):
        self.text = """
        nxf_launch() {
            /usr/bin/env python /work/
        }

        nxf_stage() {
            true
            # stage input files
            rm -f suffix_lowered_duplicated_abc.dat
            rm -f suffix_lowered_duplicated_xyz.dat
            cp -fRL /work/25/7eaa7786ca/file1.dat file1.dat
            cp -fRL /work/fe/3b80569ba5/file2.dat file2.dat
        }

        nxf_unstage() {
            true
        """
        mock_text.return_value = self.text
        mock_path.return_value = Path("/loc")
        self.assertEqual(
            self.process_execution.input_data(),
            ["/work/25/7eaa7786ca/file1.dat", "/work/fe/3b80569ba5/file2.dat"]
        )
        mock_text.assert_called_with(Path("/loc/.command.run"))



class AllOutputDataTests(ProcessExecutionTest):

    @patch("nextflow.models.ProcessExecution.input_data")
    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("os.listdir")
    def test_can_get_all_output_data(self, mock_dir, mock_path, mock_input):
        mock_input.return_value = ["file2"]
        mock_dir.return_value = ["file1", "file2", ".command.run", ".exitcode", "file3"]
        mock_path.return_value = "/loc"
        self.assertEqual(
            self.make_process_execution().all_output_data(),
            [str(Path("/loc/file1")), str(Path("/loc/file3"))]
        )
        mock_input.assert_called_with(include_path=False)
        mock_dir.assert_called_with(mock_path.return_value)
    

    @patch("nextflow.models.ProcessExecution.input_data")
    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("os.listdir")
    def test_can_get_all_output_filenames(self, mock_dir, mock_path, mock_input):
        mock_input.return_value = ["file2"]
        mock_dir.return_value = ["file1", "file2", ".command.run", ".exitcode", "file3"]
        mock_path.return_value = "/loc"
        self.assertEqual(
            self.make_process_execution().all_output_data(include_path=False),
            ["file1", "file3"]
        )
        mock_input.assert_called_with(include_path=False)
        mock_dir.assert_called_with(mock_path.return_value)


    @patch("nextflow.models.ProcessExecution.input_data")
    @patch("nextflow.models.ProcessExecution.full_path", new_callable=PropertyMock)
    @patch("os.listdir")
    def test_can_handle_no_path(self, mock_dir, mock_path, mock_input):
        mock_input.return_value = ["file2"]
        mock_dir.return_value = ["file1", "file2", ".command.run", ".exitcode", "file3"]
        mock_path.return_value = "/loc"
        self.assertEqual(
            self.make_process_execution(path="").all_output_data(), []
        )
        self.assertFalse(mock_input.called)
        self.assertFalse(mock_dir.called)