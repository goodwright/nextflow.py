from unittest import TestCase
from unittest.mock import patch
from nextflow.io import *

class FileTextTests(TestCase):

    @patch("builtins.open")
    def test_can_get_local_log_text(self, mock_open):
        mock_open.return_value.__enter__.return_value.read.return_value = "line1\nline2"
        self.assertEqual(get_file_text("/ex/file.txt", ""), "line1\nline2")
        mock_open.assert_called_with("/ex/file.txt", "r")
    

    @patch("builtins.open")
    def test_can_can_handle_no_log_text(self, mock_open):
        mock_open.side_effect = FileNotFoundError
        self.assertEqual(get_file_text("/ex/file.txt", ""), "")
        mock_open.assert_called_with("/ex/file.txt", "r")
    

    @patch("subprocess.run")
    def test_can_get_remote_log_text(self, mock_run):
        mock_run.return_value.stdout = b"line1\nline2"
        mock_run.return_value.returncode = 0
        self.assertEqual(get_file_text("/ex/file.txt", "user@host"), "line1\nline2")
        mock_run.assert_called_with(
            "ssh user@host 'cat /ex/file.txt'",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    

    @patch("subprocess.run")
    def test_can_handle_error_getting_remote_log_text(self, mock_run):
        mock_run.return_value.returncode = 1
        self.assertEqual(get_file_text("/ex/file.txt", "user@host"), "")
        mock_run.assert_called_with(
            "ssh user@host 'cat /ex/file.txt'",
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )