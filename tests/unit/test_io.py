from unittest import TestCase
from unittest.mock import patch
from nextflow.io import *

class FileTextTests(TestCase):

    @patch("builtins.open")
    def test_can_get_local_log_text(self, mock_open):
        mock_open.return_value.__enter__.return_value.read.return_value = "line1\nline2"
        self.assertEqual(get_file_text("/ex/file.txt"), "line1\nline2")
        mock_open.assert_called_with("/ex/file.txt", "r")
    

    @patch("builtins.open")
    def test_can_can_handle_no_log_text(self, mock_open):
        mock_open.side_effect = FileNotFoundError
        self.assertEqual(get_file_text("/ex/file.txt"), "")
        mock_open.assert_called_with("/ex/file.txt", "r")