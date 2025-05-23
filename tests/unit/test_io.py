import zoneinfo
from unittest import TestCase
from unittest.mock import patch, Mock
from datetime import datetime
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
    

    def test_can_use_custom_io(self):
        io = Mock()
        io.read.return_value = "line1\nline2"
        self.assertEqual(get_file_text("/ex/file.txt", io=io), "line1\nline2")
        io.read.assert_called_with("/ex/file.txt")
    

    def test_can_handle_custom_io_with_no_file(self):
        io = Mock()
        io.read.side_effect = FileNotFoundError
        self.assertEqual(get_file_text("/ex/file.txt", io=io), "")
        io.read.assert_called_with("/ex/file.txt")



class FileCreationTimeTests(TestCase):

    @patch("os.path.getctime")
    def test_can_get_creation_time(self, mock_getctime):
        mock_getctime.return_value = 123456
        self.assertEqual(get_file_creation_time("/ex/file.txt"), datetime.fromtimestamp(123456))
        mock_getctime.assert_called_with("/ex/file.txt")
    

    @patch("os.path.getctime")
    def test_can_get_creation_time_with_timezone(self, mock_getctime):
        mock_getctime.return_value = 123456
        self.assertEqual(
            get_file_creation_time("/ex/file.txt", timezone="America/New_York"),
            datetime(1970, 1, 2, 5, 17, 36)
        )
        mock_getctime.assert_called_with("/ex/file.txt")
    

    def test_can_use_custom_io(self):
        io = Mock()
        io.ctime.return_value = datetime.fromtimestamp(123456)
        self.assertEqual(get_file_creation_time("/ex/file.txt", io=io), datetime.fromtimestamp(123456))
        io.ctime.assert_called_with("/ex/file.txt")
    

    @patch("os.path.getctime")
    def test_can_handle_no_file(self, mock_getctime):
        mock_getctime.side_effect = FileNotFoundError
        self.assertEqual(get_file_creation_time("/ex/file.txt"), None)
        mock_getctime.assert_called_with("/ex/file.txt")



class ProcessIdsToPathsTest(TestCase):

    @patch("glob.glob")
    def test_can_get_paths(self, mock_glob):
        process_ids = ["ab/123456", "cd/7890123"]
        mock_glob.return_value = (
            "/ex/work/xx/yyyyyyy",
            "/ex/work/cd/789012345678"
        )
        paths = get_process_ids_to_paths(process_ids, "/ex")
        self.assertEqual(paths, {"cd/7890123": os.path.join("/ex", "work", "cd/789012345678")})
        mock_glob.assert_called_with(os.path.join("/ex", "work", "*", "*"))
    

    def test_can_use_custom_io(self):
        io = Mock()
        io.glob.return_value = (
            "/ex/work/xx/yyyyyyy",
            "/ex/work/cd/789012345678"
        )
        process_ids = ["ab/123456", "cd/7890123"]
        paths = get_process_ids_to_paths(process_ids, "/ex", io=io)
        self.assertEqual(paths, {"cd/7890123": os.path.join("/ex", "work", "cd/789012345678")})
        io.glob.assert_called_with(os.path.join("/ex", "work", "*", "*"))