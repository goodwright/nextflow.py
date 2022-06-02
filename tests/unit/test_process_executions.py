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
            "good", "bad", "Jul-06", datetime(2021, 7, 6, 1, 2, 3), 1.2, "0"
        )
        self.assertEqual(process_execution.started, 1625529723)