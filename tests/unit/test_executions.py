from datetime import datetime, timedelta
from unittest import TestCase
from unittest.mock import Mock
from nextflow.models import Execution

class ExecutionTest(TestCase):

    def make_execution(self, **kwargs):
        kwargs = {
            "identifier": "xxx_yyy", "stdout": "ok", "stderr": "bad", "return_code": "1",
            "started": datetime(2020, 1, 1), "finished": datetime(2020, 1, 2),
            "command": "nf run", "log": "N E", "path": "/ex/1",
            "process_executions": [Mock(), Mock()], **kwargs
        }
        return Execution(**kwargs)


class ExecutionCreationTests(TestCase):

    def test_can_create_execution(self):
        process_executions = [Mock(), Mock()]
        execution = Execution(
            identifier="xxx_yyy", stdout="ok", stderr="bad", return_code="1",
            started=datetime(2020, 1, 1), finished=datetime(2020, 1, 2),
            command="nf run", log="N E", path="/ex/1", process_executions=process_executions
        )
        self.assertEqual(execution.identifier, "xxx_yyy")
        self.assertEqual(execution.stdout, "ok")
        self.assertEqual(execution.stderr, "bad")
        self.assertEqual(execution.return_code, "1")
        self.assertEqual(execution.started, datetime(2020, 1, 1))
        self.assertEqual(execution.finished, datetime(2020, 1, 2))
        self.assertEqual(execution.command, "nf run")
        self.assertEqual(execution.log, "N E")
        self.assertEqual(execution.path, "/ex/1")
        self.assertEqual(execution.process_executions, process_executions)
        self.assertEqual(str(execution), "<Execution: xxx_yyy>")



class ExecutionDurationTests(ExecutionTest):

    def test_can_get_duration(self):
        execution = self.make_execution(started=datetime(2020, 1, 1, 12, 2, 1), finished=datetime(2020, 1, 2, 12, 8, 10))
        self.assertEqual(execution.duration, timedelta(days=1, seconds=369))
    

    def test_can_handle_not_finished(self):
        execution = self.make_execution(started=datetime(2020, 1, 1, 12, 2, 1), finished=None)
        self.assertIsNone(execution.duration)



class ExecutionStatusTests(ExecutionTest):

    def test_can_get_status_ok(self):
        execution = self.make_execution(return_code="0")
        self.assertEqual(execution.status, "OK")
    

    def test_can_get_status_error(self):
        execution = self.make_execution(return_code="1")
        self.assertEqual(execution.status, "ERROR")
    

    def test_can_get_status_not_finished(self):
        execution = self.make_execution(return_code="")
        self.assertEqual(execution.status, "-")