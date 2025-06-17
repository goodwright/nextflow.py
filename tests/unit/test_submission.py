from unittest.mock import Mock
from unittest import TestCase
from nextflow.models import ExecutionSubmission

class SubmissionreationTests(TestCase):

    def test_can_create_execution_submission(self):
        submission = ExecutionSubmission(
            pipeline_path="/main.nf",
            run_path="/executions/123",
            output_path="/executions/123/out",
            log_path="/executions/123/out/nextflow.log",
            nextflow_command="nextflow run /main.nf",
            timezone="UTC",
        )
        self.assertEqual(submission.pipeline_path, "/main.nf")
        self.assertEqual(submission.run_path, "/executions/123")
        self.assertEqual(submission.output_path, "/executions/123/out")
        self.assertEqual(submission.log_path, "/executions/123/out/nextflow.log")
        self.assertEqual(submission.nextflow_command, "nextflow run /main.nf")
        self.assertEqual(submission.timezone, "UTC")