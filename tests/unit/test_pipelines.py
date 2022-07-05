from os import pipe
from unittest import TestCase
from unittest.mock import PropertyMock, mock_open, patch, Mock, MagicMock
from nextflow.pipeline import *

class PipelineCreationTests(TestCase):

    def test_can_create_pipeline(self):
        pipeline = Pipeline("/path/run.nf")
        self.assertEqual(pipeline.path, "/path/run.nf")
        self.assertEqual(str(pipeline), "<Pipeline (/path/run.nf)>")
    

    def test_can_create_pipeline_with_options(self):
        pipeline = Pipeline("/path/run.nf", config="/path/nextflow.config")
        self.assertEqual(pipeline.path, "/path/run.nf")
        self.assertEqual(pipeline.config, "/path/nextflow.config")



class ConfigStringTests(TestCase):

    @patch("os.path.abspath")
    def test_can_get_config_string(self, mock_abspath):
        mock_abspath.return_value = "/full/path"
        pipeline = Pipeline("/path/run.nf", config="schema.json")
        self.assertEqual(pipeline.config_string, ' -C "/full/path"')
    

    def test_can_handle_no_config(self):
        pipeline = Pipeline("/path/run.nf")
        self.assertEqual(pipeline.config_string, "")



class PipelineCommandStringTests(TestCase):

    @patch("os.path.abspath")
    def test_can_get_command_string(self, mock_abs):
        pipeline = Pipeline("/path/run.nf")
        mock_abs.return_value = "/full/path/run.nf"
        string = pipeline.create_command_string(None, None, None)
        mock_abs.assert_called_with("/path/run.nf")
        self.assertEqual(string.strip(), 'NXF_ANSI_LOG=false nextflow run "/full/path/run.nf"')
    

    @patch("os.path.abspath")
    def test_can_get_command_string_with_params(self, mock_abs):
        pipeline = Pipeline("/path/run.nf")
        mock_abs.return_value = "/full/path/run.nf"
        string = pipeline.create_command_string({"A": "B", "C": "D"}, None, None)
        mock_abs.assert_called_with("/path/run.nf")
        self.assertEqual(string.strip(), 'NXF_ANSI_LOG=false nextflow run "/full/path/run.nf" --A=\'B\' --C=\'D\'')
    

    @patch("os.path.abspath")
    def test_can_get_command_string_with_profile(self, mock_abs):
        pipeline = Pipeline("/path/run.nf")
        mock_abs.return_value = "/full/path/run.nf"
        string = pipeline.create_command_string({"A": "B", "C": "D"}, ["prof1", "prof2"], None)
        mock_abs.assert_called_with("/path/run.nf")
        self.assertEqual(string.strip(), 'NXF_ANSI_LOG=false nextflow run "/full/path/run.nf" --A=\'B\' --C=\'D\' -profile prof1,prof2')
    

    @patch("os.path.abspath")
    def test_can_get_command_string_with_nf_version(self, mock_abs):
        pipeline = Pipeline("/path/run.nf")
        mock_abs.return_value = "/full/path/run.nf"
        string = pipeline.create_command_string({"A": "B", "C": "D"}, ["prof1", "prof2"], "1.2.3")
        mock_abs.assert_called_with("/path/run.nf")
        self.assertEqual(string.strip(), 'NXF_ANSI_LOG=false NXF_VER=1.2.3 nextflow run "/full/path/run.nf" --A=\'B\' --C=\'D\' -profile prof1,prof2')


    
class PipelineRunningTests(TestCase):

    def setUp(self):
        self.patch1 = patch("os.path.abspath")
        self.patch2 = patch("os.getcwd")
        self.patch3 = patch("nextflow.pipeline.Pipeline.create_command_string")
        self.patch4 = patch("os.chdir")
        self.patch5 = patch("subprocess.run")
        self.patch6 = patch("nextflow.pipeline.Execution.create_from_location")
        self.mock_abspath = self.patch1.start()
        self.mock_cwd = self.patch2.start()
        self.mock_command_string = self.patch3.start()
        self.mock_chdir = self.patch4.start()
        self.mock_run = self.patch5.start()
        self.mock_create = self.patch6.start()


    def tearDown(self):
        self.patch1.stop()
        self.patch2.stop()
        self.patch3.stop()
        self.patch4.stop()
        self.patch5.stop()
        self.patch6.stop()


    def test_can_run_basic_pipeline(self):
        self.mock_abspath.return_value = "/abs/loc"
        self.mock_cwd.return_value = "current"
        self.mock_command_string.return_value = "run script.nf"
        pipeline = Pipeline("/path/run.nf")
        execution = pipeline.run()
        self.mock_abspath.assert_any_call(".")
        self.mock_cwd.assert_called_with()
        self.mock_command_string.assert_called_with(None, None, None)
        self.mock_chdir.assert_any_call("/abs/loc")
        self.mock_chdir.assert_any_call("current")
        self.mock_run.assert_any_call(
            f"run script.nf",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="/abs/loc"
        )
        self.mock_create.assert_called_with(
            "/abs/loc",
            self.mock_run.return_value.stdout,
            self.mock_run.return_value.stderr,
            self.mock_run.return_value.returncode,
        )
        self.assertIs(execution, self.mock_create.return_value)
    

    def test_can_run_pipeline_with_options(self):
        self.mock_abspath.return_value = "/abs/loc"
        self.mock_cwd.return_value = "current"
        self.mock_command_string.return_value = "run script.nf"
        pipeline = Pipeline("/path/run.nf")
        execution = pipeline.run(
            location="/path", params="params", profile="profiles", version="version"
        )
        self.mock_abspath.assert_any_call("/path")
        self.mock_cwd.assert_called_with()
        self.mock_command_string.assert_called_with("params", "profiles", "version")
        self.mock_chdir.assert_any_call("/abs/loc")
        self.mock_chdir.assert_any_call("current")
        self.mock_run.assert_any_call(
            f"run script.nf",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="/abs/loc"
        )
        self.mock_create.assert_called_with(
            "/abs/loc",
            self.mock_run.return_value.stdout,
            self.mock_run.return_value.stderr,
            self.mock_run.return_value.returncode,
        )
        self.assertIs(execution, self.mock_create.return_value)
    


class PipelineRunPollTests(TestCase):

    def setUp(self):
        self.patch1 = patch("os.path.abspath")
        self.patch2 = patch("os.getcwd")
        self.patch3 = patch("nextflow.pipeline.Pipeline.create_command_string")
        self.patch4 = patch("os.chdir")
        self.patch5 = patch("builtins.open")
        self.patch6 = patch("subprocess.Popen")
        self.patch7 = patch("time.sleep")
        self.patch8 = patch("os.path.exists")
        self.patch9 = patch("nextflow.pipeline.Execution.create_from_location")
        self.patch10 = patch("os.remove")
        self.mock_abspath = self.patch1.start()
        self.mock_cwd = self.patch2.start()
        self.mock_command_string = self.patch3.start()
        self.mock_chdir = self.patch4.start()
        self.mock_open = self.patch5.start()
        self.mock_popen = self.patch6.start()
        self.mock_sleep = self.patch7.start()
        self.mock_exists = self.patch8.start()
        self.mock_create = self.patch9.start()
        self.mock_remove = self.patch10.start()

        self.file_contexts = [Mock() for _ in range(10)]
        self.mock_open.return_value.__enter__.side_effect = self.file_contexts
        self.file_contexts[4].read.return_value = "g"
        self.file_contexts[5].read.return_value = "b"
        self.file_contexts[6].read.return_value = "goo"
        self.file_contexts[7].read.return_value = "ba"
        self.file_contexts[8].read.return_value = "good"
        self.file_contexts[9].read.return_value = "bad"


    def tearDown(self):
        self.patch1.stop()
        self.patch2.stop()
        self.patch3.stop()
        self.patch4.stop()
        self.patch5.stop()
        self.patch6.stop()
        self.patch7.stop()
        self.patch8.stop()
        self.patch9.stop()


    def test_can_poll_basic_pipeline(self):
        self.mock_abspath.return_value = "/abs/loc"
        self.mock_cwd.return_value = "current"
        self.mock_command_string.return_value = "run script.nf"
        process = Mock()
        process.poll.side_effect = [None, None, None, "0"]
        self.mock_popen.return_value = process
        self.mock_exists.side_effect = [False, True, True, True, True, True, True, False, False]
        pipeline = Pipeline("/path/run.nf")
        executions = list(pipeline.run_and_poll())
        self.assertEqual(len(executions), 3)
        for execution in executions:
            self.assertIs(execution, self.mock_create.return_value)
        self.mock_abspath.assert_any_call(".")
        self.mock_cwd.assert_called_with()
        self.mock_command_string.assert_called_with(None, None, None)
        self.mock_chdir.assert_any_call("/abs/loc")
        self.mock_chdir.assert_any_call("current")
        self.mock_open.assert_any_call("nfstdout", "w")
        self.mock_open.assert_any_call("nfstderr", "w")
        self.mock_popen.assert_called_with(
            "run script.nf",
            stdout=self.file_contexts[0], stderr=self.file_contexts[1],
            universal_newlines=True, shell=True, cwd="/abs/loc"
        )
        self.mock_exists.assert_any_call(os.path.join("/abs/loc", ".nextflow.log"))
        self.mock_exists.assert_any_call(os.path.join("/abs/loc", ".nextflow", "history"))
        self.assertEqual(self.mock_exists.call_count, 9)
        self.mock_sleep.assert_called_with(5)
        self.assertEqual(self.mock_sleep.call_count, 4)
        self.mock_create.assert_any_call("/abs/loc", "g", "b", None)
        self.mock_create.assert_any_call("/abs/loc", "goo", "ba", None)
        self.mock_create.assert_any_call("/abs/loc", "good", "bad", "0")
    

    def test_can_poll_pipeline_with_options(self):
        self.mock_abspath.return_value = "/abs/loc"
        self.mock_cwd.return_value = "current"
        self.mock_command_string.return_value = "run script.nf"
        process = Mock()
        process.poll.side_effect = [None, None, None, "0"]
        self.mock_popen.return_value = process
        self.mock_exists.side_effect = [False, True, True, True, True, True, True, True, True]
        pipeline = Pipeline("/path/run.nf")
        executions = list(pipeline.run_and_poll(
            location="otherloc", params="params", profile="profiles",
            version="version", sleep=2
        ))
        self.assertEqual(len(executions), 3)
        for execution in executions:
            self.assertIs(execution, self.mock_create.return_value)
        self.mock_abspath.assert_any_call("otherloc")
        self.mock_cwd.assert_called_with()
        self.mock_command_string.assert_called_with("params", "profiles", "version")
        self.mock_chdir.assert_any_call("/abs/loc")
        self.mock_chdir.assert_any_call("current")
        self.mock_open.assert_any_call("nfstdout", "w")
        self.mock_open.assert_any_call("nfstderr", "w")
        self.mock_popen.assert_called_with(
            "run script.nf",
            stdout=self.file_contexts[0], stderr=self.file_contexts[1],
            universal_newlines=True, shell=True, cwd="/abs/loc"
        )
        self.mock_exists.assert_any_call(os.path.join("/abs/loc", ".nextflow.log"))
        self.mock_exists.assert_any_call(os.path.join("/abs/loc", ".nextflow", "history"))
        self.assertEqual(self.mock_exists.call_count, 9)
        self.mock_sleep.assert_called_with(2)
        self.assertEqual(self.mock_sleep.call_count, 4)
        self.mock_create.assert_any_call("/abs/loc", "g", "b", None)
        self.mock_create.assert_any_call("/abs/loc", "goo", "ba", None)
        self.mock_create.assert_any_call("/abs/loc", "good", "bad", "0")
        self.mock_remove.assert_any_call("nfstdout")
        self.mock_remove.assert_any_call("nfstderr")



class DirectRunningTests(TestCase):

    @patch("nextflow.pipeline.Pipeline")
    def test_can_run_directly(self, mock_Pipeline):
        execution = run("/path", "/config", 1, 2, a=3, b=4)
        self.assertIs(execution, mock_Pipeline.return_value.run.return_value)
        mock_Pipeline.assert_called_with(path="/path", config="/config")
        mock_Pipeline.return_value.run.assert_called_with(1, 2, a=3, b=4)



class DirectPollingTests(TestCase):

    @patch("nextflow.pipeline.Pipeline")
    def test_can_run_directly(self, mock_Pipeline):
        execution = run_and_poll("/path", "/config", 1, 2, a=3, b=4)
        self.assertIs(execution, mock_Pipeline.return_value.run_and_poll.return_value)
        mock_Pipeline.assert_called_with(path="/path", config="/config")
        mock_Pipeline.return_value.run_and_poll.assert_called_with(1, 2, a=3, b=4)