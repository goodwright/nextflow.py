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
        pipeline = Pipeline("/path/run.nf", schema="schema.json", config="/path/nextflow.config")
        self.assertEqual(pipeline.path, "/path/run.nf")
        self.assertEqual(pipeline.schema, "schema.json")
        self.assertEqual(pipeline.config, "/path/nextflow.config")



class PipelineInputSchemaTests(TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data=json.dumps({"definitions": {"1": 2}}))
    def test_can_get_input_schema(self, mock_open):
        pipeline = Pipeline("/path/run.nf", schema="schema.json")
        self.assertEqual(pipeline.input_schema, {"1": 2})
        mock_open.assert_called_with("schema.json")
    

    def test_can_handle_no_schema(self):
        pipeline = Pipeline("/path/run.nf")
        self.assertIsNone(pipeline.input_schema)



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
        string = pipeline.create_command_string(None, None)
        mock_abs.assert_called_with("/path/run.nf")
        self.assertEqual(string.strip(), 'NXF_ANSI_LOG=false nextflow run "/full/path/run.nf"')
    

    @patch("os.path.abspath")
    def test_can_get_command_string_with_params(self, mock_abs):
        pipeline = Pipeline("/path/run.nf")
        mock_abs.return_value = "/full/path/run.nf"
        string = pipeline.create_command_string({"A": "B", "C": "D"}, None)
        mock_abs.assert_called_with("/path/run.nf")
        self.assertEqual(string.strip(), 'NXF_ANSI_LOG=false nextflow run "/full/path/run.nf" --A=\'B\' --C=\'D\'')
    

    @patch("os.path.abspath")
    def test_can_get_command_string_with_profile(self, mock_abs):
        pipeline = Pipeline("/path/run.nf")
        mock_abs.return_value = "/full/path/run.nf"
        string = pipeline.create_command_string({"A": "B", "C": "D"}, ["prof1", "prof2"])
        mock_abs.assert_called_with("/path/run.nf")
        self.assertEqual(string.strip(), 'NXF_ANSI_LOG=false nextflow run "/full/path/run.nf" --A=\'B\' --C=\'D\' -profile prof1,prof2')


    
class PipelineRunningTests(TestCase):

    def setUp(self):
        self.patch1 = patch("os.path.abspath")
        self.patch2 = patch("os.getcwd")
        self.patch3 = patch("nextflow.pipeline.Pipeline.create_command_string")
        self.patch4 = patch("os.chdir")
        self.patch5 = patch("subprocess.run")
        self.patch6 = patch("builtins.open")
        self.patch7 = patch("nextflow.pipeline.Execution.create_from_location")
        self.mock_abspath = self.patch1.start()
        self.mock_cwd = self.patch2.start()
        self.mock_command_string = self.patch3.start()
        self.mock_chdir = self.patch4.start()
        self.mock_run = self.patch5.start()
        self.mock_open = self.patch6.start()
        self.mock_Execution = self.patch7.start()
        open_return = MagicMock()
        mock_file = Mock()
        open_return.__enter__.return_value = mock_file
        mock_file.read.return_value = "abc [xx_yy] def"
        self.mock_open.return_value = open_return


    def tearDown(self):
        self.patch1.stop()
        self.patch2.stop()
        self.patch3.stop()
        self.patch4.stop()
        self.patch5.stop()
        self.patch6.stop()
        self.patch7.stop()


    def test_can_run_basic_pipeline(self):
        self.mock_abspath.side_effect = ["abs1", "abs2"]
        self.mock_cwd.return_value = "current"
        self.mock_command_string.return_value = "run script.nf"
        pipeline = Pipeline("/path/run.nf")
        execution = pipeline.run()
        self.mock_abspath.assert_any_call(".")
        self.mock_cwd.assert_called_with()
        self.mock_chdir.assert_any_call("abs1")
        self.mock_chdir.assert_any_call("current")
        self.mock_run.assert_any_call(
            f"run script.nf",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="abs1"
        )
        self.mock_Execution.assert_called_with(
            "abs1",
            self.mock_run.return_value.stdout,
            self.mock_run.return_value.stderr,
            self.mock_run.return_value.returncode,
            True
        )
        self.assertIs(execution, self.mock_Execution.return_value)
    


class PipelineRunPollTests(TestCase):

    @patch("os.path.abspath")
    @patch("os.getcwd")
    @patch("nextflow.pipeline.Pipeline.create_command_string")
    @patch("os.chdir")
    @patch("subprocess.Popen")
    @patch("time.sleep")
    @patch("builtins.open")
    @patch("os.path.exists")
    @patch("nextflow.pipeline.Execution.create_from_location")
    def test(self, mock_ex, mock_exist, mock_open, mock_sleep, mock_pop, mock_ch, mock_com, mock_cwd, mock_abs):
        mock_abs.return_value = "/abs/loc"
        mock_cwd.return_value = "current"
        mock_com.return_value = "nextflow run"
        mock_pop.return_value.poll.side_effect = [None, None, None, 0]
        mock_pop.return_value.communicate.return_value = ["out", "err"]
        mock_exist.side_effect = [False, True, True, True, True, True, True]
        pipeline = Pipeline("/path/run.nf")
        executions = list(pipeline.run_and_poll(
            location="/loc", params="PARAMS", profile="PROFILE", sleep=2
        ))
        mock_abs.assert_called_with("/loc")
        mock_cwd.assert_called_with()
        mock_com.assert_called_with("PARAMS", "PROFILE")
        mock_ch.assert_any_call("/abs/loc")
        mock_ch.assert_any_call("current")
        mock_pop.assert_called_with(
            "nextflow run", stdout=-1, stderr=-1, universal_newlines=True,
            shell=True, cwd="/abs/loc"
        )
        mock_sleep.assert_called_with(2)
        self.assertEqual(mock_sleep.call_count, 4)
        self.assertEqual(len(executions), 3)
        mock_ex.assert_any_call("/abs/loc", "", "", None, use_nextflow=False)
        mock_ex.assert_any_call("/abs/loc", "", "", None, use_nextflow=False)
        mock_ex.assert_any_call("/abs/loc", "out", "err", 0, use_nextflow=True)