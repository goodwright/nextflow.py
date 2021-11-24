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



class PipelineRunningTests(TestCase):

    def setUp(self):
        self.patch1 = patch("os.path.abspath")
        self.patch2 = patch("os.getcwd")
        self.patch3 = patch("nextflow.pipeline.Pipeline.config_string", new_callable=PropertyMock)
        self.patch4 = patch("os.chdir")
        self.patch5 = patch("subprocess.run")
        self.patch6 = patch("builtins.open")
        self.patch7 = patch("nextflow.pipeline.Execution")
        self.mock_abspath = self.patch1.start()
        self.mock_cwd = self.patch2.start()
        self.mock_config_string = self.patch3.start()
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
        self.mock_config_string.return_value = " -C conf"
        pipeline = Pipeline("/path/run.nf")
        execution = pipeline.run()
        self.mock_abspath.assert_any_call(".")
        self.mock_abspath.assert_any_call("/path/run.nf")
        self.mock_cwd.assert_called_with()
        self.mock_chdir.assert_any_call("abs1")
        self.mock_chdir.assert_any_call("current")
        self.mock_run.assert_any_call(
            f"nextflow -C conf run \"abs2\" ",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="abs1"
        )
        self.mock_Execution.assert_called_with("abs1", "xx_yy", process=self.mock_run.return_value)
        self.assertIs(execution, self.mock_Execution.return_value)
    

    def test_can_run_pipeline_with_arguments(self):
        self.mock_abspath.side_effect = ["abs1", "abs2"]
        self.mock_cwd.return_value = "current"
        self.mock_config_string.return_value = " -C conf"
        pipeline = Pipeline("/path/run.nf")
        execution = pipeline.run(location="runloc", params={"1": "2", "3": "4"}, profile=["test", "test2"])
        self.mock_abspath.assert_any_call("runloc")
        self.mock_abspath.assert_any_call("/path/run.nf")
        self.mock_cwd.assert_called_with()
        self.mock_chdir.assert_any_call("abs1")
        self.mock_chdir.assert_any_call("current")
        self.mock_run.assert_any_call(
            f"nextflow -C conf run \"abs2\" --1='2' --3='4' -profile test,test2",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, shell=True, cwd="abs1"
        )
        self.mock_Execution.assert_called_with("abs1", "xx_yy", process=self.mock_run.return_value)
        self.assertIs(execution, self.mock_Execution.return_value)