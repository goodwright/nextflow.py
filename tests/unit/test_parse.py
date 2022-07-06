from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from nextflow.parse import get_imports, get_module_paths, get_workflows

class ImportTests(TestCase):

    def test_can_get_imports(self):
        imports = get_imports("""
        include { DUPLICATE_AND_LOWER } from './duplicateandlower';
        include XXX from '123';
        include{APPEND}from"../modules/append.nf";
        """)
        self.assertEqual(imports, {
            "DUPLICATE_AND_LOWER": "./duplicateandlower.nf",
            "APPEND": "../modules/append.nf",
        })



class WorkflowTests(TestCase):

    def test_can_get_workflows(self):
        workflows = get_workflows("""
        workflow {
            line1
        }
        workflow W1 {line2 }
        workflowz {
            line3
        }
        """)
        self.assertEqual(workflows, [
            {"name": "", "body": "line1"},
            {"name": "W1", "body": "line2"},
        ])



class ModuleTests(TestCase):

    @patch("builtins.open")
    @patch("nextflow.parse.get_imports")
    @patch("nextflow.parse.get_workflows")
    def test_can_get_modules(self, mock_wf, mock_imp, mock_open):
        mock_open.return_value.__enter__.return_value.read.side_effect = ["fs1", "fs2", "fs3", "fs4", "fs5"]
        mock_imp.side_effect = [{
            "WF1": "wf1.nf",
            "PROC1": "proc1.nf",
            "PROC2": "proc2.nf",
        }, {
            "PROC3": "proc3.nf",
        }, {}, {}, {}]
        mock_wf.side_effect = [[
            {"name": "", "body": "WF1()PROC1()"},
            {"name": "WF2", "body": "PROC2()"},
        ], [
            {"name": "WF3", "body": "PROC3()"},
        ], [], [], []]
        modules = get_module_paths("path", prefix="X:")
        self.assertEqual(modules, {
            "X:PROC1": str(Path("proc1.nf").resolve()),
            "X:WF2:PROC2": str(Path("proc2.nf").resolve()),
            "WF3:PROC3": str(Path("proc3.nf").resolve()),
        })
        for fs in ["fs1", "fs2", "fs3", "fs4", "fs5"]:
            mock_imp.assert_any_call(fs)
            mock_wf.assert_any_call(fs)
        mock_open.assert_any_call("path")
        mock_open.assert_any_call(Path("proc1.nf").resolve())
        mock_open.assert_any_call(Path("proc2.nf").resolve())
        mock_open.assert_any_call(Path("proc3.nf").resolve())
        mock_open.assert_any_call(Path("wf1.nf").resolve())