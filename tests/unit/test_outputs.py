from unittest import TestCase
from unittest.mock import patch, call
from nextflow.outputs import *

class PipelineProcessNamesTests(TestCase):

    @patch("nextflow.outputs.get_file_process_paths")
    def test_can_get_pipeline_process_names(self, mock_paths):
        mock_paths.return_value = {
            "module1": {
                "path": "/ex/module1.nf"
            },
            "module2": {
                "path": "/ex/module2.nf"
            }
        }
        outputs = get_pipeline_process_names("/ex/file.nf")
        self.assertEqual(outputs, ["module1", "module2"])
        mock_paths.assert_called_with("/ex/file.nf")



class FileProcessPathsTests(TestCase):

    @patch("nextflow.outputs.get_file_text")
    @patch("nextflow.outputs.get_import_names_to_paths")
    def test_workflow_with_no_subworkflows(self, mock_imports, mock_open):
        filestring = (
            "include { MOD2   } from '../modules/python/check/main'\n"
            "\n"
            "workflow MYSUB {\n"
            "MOD1()\n"
            "MOD2()\n"
            "}\n"
            "\n"
            "workflow {\n"
            "MOD2()\n"
            "MOD3()\n"
            "}\n"
        )
        mock_open.side_effect = (filestring, "mod-1", "mod-2", "mod-3")
        mock_imports.side_effect = [{
            "MOD1": "/ex/modules/python/mod1/main.nf",
            "MOD2": "/ex/modules/python/mod2/main.nf",
            "MOD3": "/ex/modules/python/mod3/main.nf",
        }, {}, {}, {}]
        paths = get_file_process_paths("/ex/file.nf")
        self.assertEqual(paths, {
            "MYSUB:MOD1": {"path": "/ex/modules/python/mod1/main.nf", "has_workflow_name": True},
            "MYSUB:MOD2": {"path": "/ex/modules/python/mod2/main.nf", "has_workflow_name": True},
            "MOD2": {"path": "/ex/modules/python/mod2/main.nf", "has_workflow_name": False},
            "MOD3": {"path": "/ex/modules/python/mod3/main.nf", "has_workflow_name": False}
        })
        self.assertEqual(mock_open.call_args_list, [
            call("/ex/file.nf"),
            call("/ex/modules/python/mod1/main.nf"),
            call("/ex/modules/python/mod2/main.nf"),
            call("/ex/modules/python/mod3/main.nf"),
        ])
        self.assertEqual(mock_imports.call_args_list, [
            call(filestring, "/ex/file.nf"),
            call("mod-1", "/ex/modules/python/mod1/main.nf"),
            call("mod-2", "/ex/modules/python/mod2/main.nf"),
            call("mod-3", "/ex/modules/python/mod3/main.nf"),
        ])
    

    @patch("nextflow.outputs.get_file_text")
    @patch("nextflow.outputs.get_import_names_to_paths")
    def test_workflow_with_no_workflows(self, mock_imports, mock_open):
        filestring1 = (
            "include {\n" 
            "  WORK2\n"
            "   } from '../modules/python/check/main'\n"
            "\n"
            " workflow MYSUB   {\n"
            "MOD1()\n"
            "WORK2()\n"
            "MOD2()\n"
            "}\n"
            "\n"
            "workflow {\n"
            "MOD2()\n"
            "WORK1()\n"
            "MOD3()\n"
            "}\n"
        )
        filestring2 = (
            "  workflow   MYSUB2{\n"
            "MOD4()\n"
            "MOD1()\n"
            "}\n"
            "\n"
            "workflow{\n"
            "MOD5()\n"
            "MYSUB2()\n"
            "}\n"
        )
        filestring3 = (
            "     workflow{\n"
            "MOD5()\n"
            "MOD2()\n"
            "}\n"
        )
        mock_open.side_effect = (
            filestring1, "mod-1", filestring2, "mod-4", "mod-5", "mod-2", "mod-3", filestring3, "mod-5", "mod-2"
        )
        mock_imports.side_effect = [{
            "MOD1": "/ex/modules/python/mod1/main.nf",
            "WORK1": "/ex/workflows/python/work1/main.nf",
            "MOD2": "/ex/modules/python/mod2/main.nf",
            "MOD3": "/ex/modules/python/mod3/main.nf",
            "WORK2": "/ex/workflows/python/work2/main.nf",
        }, {}, {
            "MOD4": "/ex/modules/python/mod4/main.nf",
            "MOD5": "/ex/modules/python/mod5/main.nf",
        }, {}, {}, {}, {}, {
            "MOD5": "/ex/modules/python/mod5/main.nf",
            "MOD2": "/ex/modules/python/mod2/main.nf",
        }, {}, {}]
        paths = get_file_process_paths("/ex/file.nf")
        self.assertEqual(paths, {
            "MYSUB:MOD1": {"path": "/ex/modules/python/mod1/main.nf", "has_workflow_name": True},
            "MYSUB:WORK2:MOD5": {"path": "/ex/modules/python/mod5/main.nf", "has_workflow_name": True},
            "MYSUB:WORK2:MOD2": {"path": "/ex/modules/python/mod2/main.nf", "has_workflow_name": True},
            "MYSUB:MOD2": {"path": "/ex/modules/python/mod2/main.nf", "has_workflow_name": True},
            "MOD2": {"path": "/ex/modules/python/mod2/main.nf", "has_workflow_name": False},
            "WORK1:MOD4": {"path": "/ex/modules/python/mod4/main.nf", "has_workflow_name": False},
            "WORK1:MOD5": {"path": "/ex/modules/python/mod5/main.nf", "has_workflow_name": False},
            "MOD3": {"path": "/ex/modules/python/mod3/main.nf", "has_workflow_name": False}
        })
        self.assertEqual(mock_open.call_args_list, [
            call("/ex/file.nf"),
            call("/ex/modules/python/mod1/main.nf"),
            call("/ex/workflows/python/work1/main.nf"),
            call("/ex/modules/python/mod4/main.nf"),
            call("/ex/modules/python/mod5/main.nf"),
            call("/ex/modules/python/mod2/main.nf"),
            call("/ex/modules/python/mod3/main.nf"),
            call("/ex/workflows/python/work2/main.nf"),
            call("/ex/modules/python/mod5/main.nf"),
            call("/ex/modules/python/mod2/main.nf"),
        ])
        self.assertEqual(mock_imports.call_args_list, [
            call(filestring1, "/ex/file.nf"),
            call("mod-1", "/ex/modules/python/mod1/main.nf"),
            call(filestring2, "/ex/workflows/python/work1/main.nf"),
            call("mod-4", "/ex/modules/python/mod4/main.nf"),
            call("mod-5", "/ex/modules/python/mod5/main.nf"),
            call("mod-2", "/ex/modules/python/mod2/main.nf"),
            call("mod-3", "/ex/modules/python/mod3/main.nf"),
            call(filestring3, "/ex/workflows/python/work2/main.nf"),
            call("mod-5", "/ex/modules/python/mod5/main.nf"),
            call("mod-2", "/ex/modules/python/mod2/main.nf"),
        ])




class ImportNamesToPathsTests(TestCase):

    def test_can_get_import_names_to_paths(self):
        filestring = (
            "include { CHECK_SAMPLESHEET   } from '../../modules/python/check_samplesheet/main'\n"
            '   include {MERGE_DETERMINANTS} from    "../../modules/python/merge_determinants/main"\n'
            "include { \n"
            "ALIGN AS STAR_ALIGN \n"
            "} from '../../modules/python/align/main'\n"
            "include {\n"
            "  BAM_SORT_STATS_SAMTOOLS\n"
            "} from '../../modules/python/build/main'\n"
            "\n"
            "include NAME1 from 'path/to/file1'\n"
            "include {NAME2} from path/to/file2\n"
        )
        lookup = get_import_names_to_paths(filestring, "/ex/file.nf")
        self.assertEqual(lookup, {
            "CHECK_SAMPLESHEET": "/ex/../../modules/python/check_samplesheet/main.nf",
            "STAR_ALIGN": "/ex/../../modules/python/align/main.nf",
            "MERGE_DETERMINANTS": "/ex/../../modules/python/merge_determinants/main.nf",
            "BAM_SORT_STATS_SAMTOOLS": "/ex/../../modules/python/build/main.nf",
        })