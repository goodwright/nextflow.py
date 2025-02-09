import os
import re
import shutil
from datetime import datetime
from unittest import TestCase

class RunTestCase(TestCase):

    def setUp(self):
        self.rundirectory = self.get_path("rundirectory")
        if os.path.exists(self.rundirectory): shutil.rmtree(self.rundirectory)
        os.mkdir(self.rundirectory)
        self.current_directory = os.getcwd()
    

    def tearDown(self):
        shutil.rmtree(self.rundirectory)
        if os.path.exists(".nextflow"): shutil.rmtree(".nextflow")
        os.chdir(self.current_directory)


    def get_path(self, name):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(
            dir_path, "pipelines", name.replace("/", os.path.sep)
        )


    def check_running_execution(self, execution, last_stdout, output_path=None):
        if not output_path: self.assertIn(".nextflow", os.listdir(self.get_path("rundirectory")))
        self.assertIn(".nextflow.log", os.listdir(output_path or self.get_path("rundirectory")))
        self.assertGreaterEqual(len(execution.stdout), len(last_stdout))
        return execution.stdout
    

    def check_execution(self, execution, line_count=24, output_path=None, version=None, timezone=None, report=None, timeline=None, dag=None, trace=None, check_stderr=True):
        # Files created
        if not output_path: self.assertIn(".nextflow", os.listdir(self.get_path("rundirectory")))
        self.assertIn(".nextflow.log", os.listdir(output_path or self.get_path("rundirectory")))

        # Execution is correct
        self.assertTrue(re.match(r"[a-z]+_[a-z]+", execution.identifier))
        self.assertIn("N E X T F L O W", execution.stdout)
        if check_stderr: self.assertTrue(not execution.stderr or (
            execution.stderr.count("\n") == 1 and "is available" in execution.stderr
        ))
        self.assertEqual(execution.return_code, "0")
        if not timezone:
            self.assertLessEqual((datetime.now() - execution.started).seconds, 100)
            self.assertLessEqual((datetime.now() - execution.finished).seconds, 100)
        self.assertGreater(execution.finished, execution.started)
        if version:
            self.assertTrue(execution.command.startswith(f"NXF_ANSI_LOG=false NXF_VER={version} nextflow -Duser.country=US"))
        elif timezone:
            self.assertTrue(execution.command.startswith(f"NXF_ANSI_LOG=false TZ={timezone} nextflow -Duser.country=US"))
        elif output_path:
            self.assertTrue(execution.command.startswith(f"NXF_ANSI_LOG=false NXF_WORK={output_path}/work nextflow -Duser.country=US"))
        else:
            self.assertTrue(execution.command.startswith("NXF_ANSI_LOG=false nextflow -Duser.country=US"))
        self.assertIn("Starting process", execution.log)
        self.assertIn("Execution complete -- Goodbye", execution.log)
        self.assertEqual(execution.path, output_path or self.get_path("rundirectory"))
        self.assertEqual(len(execution.process_executions), 8)
        self.assertLessEqual(execution.duration.seconds, 50)
        self.assertEqual(execution.status, "OK")

        # Reports
        if report:
            self.assertIn(report, os.listdir(self.get_path("rundirectory")))
            with open(os.path.join(self.get_path("rundirectory"), report)) as f:
                self.assertIn("Nextflow Workflow Report", f.read())
        else:
            self.assertNotIn(report, os.listdir(self.get_path("rundirectory")))
        if timeline:
            self.assertIn(timeline, os.listdir(self.get_path("rundirectory")))
            with open(os.path.join(self.get_path("rundirectory"), timeline)) as f:
                self.assertIn("<h3>Processes execution timeline</h3>", f.read())
        else:
            self.assertNotIn(timeline, os.listdir(self.get_path("rundirectory")))
        if dag:
            self.assertIn(dag, os.listdir(self.get_path("rundirectory")))
            with open(os.path.join(self.get_path("rundirectory"), dag)) as f:
                html = f.read()
                self.assertTrue("Cytoscape.js with Dagre" in html or "import mermaid" in html)
        else:
            self.assertNotIn(dag, os.listdir(self.get_path("rundirectory")))
        if trace:
            self.assertIn(trace, os.listdir(self.get_path("rundirectory")))
            with open(os.path.join(self.get_path("rundirectory"), trace)) as f:
                self.assertIn("peak_rss", f.read())
        else:
            self.assertNotIn(trace, os.listdir(self.get_path("rundirectory")))

        # Process executions are fine
        proc_ex = self.get_process_execution(execution, "SPLIT_FILE")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "Splitting...\n")
        self.assertEqual(proc_ex.stderr, "")
        self.assertTrue(proc_ex.bash.startswith, "#!/usr/bin/env")
        self.assertEqual(proc_ex.process, "SPLIT_FILE")
        self.assertEqual(proc_ex.input_data(), [self.get_path("files/data.txt")])
        self.assertEqual(proc_ex.input_data(include_path=False), ["data.txt"])
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)),
            {"abc.dat", "xyz.dat", "log.txt"}
        )
        self.assertIn(proc_ex.identifier, proc_ex.all_output_data()[0])

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE")
        self.assertEqual(proc_ex.input_data(include_path=False), ["abc.dat"])
        self.assertIn(
            self.get_process_execution(execution, "SPLIT_FILE").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"duplicated_abc.dat"}
        )
        with open(proc_ex.all_output_data(include_path=True)[0]) as f:
            self.assertEqual(len(f.read().splitlines()), line_count)

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE")
        self.assertEqual(proc_ex.input_data(include_path=False), ["xyz.dat"])
        self.assertIn(
            self.get_process_execution(execution, "SPLIT_FILE").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"duplicated_xyz.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_abc.dat)")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER")
        self.assertEqual(proc_ex.input_data(include_path=False), ["duplicated_abc.dat"])
        self.assertIn(
            self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (abc.dat)").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"lowered_duplicated_abc.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER (duplicated_xyz.dat)")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:DUPLICATE_AND_LOWER:LOWER")
        self.assertEqual(proc_ex.input_data(include_path=False), ["duplicated_xyz.dat"])
        self.assertIn(
            self.get_process_execution(execution, "PROCESS_DATA:DUPLICATE_AND_LOWER:DUPLICATE (xyz.dat)").identifier,
            proc_ex.input_data()[0]
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"lowered_duplicated_xyz.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:APPEND (lowered_duplicated_abc.dat)")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:APPEND")
        self.assertEqual(set(proc_ex.input_data(include_path=False)), {"lowered_duplicated_abc.dat", "suffix.txt"})
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"suffix_lowered_duplicated_abc.dat"}
        )

        proc_ex = self.get_process_execution(execution, "PROCESS_DATA:APPEND (lowered_duplicated_xyz.dat)")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "PROCESS_DATA:APPEND")
        self.assertEqual(set(proc_ex.input_data(include_path=False)), {"lowered_duplicated_xyz.dat", "suffix.txt"})
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"suffix_lowered_duplicated_xyz.dat"}
        )

        proc_ex = self.get_process_execution(execution, "JOIN:COMBINE_FILES")
        self.check_process_execution(proc_ex, execution, False, check_time=not timezone)
        self.assertEqual(proc_ex.stdout, "")
        self.assertEqual(proc_ex.stderr, "")
        self.assertEqual(proc_ex.process, "JOIN:COMBINE_FILES")
        self.assertEqual(
            set(proc_ex.input_data(include_path=False)),
            {"suffix_lowered_duplicated_abc.dat", "suffix_lowered_duplicated_xyz.dat"}
        )
        self.assertEqual(
            set(proc_ex.all_output_data(include_path=False)), {"combined.txt"}
        )


    def get_process_execution(self, execution, name):
        return [e for e in execution.process_executions if e.name == name][0]


    def check_process_execution(self, process_execution, execution, long, check_time=True):
        self.assertEqual(process_execution.submitted.year, datetime.now().year)
        self.assertEqual(process_execution.started.year, datetime.now().year)
        if check_time:
            self.assertLessEqual((datetime.now() - execution.started).seconds, 450 if long else 100)
            self.assertLessEqual((datetime.now() - execution.finished).seconds, 450 if long else 100)
        self.assertEqual(process_execution.return_code, "0")
        self.assertEqual(process_execution.status, "COMPLETED")
        self.assertIs(process_execution.execution, execution)
        self.assertGreaterEqual(process_execution.duration.seconds, 0)
        self.assertLessEqual(process_execution.duration.seconds, 60)

