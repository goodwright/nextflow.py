Changelog
---------

Release 0.6.1
~~~~~~~~~~~~~

`7th July, 2023`

* Added option to specify timezone to Nextflow.


Release 0.6.0
~~~~~~~~~~~~~

`24th May, 2023`

* Added ability to use custom runners for starting jobs.
* Removed pipeline class to.
* Overhauled architecture.


Release 0.5.0
~~~~~~~~~~~~~

`28th October, 2022`

* Little c (`-c`) is now used instead of big C (`-C`) for passing config.
* You can now pass multiple config files during pipeline execution.


Release 0.4.2
~~~~~~~~~~~~~

`26th September, 2022`

* Added `bash` attribute to process executions.


Release 0.4.1
~~~~~~~~~~~~~

`11th September, 2022`

* Fixed issue in execution polling where previous execution interferes initially.
* Execution parsing now checks directory is fully ready for parsing.
* Fixed issue where logs are unparseable in certain locales.


Release 0.4.0
~~~~~~~~~~~~~

`13th July, 2022`

* Process executions now report their input files as paths.
* Process executions now report all their output files as paths.
* Executions now have properties for their originating pipeline.
* Removed schema functionality.


Release 0.3.1
~~~~~~~~~~~~~

`15th June, 2022`

* Process polling now accesses stdout and stderr while process is ongoing.


Release 0.3
~~~~~~~~~~~

`4th June, 2022`

* Allow module-level run methods for directly running pipelines.
* Allow for running pipelines with different Nextflow versions.
* Improved datetime parsing.
* Simplified process execution parsing.
* Fixed concatenation of process executions with no parentheses.
* Tests now check compatability with different Nextflow versions.

Release 0.2.2
~~~~~~~~~~~~~

`21st March, 2022`

* Log outputs now have ANSI codes removed.

Release 0.2.1
~~~~~~~~~~~~~

`19th February, 2022`

* Execution polling now handles unready execution directory.
* Better detection of failed process executions mid execution.


Release 0.2
~~~~~~~~~~~

`14th February, 2022`

* Added method for running while continuously polling pipeline execution.
* Optimised process execution object creation from file state.

Release 0.1.4
~~~~~~~~~~~~~

`12th January, 2022`

* Pipeline command generation no longer applies quotes if there are already quotes.


Release 0.1.3
~~~~~~~~~~~~~

`24th November, 2021`

* Fixed Windows file separator issues.
* Renamed NextflowProcess -> ProcessExecution.

Release 0.1.2
~~~~~~~~~~~~~

`3rd November, 2021`

* Better handling of missing Nextflow executable.

Release 0.1.1
~~~~~~~~~~~~~

`29th October, 2021`

* Renamed `nextflow_processes` to `process_executions`.
* Added quotes around paths to handle spaces in paths.

Release 0.1
~~~~~~~~~~~~~

`18th October, 2021`

* Basic Pipeline object.
* Basic Execution object.
* Basic ProcessExecution object.