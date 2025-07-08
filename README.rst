nextflow.py
===========

|ci| |version| |pypi| |nextflow| |license|

.. |ci| image:: https://github.com/goodwright/nextflow.py/actions/workflows/main.yml/badge.svg
  :target: https://github.com/goodwright/nextflow.py/actions/workflows/main.yml

.. |version| image:: https://img.shields.io/pypi/v/nextflowpy.svg
  :target: https://pypi.org/project/nextflowpy/

.. |pypi| image:: https://img.shields.io/pypi/pyversions/nextflowpy.svg
  :target: https://pypi.org/project/nextflowpy/

.. |nextflow| image:: https://img.shields.io/badge/Nextflow-22.10%20|23.10%20|24.10%20-orange
  :target: https://www.nextflow.io/

.. |license| image:: https://img.shields.io/pypi/l/nextflowpy.svg?color=blue
  :target: https://github.com/goodwright/nextflow.py/blob/master/LICENSE

nextflow.py is a Python wrapper around the Nextflow pipeline framework. It lets
you run Nextflow pipelines from Python code.

Example
-------

   >>> import nextflow
   >>> execution = nextflow.run("main.nf", params={"param1": "123"})
   >>> print(execution.status)


Installing
----------

pip
~~~

nextflow.py can be installed using pip::

    $ pip install nextflowpy

If you get permission errors, try using ``sudo``::

    $ sudo pip install nextflowpy


Development
~~~~~~~~~~~

The repository for nextflow.py, containing the most recent iteration, can be
found `here <http://github.com/goodwright/nextflow.py/>`_. To clone the
nextflow.py repository directly from there, use::

    $ git clone git://github.com/goodwright/nextflow.py.git


Nextflow
~~~~~~~~

nextflow.py requires the Nextflow executable to be installed and in your PATH.
Instructions for installing Nextflow can be found at
`their website <https://www.nextflow.io/docs/latest/getstarted.html#installation/>`_.


Testing
~~~~~~~

To test a local version of nextflow.py, cd to the nextflow.py directory and run::

    $ python -m unittest discover tests

You can opt to only run unit tests or integration tests::

    $ python -m unittest discover tests.unit
    $ python -m unittest discover tests.integration
  
The `freezegun` library must be installed to run the unit tests:

    $ pip install freezegun

Overview
--------

Running
~~~~~~~

To run a pipeline, the ``run`` function is used. The only required
parameter is the path to the pipeline file:

    >>> pipeline = nextflow.run("pipelines/my-pipeline.nf")

This will return an ``Execution`` object, which represents the pipeline
execution that just took place (see below for details on this object). You can
customise the execution with various options:

    >>> execution = pipeline.run("my-pipeline.nf", run_path="./rundir", output_path="./outputs", params={"param1": "123"}, profiles=["docker", "test"], version="22.0.1", configs=["env.config"], timezone="UTC", report="report.html", timeline="timeline.html", dag="dag.html", trace="trace.txt")

* ``run_path`` - The location to run the pipeline from, which by default is just the current working directory.

* ``output_path`` - The location to store the execution outputs (``work`` etc.), which by default is the ``run_path``.

* ``params`` - A dictionary of parameters to pass to the pipeline as command. In the above example, this would run the pipeline with ``--param1=123``.

* ``profiles`` - A list of Nextflow profiles to use when running the pipeline. These are defined in the ``nextflow.config`` file, and can be used to configure things like the executor to use, or the container engine to use. In the above example, this would run the pipeline with ``-profile docker,test``.

* ``version`` - The version of Nextflow to use when running the pipeline. By default, the version of Nextflow installed on the system is used, but this can be overridden with this parameter.

* ``configs`` - A list of config files to use when running the pipeline. These are merged with the config files specified in the pipeline itself, and can be used to override any of the settings in the pipeline config.

* ``timezone`` - A timezone to pass to Nextflow - this determines the timestamps used in the log file.

* ``report`` - A filename for a report file to generate. This will be an HTML file containing information about the pipeline execution.

* ``timeline`` - A filename for a timeline file to generate. This will be an HTML file containing a timeline of the pipeline execution.

* ``dag`` - A filename for a DAG file to generate. This will be an HTML file containing a DAG diagram of the pipeline execution.

* ``trace`` - A filename for a trace file to generate. This will be an TSV file containing runtime information about the pipeline execution.

* ``resume`` - Whether to resume an existing execution. Can be ``True`` or the UUID of an existing execution to resume from.


Custom Runners
~~~~~~~~~~~~~~

When you run a pipeline with nextflow.py, it will generate the command string
that you would use at the command line if you were running the pipeline
manually. This will be some variant of ``nextflow run some-pipeline.nf``, and
will include any parameters, profiles, versions, and config files that you
passed in.

By default, nextflow.py will then run this command using the standard Python
``subprocess`` module. However, you can customise this behaviour by passing in
a custom 'runner' function. This is a function which takes the command string
and submits the job in some other way. For example, you could use a custom
runner to submit the job to a cluster, or to a cloud platform.

This runner function is passed to the ``run`` method as the
``runner`` parameter:

    >>> execution = pipeline.run("my-pipeline.nf", runner=my_custom_runner)

Once the run command string has been passed to the runner, nextflow.py will
wait for the pipeline to complete by watching the execution directory, and then
return the ``Execution`` object as normal.

Custom IO
~~~~~~~~~

By default, nextflow will try to access the local filesystem when checking the
output pipeline files. If you need to define alternative ways of doing this, you
can create a custom ``IO`` object and pass it in as the ``io`` parameter to the
``run`` method:

    >>> execution = pipeline.run("my-pipeline.nf", io=my_custom_io)

This object must define the following methods:

* ``abspath(path)`` - Return the absolute path to a file.
* ``listdir(path)`` - List the contents of a directory.
* ``read(path, mode="r")`` - Read the contents of a file.
* ``glob(path)`` - Glob a path.
* ``ctime(path)`` - Get the creation time of a file.

Polling
~~~~~~~

The function described above will run the pipeline and wait while it does, with
the completed ``Execution`` being returned only at the end.

An alternate method is to use ``run_and_poll``, which returns an
``Execution`` object every few seconds representing the state of the
pipeline execution at that moment in time, as a generator::

    for execution in pipeline.run_and_poll(sleep=2, run_path="./rundir", params={"param1": "123"}):
        print("Processing intermediate execution")

By default, an ``Execution`` will be returned every second, but you can
adjust this as required with the ``sleep`` parameter. This is useful if you want
to get information about the progress of the pipeline execution as it proceeds.

Executions
~~~~~~~~~~

An ``Execution`` represents a single execution of a pipeline. It has
properties for:

* ``identifier`` - The unique ID of that run, generated by Nextflow.

* ``uuid`` - The unique UUID of the session, generated by Nextflow.

* ``started`` - When the pipeline ran (as a Python datetime).

* ``finished`` - When the pipeline completed (as a Python datetime).

* ``duration`` - how long the pipeline ran for (if finished).

* ``status`` - the status Nextflow reports on completion.

* ``command`` - the command used to run the pipeline.

* ``stdout`` - the stdout of the execution process.

* ``stderr`` - the stderr of the execution process.

* ``log`` - the full text of the log file produced.

* ``return_code`` - the exit code of the run - usually 0 or 1.

* ``path`` - the path to the execution directory.

It also has a ``process_executions`` property, which is a list of
``ProcessExecution`` objects. Nextflow processes data by chaining
together isolated 'processes', and each of these has a
``ProcessExecution`` object representing its execution. These have the
following properties:

* ``identifier`` - The unique ID generated by Nextflow, of the form ``xx/xxxxxx``.

* ``process`` - The name of the process that spawned the process execution.

* ``name`` - The name of this specific process execution.

* ``status`` - the status Nextflow reports on completion.

* ``stdout`` - the stdout of the process execution.

* ``stderr`` - the stderr of the process execution.

* ``submitted`` - When the process execution was submitted (as a Python datetime).

* ``started`` - When the process execution started (as a Python datetime).

* ``finished`` - When the process execution completed (as a Python datetime).

* ``duration`` - how long the process execution took in seconds.

* ``return_code`` - the exit code of the process execution - usually 0 or 1.

* ``path`` - the local path to the process execution directory.

* ``full_path`` - the absolute path to the process execution directory.

* ``bash`` - the bash file contents generated for the process execution.

* ``cached`` - whether the process execution was cached.

Process executions can have various files passed to them, and will create files
during their execution too. These can be obtained as follows:

    >>> process_execution.input_data() # Full absolute paths
    >>> process_execution.input_data(include_path=False) # Just file names
    >>> process_execution.all_output_data() # Full absolute paths
    >>> process_execution.all_output_data(include_path=False) # Just file names

.. note::
   Nextflow makes a distinction between process output files which were
   'published' via some channel, and those which weren't. It is not possible to
   distinguish these once execution is complete, so nextflow.py reports all
   output files, not just those which are 'published'.

Changelog
---------

Release 0.12.0
~~~~~~~~~~~~~~

`8th July, 2025`

* Add separate `ExecutionSubmission` model for submission without watching.
* You can now specify a Java home for the Nextflow process.


Release 0.11.0
~~~~~~~~~~~~~~

`24th April, 2025`

* Added support for custom filesystem access.
* Fixed bug in timezone applying.


Release 0.10.0
~~~~~~~~~~~~~~

`20th March, 2025`

* Added support for execution resuming.
* You can now specify separate paths for the log file and work directory.


Release 0.9.0
~~~~~~~~~~~~~

`8th February, 2025`

* Process submission time now distinguished from start time.
* Added support for staging inputs by copy.
* Added utilities for predicting all process names for a pipeline.


Release 0.8.3
~~~~~~~~~~~~~

`2nd February, 2025`

* Trace reports can now be produced with the `trace` parameter.
* All non-zero exit codes are now treated as errors.


Release 0.8.2
~~~~~~~~~~~~~

`25th January, 2025`

* Optimise log parsing when polling executions.


Release 0.8.1
~~~~~~~~~~~~~

`14th November, 2023`

* Handle pure nextflow process statuses better.


Release 0.8.0
~~~~~~~~~~~~~

`5th September, 2023`

* You can use `output_path` to specify where the execution contents go.


Release 0.7.1
~~~~~~~~~~~~~

`22nd August, 2023`

* Fixed bug in handling empty param values.


Release 0.7.0
~~~~~~~~~~~~~

`22nd July, 2023`

* An execution report can now be published with the `report` parameter.
* A timeline report can now be published with the `timeline` parameter.
* A DAG report can now be published with the `dag` parameter.



Release 0.6.2
~~~~~~~~~~~~~

`21st July, 2023`

* Fixed issue in handling no path for process execution input data.


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
* Tests now check compatibility with different Nextflow versions.

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