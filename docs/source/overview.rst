Overview
--------

Running
~~~~~~~

To run a pipeline, the :py:func:`.run` function is used. The only required
parameter is the path to the pipeline file:

    >>> pipeline = nextflow.run("pipelines/my-pipeline.nf")

This will return an :py:class:`.Execution` object, which represents the pipeline
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

This runner function is passed to the :py:func:`.run` method as the
``runner`` parameter:

    >>> execution = pipeline.run("my-pipeline.nf", runner=my_custom_runner)

Once the run command string has been passed to the runner, nextflow.py will
wait for the pipeline to complete by watching the execution directory, and then
return the :py:class:`.Execution` object as normal.

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
the completed :py:class:`.Execution` being returned only at the end.

An alternate method is to use :py:func:`.run_and_poll`, which returns an
:py:class:`.Execution` object every few seconds representing the state of the
pipeline execution at that moment in time, as a generator::

    for execution in pipeline.run_and_poll(sleep=2, run_path="./rundir", params={"param1": "123"}):
        print("Processing intermediate execution")

By default, an :py:class:`.Execution` will be returned every second, but you can
adjust this as required with the ``sleep`` parameter. This is useful if you want
to get information about the progress of the pipeline execution as it proceeds.

Executions
~~~~~~~~~~

An :py:class:`.Execution` represents a single execution of a pipeline. It has
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
:py:class:`.ProcessExecution` objects. Nextflow processes data by chaining
together isolated 'processes', and each of these has a
:py:class:`.ProcessExecution` object representing its execution. These have the
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