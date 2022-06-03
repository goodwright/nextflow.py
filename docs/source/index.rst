nextflow.py
===========

nextflow.py is a Python wrapper around the Nextflow pipeline framework. It lets
you run Nextflow pipelines from Python code.

Example
-------

   >>> import nextflow
   >>> execution = nextflow.run(path="main.nf", params={"param1": "123"})
   >>> print(execution.status)

Table of Contents
-----------------

.. toctree ::
   installing
   overview
   api
   changelog
