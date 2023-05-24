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

