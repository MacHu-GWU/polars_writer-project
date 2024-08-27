
.. .. image:: https://readthedocs.org/projects/polars-writer/badge/?version=latest
    :target: https://polars-writer.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/polars_writer-project/actions/workflows/main.yml/badge.svg
    :target: https://github.com/MacHu-GWU/polars_writer-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/polars_writer-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/polars_writer-project

.. image:: https://img.shields.io/pypi/v/polars-writer.svg
    :target: https://pypi.python.org/pypi/polars-writer

.. image:: https://img.shields.io/pypi/l/polars-writer.svg
    :target: https://pypi.python.org/pypi/polars-writer

.. image:: https://img.shields.io/pypi/pyversions/polars-writer.svg
    :target: https://pypi.python.org/pypi/polars-writer

.. image:: https://img.shields.io/badge/Release_History!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/polars_writer-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/polars_writer-project

------

.. .. image:: https://img.shields.io/badge/Link-Document-blue.svg
    :target: https://polars-writer.readthedocs.io/en/latest/

.. .. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://polars-writer.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/polars_writer-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/polars_writer-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/polars_writer-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/polars-writer#files


Welcome to ``polars_writer`` Documentation
==============================================================================
.. .. image:: https://polars-writer.readthedocs.io/en/latest/_static/polars_writer-logo.png
    :target: https://polars-writer.readthedocs.io/en/latest/

``polars_writer`` is a library that allows defining methods like polars.DataFrame.write_csv and polars.DataFrame.write_json using a pure JSON-friendly parameter format. The purpose of this library is to provide an interface independent of the Python language itself, allowing users to directly define the behavior of polars writers using JSON. This library is intended to be used in some end-user facing data products.

See `Usage example <https://github.com/MacHu-GWU/polars_writer-project/blob/main/tests/test_writer.py>`_


.. _install:

Install
------------------------------------------------------------------------------

``polars_writer`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install polars-writer

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade polars-writer
