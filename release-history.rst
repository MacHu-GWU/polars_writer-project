.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.3.2 (2024-09-01)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- Add the missing ``Writer.to_dict`` and ``Writer.from_dict`` methods.


0.3.1 (2024-08-27)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add support to get the method and kwargs to read and scan data from the writer.
- Now ``Writer.write`` method support write kwargs override.
- Add support to deltalake.
- Add the following public APIs:
    - ``polars_writer.api.Writer.to_read_method_and_kwargs``
    - ``polars_writer.api.Writer.to_read_kwargs``
    - ``polars_writer.api.Writer.read``
    - ``polars_writer.api.Writer.to_scan_method_and_kwargs``
    - ``polars_writer.api.Writer.to_scan_kwargs``
    - ``polars_writer.api.Writer.scan``


0.2.1 (2024-08-26)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add the following public APIs:
    - ``polars_writer.api.Writer.to_method_and_kwargs``
    - ``polars_writer.api.Writer.to_kwargs``
    - ``polars_writer.api.Writer.write``


0.1.1 (2024-08-22)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- First release
- Add the following public APIs:
    - ``polars_writer.api.Writer``
