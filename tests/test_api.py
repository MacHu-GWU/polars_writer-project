# -*- coding: utf-8 -*-

from polars_writer import api


def test():
    _ = api
    _ = api.Writer
    _ = api.Writer.to_method_and_kwargs
    _ = api.Writer.to_kwargs
    _ = api.Writer.write
    _ = api.Writer.to_read_method_and_kwargs
    _ = api.Writer.to_read_kwargs
    _ = api.Writer.read
    _ = api.Writer.to_scan_method_and_kwargs
    _ = api.Writer.to_scan_kwargs
    _ = api.Writer.scan


if __name__ == "__main__":
    from polars_writer.tests import run_cov_test

    run_cov_test(__file__, "polars_writer.api", preview=False)
