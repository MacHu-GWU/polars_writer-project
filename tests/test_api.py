# -*- coding: utf-8 -*-

from polars_writer import api


def test():
    _ = api
    _ = api.Writer


if __name__ == "__main__":
    from polars_writer.tests import run_cov_test

    run_cov_test(__file__, "polars_writer.api", preview=False)
