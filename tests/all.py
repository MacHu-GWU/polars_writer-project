# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from polars_writer.tests import run_cov_test

    run_cov_test(__file__, "polars_writer", is_folder=True, preview=False)
