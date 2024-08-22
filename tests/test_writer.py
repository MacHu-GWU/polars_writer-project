# -*- coding: utf-8 -*-

import pytest
from polars_writer.writer import Writer


class TestWriter:
    def test(self):
        with pytest.raises(ValueError):
            writer = Writer(format="invalid")
        with pytest.raises(ValueError):
            writer = Writer(format="parquet", parquet_compression="invalid")
        with pytest.raises(ValueError):
            writer = Writer(format="delta", delta_mode="invalid")

        Writer(format="csv").to_kwargs()
        Writer(format="json").to_kwargs()
        Writer(format="ndjson").to_kwargs()
        Writer(format="parquet").to_kwargs()
        Writer(format="delta").to_kwargs()


if __name__ == "__main__":
    from polars_writer.tests import run_cov_test

    run_cov_test(__file__, "polars_writer.writer", preview=False)
