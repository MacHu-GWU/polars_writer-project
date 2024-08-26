# -*- coding: utf-8 -*-

import pytest
import io
import polars as pl
from polars_writer.writer import Writer


class TestWriter:
    def test(self):
        with pytest.raises(ValueError):
            writer = Writer(format="invalid")
        with pytest.raises(ValueError):
            writer = Writer(format="parquet", parquet_compression="invalid")
        with pytest.raises(ValueError):
            writer = Writer(format="delta", delta_mode="invalid")

        df = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "cathy"]})

        buffer = io.BytesIO()
        writer = Writer(format="csv")
        writer.write(df, file_args=[buffer])
        df1 = pl.read_csv(buffer.getvalue())
        assert df1.to_dicts() == df.to_dicts()

        buffer = io.BytesIO()
        writer = Writer(format="json")
        writer.write(df, file_args=[buffer])
        df1 = pl.read_json(buffer.getvalue())
        assert df1.to_dicts() == df.to_dicts()

        buffer = io.BytesIO()
        writer = Writer(format="ndjson")
        writer.write(df, file_args=[buffer])
        df1 = pl.read_ndjson(buffer.getvalue())
        assert df1.to_dicts() == df.to_dicts()

        buffer = io.BytesIO()
        writer = Writer(format="parquet")
        writer.write(df, file_args=[buffer])
        df1 = pl.read_parquet(buffer.getvalue())
        assert df1.to_dicts() == df.to_dicts()

        Writer(format="parquet").to_method_and_kwargs()


if __name__ == "__main__":
    from polars_writer.tests import run_cov_test

    run_cov_test(__file__, "polars_writer.writer", preview=False)
