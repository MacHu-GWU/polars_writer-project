# -*- coding: utf-8 -*-

import pytest
import io
import shutil
from pathlib import Path
import polars as pl
from polars_writer.writer import Writer


dir_here = Path(__file__).absolute().parent
dir_tmp = dir_here / "tmp"
path_tmp = dir_here / "tmp.txt"
shutil.rmtree(dir_tmp, ignore_errors=True)
dir_tmp.mkdir(exist_ok=True)


class TestWriter:
    def test_construct(self):
        # test invalid arguments
        with pytest.raises(ValueError):
            writer = Writer(format="invalid")
        with pytest.raises(ValueError):
            writer = Writer(format="parquet", parquet_compression="invalid")
        with pytest.raises(ValueError):
            writer = Writer(format="delta", delta_mode="invalid")

        writer = Writer(format="csv")
        kwargs = writer.to_kwargs()
        assert "storage_options" not in kwargs
        assert "delta_write_options" not in kwargs
        assert "delta_merge_options" not in kwargs

        dct = writer.to_dict()
        assert "storage_options" not in dct
        assert writer == Writer.from_dict(dct)

    def test_write(self):
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "cathy"]})

        buffer = io.BytesIO()
        writer = Writer(format="csv")
        writer.write(df, file_args=[buffer])
        b = buffer.getvalue()
        df1 = writer.read(file_args=[b])
        assert df1.to_dicts() == df.to_dicts()
        path_tmp.write_bytes(b)
        df2 = writer.scan(file_args=[path_tmp]).collect()
        assert df2.to_dicts() == df.to_dicts()

        buffer = io.BytesIO()
        writer = Writer(format="json")
        writer.write(df, file_args=[buffer])
        b = buffer.getvalue()
        df1 = writer.read(file_args=[b])
        assert df1.to_dicts() == df.to_dicts()
        # polars doesn't support scan_json

        buffer = io.BytesIO()
        writer = Writer(format="ndjson")
        writer.write(df, file_args=[buffer])
        b = buffer.getvalue()
        df1 = writer.read(file_args=[b])
        assert df1.to_dicts() == df.to_dicts()
        path_tmp.write_bytes(b)
        df2 = writer.scan(file_args=[path_tmp]).collect()
        assert df2.to_dicts() == df.to_dicts()

        buffer = io.BytesIO()
        writer = Writer(format="parquet")
        writer.write(df, file_args=[buffer], write_kwargs={"compression_level": 9})
        b = buffer.getvalue()
        df1 = writer.read(file_args=[b], read_kwargs={"low_memory": True})
        assert df1.to_dicts() == df.to_dicts()
        path_tmp.write_bytes(b)
        df2 = writer.scan(
            file_args=[path_tmp], scan_kwargs={"low_memory": True}
        ).collect()
        assert df2.to_dicts() == df.to_dicts()

        Writer(format="parquet").to_method_and_kwargs()

        writer = Writer(format="delta", delta_mode="append")
        writer.write(df, file_args=[dir_tmp])
        df1 = writer.read(file_args=[str(dir_tmp)])
        assert df1.to_dicts() == df.to_dicts()
        df2 = writer.scan(file_args=[str(dir_tmp)]).collect()
        assert df2.to_dicts() == df.to_dicts()


if __name__ == "__main__":
    from polars_writer.tests import run_cov_test

    run_cov_test(__file__, "polars_writer.writer", preview=False)
