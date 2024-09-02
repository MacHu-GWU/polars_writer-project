# -*- coding: utf-8 -*-

"""
Polars DataFrame Writer Module

This module provides functionality for writing Polars DataFrames to various file formats.
It includes a Writer class and several enums to support different output formats and options.

Classes:

- :class:`FormatEnum`
- :class:`WriteMethodEnum`
- :class:`ParquetCompressionEnum`
- :class:`DeltaModeEnum`
- :class:`Writer`: Main class for configuring and executing write operations.
"""

import typing as T
import enum
import dataclasses

import polars as pl
from func_args import NOTHING, resolve_kwargs

if T.TYPE_CHECKING:  # pragma: no cover
    import polars as pl


class FormatEnum(str, enum.Enum):
    """
    Enumeration of supported file formats for writing Polars DataFrames.
    """

    csv = "csv"
    json = "json"
    ndjson = "ndjson"
    parquet = "parquet"
    delta = "delta"


class WriteMethodEnum(str, enum.Enum):
    """
    Enumeration of corresponding write methods in Polars for each supported format.
    """

    write_csv = "write_csv"
    write_json = "write_json"
    write_ndjson = "write_ndjson"
    write_parquet = "write_parquet"
    write_delta = "write_delta"


class ReadMethodEnum(str, enum.Enum):
    """
    Enumeration of corresponding read methods in Polars for each supported format.
    """

    read_csv = "read_csv"
    read_json = "read_json"
    read_ndjson = "read_ndjson"
    read_parquet = "read_parquet"
    read_delta = "read_delta"


class ScanMethodEnum(str, enum.Enum):
    """
    Enumeration of corresponding scan methods in Polars for each supported format.
    """

    scan_csv = "scan_csv"
    scan_json = "scan_json"
    scan_ndjson = "scan_ndjson"
    scan_parquet = "scan_parquet"
    scan_delta = "scan_delta"


class ParquetCompressionEnum(str, enum.Enum):
    """
    Enumeration of supported compression algorithms for Parquet files.
    """

    lz4 = "lz4"
    uncompressed = "uncompressed"
    snappy = "snappy"
    gzip = "gzip"
    lzo = "lzo"
    brotli = "brotli"
    zstd = "zstd"


class DeltaModeEnum(str, enum.Enum):
    """
    Enumeration of write modes for Delta Lake operations.
    """

    error = "error"
    append = "append"
    overwrite = "overwrite"
    ignore = "ignore"
    merge = "merge"


@dataclasses.dataclass
class Writer:
    """
    Writer class for configuring and executing write operations on Polars DataFrames.

    This class supports writing DataFrames to various file formats with customizable options.
    """

    # fmt: off
    format: str = dataclasses.field()
    # common
    storage_options: T.Dict[str, T.Any] = dataclasses.field(default=NOTHING)
    # csv
    csv_include_header: bool = dataclasses.field(default=NOTHING)
    csv_delimiter: str = dataclasses.field(default=NOTHING)
    csv_line_terminator: str = dataclasses.field(default=NOTHING)
    csv_quote_char: str = dataclasses.field(default=NOTHING)
    csv_datetime_format: str = dataclasses.field(default=NOTHING)
    csv_date_format: str = dataclasses.field(default=NOTHING)
    csv_float_scientific: bool = dataclasses.field(default=NOTHING)
    csv_float_precision: int = dataclasses.field(default=NOTHING)
    csv_null_value: str = dataclasses.field(default=NOTHING)
    csv_quote_style: str = dataclasses.field(default=NOTHING)
    # json
    # ndjson
    # parquet
    parquet_compression: str = dataclasses.field(default=NOTHING)
    parquet_compression_level: int = dataclasses.field(default=NOTHING)
    parquet_statistics: T.Union[bool, str, T.Dict[str, bool]] = dataclasses.field(default=NOTHING)
    parquet_row_group_size: T.Optional[int] = dataclasses.field(default=NOTHING)
    parquet_data_page_size: T.Optional[int] = dataclasses.field(default=NOTHING)
    parquet_use_pyarrow: bool = dataclasses.field(default=NOTHING)
    parquet_pyarrow_options: T.Optional[T.Dict[str, T.Any]] = dataclasses.field(default=NOTHING)
    parquet_partition_by: T.Optional[T.Union[str, T.Sequence[str]]] = dataclasses.field(default=NOTHING)
    parquet_partition_chunk_size_bytes: int = dataclasses.field(default=NOTHING)
    # delta
    delta_mode: str = dataclasses.field(default=NOTHING)
    delta_overwrite_schema: bool = dataclasses.field(default=NOTHING)
    delta_write_options: T.Dict[str, T.Any] = dataclasses.field(default=NOTHING)
    delta_merge_options: T.Dict[str, T.Any] = dataclasses.field(default=NOTHING)
    # fmt: on

    def __post_init__(self):
        if self.format is not NOTHING:
            try:
                FormatEnum[self.format]
            except KeyError:
                raise ValueError(f"Invalid format: {self.format}")
        if self.parquet_compression is not NOTHING:
            try:
                ParquetCompressionEnum[self.parquet_compression]
            except KeyError:
                raise ValueError(
                    f"Invalid parquet_compression: {self.parquet_compression}"
                )
        if self.delta_mode is not NOTHING:
            try:
                DeltaModeEnum[self.delta_mode]
            except KeyError:
                raise ValueError(f"Invalid delta_mode: {self.delta_mode}")

    @classmethod
    def from_dict(cls, dct: T.Dict[str, T.Any]):
        return cls(**dct)

    def to_dict(self) -> T.Dict[str, T.Any]:
        return resolve_kwargs(
            format=self.format,
            storage_options=self.storage_options,
            csv_include_header=self.csv_include_header,
            csv_delimiter=self.csv_delimiter,
            csv_line_terminator=self.csv_line_terminator,
            csv_quote_char=self.csv_quote_char,
            csv_datetime_format=self.csv_datetime_format,
            csv_date_format=self.csv_date_format,
            csv_float_scientific=self.csv_float_scientific,
            csv_float_precision=self.csv_float_precision,
            csv_null_value=self.csv_null_value,
            csv_quote_style=self.csv_quote_style,
            parquet_compression=self.parquet_compression,
            parquet_compression_level=self.parquet_compression_level,
            parquet_statistics=self.parquet_statistics,
            parquet_row_group_size=self.parquet_row_group_size,
            parquet_data_page_size=self.parquet_data_page_size,
            parquet_use_pyarrow=self.parquet_use_pyarrow,
            parquet_pyarrow_options=self.parquet_pyarrow_options,
            parquet_partition_by=self.parquet_partition_by,
            parquet_partition_chunk_size_bytes=self.parquet_partition_chunk_size_bytes,
            delta_mode=self.delta_mode,
            delta_overwrite_schema=self.delta_overwrite_schema,
            delta_write_options=self.delta_write_options,
            delta_merge_options=self.delta_merge_options,
        )

    def is_csv(self) -> bool:
        return self.format == FormatEnum.csv.value

    def is_json(self) -> bool:
        return self.format == FormatEnum.json.value

    def is_ndjson(self) -> bool:
        return self.format == FormatEnum.ndjson.value

    def is_parquet(self) -> bool:
        return self.format == FormatEnum.parquet.value

    def is_delta(self) -> bool:
        return self.format == FormatEnum.delta.value

    def to_method_and_kwargs(self) -> T.Tuple[str, T.Dict[str, T.Any]]:
        """
        Get the appropriate write method and keyword arguments for the chosen format.

        :return: A tuple containing the write method name and a dictionary of keyword arguments.
        """
        if self.is_csv():
            return (
                WriteMethodEnum.write_csv.value,
                resolve_kwargs(
                    include_header=self.csv_include_header,
                    separator=self.csv_delimiter,
                    line_terminator=self.csv_line_terminator,
                    quote_char=self.csv_quote_char,
                    datetime_format=self.csv_datetime_format,
                    date_format=self.csv_date_format,
                    float_scientific=self.csv_float_scientific,
                    float_precision=self.csv_float_precision,
                    null_value=self.csv_null_value,
                    quote_style=self.csv_quote_style,
                ),
            )
        elif self.is_json():
            return (WriteMethodEnum.write_json.value, dict())
        elif self.is_ndjson():
            return (WriteMethodEnum.write_ndjson.value, dict())
        elif self.is_parquet():
            return (
                WriteMethodEnum.write_parquet,
                resolve_kwargs(
                    compression=self.parquet_compression,
                    compression_level=self.parquet_compression_level,
                ),
            )
        elif self.is_delta():
            return (
                WriteMethodEnum.write_delta,
                resolve_kwargs(
                    mode=self.delta_mode,
                    overwrite_schema=self.delta_overwrite_schema,
                    delta_write_options=self.delta_write_options,
                    delta_merge_options=self.delta_merge_options,
                    storage_options=self.storage_options,
                ),
            )
        else:  # pragma: no cover
            raise NotImplementedError

    def to_kwargs(self) -> T.Dict[str, T.Any]:  # pragma: no cover
        """
        Get the keyword arguments for the write operation.

        A dictionary of keyword arguments for the write method.
        """
        method, kwargs = self.to_method_and_kwargs()
        return kwargs

    def write(
        self,
        df: "pl.DataFrame",
        file_args: T.List[T.Any],
        write_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
    ):
        """
        Write the given Polars DataFrame to the specified output.

        :param df: The Polars DataFrame to write.
        :param file_args: Arguments for the file path or location.
        :param write_kwargs: Optional keyword arguments for the write method.

        :return: The result of the write operation (format-dependent).
        """
        method, kwargs = self.to_method_and_kwargs()
        write_method = getattr(df, method)
        if write_kwargs is not None:  # override default kwargs
            kwargs.update(write_kwargs)
        # print(f"{file_args = }")
        # print("kwargs: ")
        # for k, v in kwargs.items():
        #     print(f"  {k} = {v}")
        return write_method(*file_args, **kwargs)

    def to_read_method_and_kwargs(self) -> T.Tuple[str, T.Dict[str, T.Any]]:
        """
        Get the appropriate read method and keyword arguments for the chosen format.

        :return: A tuple containing the read method name and a dictionary of keyword arguments.
        """
        if self.is_csv():
            return (
                ReadMethodEnum.read_csv.value,
                resolve_kwargs(
                    has_header=self.csv_include_header,
                    separator=self.csv_delimiter,
                    eol_char=self.csv_line_terminator,
                    quote_char=self.csv_quote_char,
                    storage_options=self.storage_options,
                ),
            )
        elif self.is_json():
            return (ReadMethodEnum.read_json.value, dict())
        elif self.is_ndjson():
            return (ReadMethodEnum.read_ndjson.value, dict())
        elif self.is_parquet():
            return (
                ReadMethodEnum.read_parquet,
                resolve_kwargs(
                    use_pyarrow=self.parquet_use_pyarrow,
                    storage_options=self.storage_options,
                ),
            )
        elif self.is_delta():
            return (
                ReadMethodEnum.read_delta,
                resolve_kwargs(
                    storage_options=self.storage_options,
                ),
            )
        else:  # pragma: no cover
            raise NotImplementedError

    def to_read_kwargs(self) -> T.Dict[str, T.Any]:  # pragma: no cover
        """
        Get the appropriate read method and keyword arguments for the chosen format.

        :return: A tuple containing the read method name and a dictionary of keyword arguments.
        """
        method, kwargs = self.to_read_method_and_kwargs()
        return kwargs

    def read(
        self,
        file_args: T.List[T.Any],
        read_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
    ) -> pl.DataFrame:
        """
        todo: docstring
        """
        method, kwargs = self.to_read_method_and_kwargs()
        read_method = getattr(pl, method)
        if read_kwargs is not None:  # override default kwargs
            kwargs.update(read_kwargs)
            # print(f"{file_args = }")
            # print("kwargs: ")
            # for k, v in kwargs.items():
            #     print(f"  {k} = {v}")
        return read_method(*file_args, **kwargs)

    def to_scan_method_and_kwargs(self) -> T.Tuple[str, T.Dict[str, T.Any]]:
        """
        Get the appropriate scan method and keyword arguments for the chosen format.

        :return: A tuple containing the scan method name and a dictionary of keyword arguments.
        """
        if self.is_csv():
            return (
                ScanMethodEnum.scan_csv.value,
                resolve_kwargs(
                    has_header=self.csv_include_header,
                    separator=self.csv_delimiter,
                    eol_char=self.csv_line_terminator,
                    quote_char=self.csv_quote_char,
                    storage_options=self.storage_options,
                ),
            )
        elif self.is_json():  # pragma: no cover
            raise ValueError("polars doesn't support 'scan_json'!")
        elif self.is_ndjson():
            return (ScanMethodEnum.scan_ndjson.value, dict())
        elif self.is_parquet():
            return (
                ScanMethodEnum.scan_parquet,
                resolve_kwargs(
                    use_pyarrow=self.parquet_use_pyarrow,
                    storage_options=self.storage_options,
                ),
            )
        elif self.is_delta():
            return (
                ScanMethodEnum.scan_delta,
                resolve_kwargs(
                    storage_options=self.storage_options,
                ),
            )
        else:  # pragma: no cover
            raise NotImplementedError

    def to_scan_kwargs(self) -> T.Dict[str, T.Any]:  # pragma: no cover
        """
        Get the scan arguments for the write operation.

        A dictionary of scan arguments for the write method.
        """
        method, kwargs = self.to_scan_method_and_kwargs()
        return kwargs

    def scan(
        self,
        file_args: T.List[T.Any],
        scan_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
    ) -> pl.LazyFrame:
        """
        todo: docstring
        """
        method, kwargs = self.to_scan_method_and_kwargs()
        scan_method = getattr(pl, method)
        if scan_kwargs is not None:  # override default kwargs
            kwargs.update(scan_kwargs)
            # print(f"{file_args = }")
            # print("kwargs: ")
            # for k, v in kwargs.items():
            #     print(f"  {k} = {v}")
        return scan_method(*file_args, **kwargs)
