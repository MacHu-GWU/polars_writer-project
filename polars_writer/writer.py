# -*- coding: utf-8 -*-

import typing as T
import enum
import dataclasses

from func_args import NOTHING, resolve_kwargs

if T.TYPE_CHECKING:  # pragma: no cover
    import polars as pl


class FormatEnum(str, enum.Enum):
    csv = "csv"
    json = "json"
    ndjson = "ndjson"
    parquet = "parquet"
    delta = "delta"


class WriteMethodEnum(str, enum.Enum):
    write_csv = "write_csv"
    write_json = "write_json"
    write_ndjson = "write_ndjson"
    write_parquet = "write_parquet"
    write_delta = "write_delta"


class ParquetCompressionEnum(str, enum.Enum):
    lz4 = "lz4"
    uncompressed = "uncompressed"
    snappy = "snappy"
    gzip = "gzip"
    lzo = "lzo"
    brotli = "brotli"
    zstd = "zstd"


class DeltaModeEnum(str, enum.Enum):
    error = "error"
    append = "append"
    overwrite = "overwrite"
    ignore = "ignore"
    merge = "merge"


@dataclasses.dataclass
class Writer:
    format: str = dataclasses.field()
    # common
    storage_options: T.Dict[str, T.Any] = dataclasses.field(default_factory=dict)
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
    # delta
    delta_mode: str = dataclasses.field(default=NOTHING)
    delta_overwrite_schema: bool = dataclasses.field(default=NOTHING)
    delta_write_options: T.Dict[str, T.Any] = dataclasses.field(default_factory=dict)
    delta_merge_options: T.Dict[str, T.Any] = dataclasses.field(default_factory=dict)

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
        if self.is_csv():
            return WriteMethodEnum.write_csv.value, resolve_kwargs(
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
            )
        elif self.is_json():
            return WriteMethodEnum.write_json.value, dict()
        elif self.is_ndjson():
            return WriteMethodEnum.write_ndjson.value, dict()
        elif self.is_parquet():
            return WriteMethodEnum.write_parquet, resolve_kwargs(
                compression=self.parquet_compression,
                compression_level=self.parquet_compression_level,
            )
        elif self.is_delta():
            return WriteMethodEnum.write_delta, resolve_kwargs(
                mode=self.delta_mode,
                overwrite_schema=self.delta_overwrite_schema,
                delta_write_options=self.delta_write_options,
                delta_merge_options=self.delta_merge_options,
            )
        else:  # pragma: no cover
            raise NotImplementedError

    def to_kwargs(self) -> T.Dict[str, T.Any]:
        method, kwargs = self.to_method_and_kwargs()
        return kwargs

    def write(
        self,
        df: "pl.DataFrame",
        file_args: T.List[T.Any],
    ):
        method, kwargs = self.to_method_and_kwargs()
        write_method = getattr(df, method)
        return write_method(*file_args, **kwargs)
