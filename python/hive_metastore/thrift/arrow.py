import logging
import re
from typing import Iterator

import pyarrow as pa
from pyarrow.dataset import dataset
from pyarrow.fs import S3FileSystem

from . import ThriftHiveMetastore
from .internal.ttypes import Table, FieldSchema, StorageDescriptor

logger = logging.Logger(__name__)


def unquote(string: str, quote: str = '"', escape: str = "\\") -> str:
    """
    If string starts and ends with a quote, unquote it
    """
    if string.startswith(quote) and string.endswith(quote):
        string = string[1:-1]
        string = string.replace(f"{escape}{quote}", quote).replace(f"{escape}{escape}", escape)
    return string


def aware_split(
    string: str,
    delimiter: str = ",",
    max_split: int = -1,
    quote: str = '"',
    escaped_quote: str = r"\"",
    open_bracket: str = "(",
    close_bracket: str = ")",
) -> Iterator[str]:
    """
    A split function that is aware of quotes and brackets/parentheses.

    :param string: string to split
    :param delimiter: string defining where to split, usually a comma or space
    :param max_split: Maximum number of splits to do. -1 (default) means no limit.
    :param quote: string, either a single or a double quote
    :param escaped_quote: string representing an escaped quote
    :param open_bracket: string, either [, {, < or (
    :param close_bracket: string, either ], }, > or )
    """
    parens = 0
    quotes = False
    i = 0
    if max_split < -1:
        raise ValueError(f"max_split must be >= -1, got {max_split}")
    elif max_split == 0:
        yield string
        return
    for j, character in enumerate(string):
        complete = parens == 0 and not quotes
        if complete and character == delimiter:
            if max_split != -1:
                max_split -= 1
            yield string[i:j]
            i = j + len(delimiter)
            if max_split == 0:
                break
        elif character == open_bracket:
            parens += 1
        elif character == close_bracket:
            parens -= 1
        elif character == quote:
            if quotes and string[j - len(escaped_quote) + 1: j + 1] != escaped_quote:
                quotes = False
            elif not quotes:
                quotes = True
    yield string[i:]


def parse_dtype(dtype: str) -> pa.DataType:
    """
    See Also:
        - https://cwiki.apache.org/confluence/display/hive/languagemanual+types
        - https://arrow.apache.org/docs/python/api/datatypes.html
    """
    dtype = dtype.strip().upper()
    match = re.match(r"^(?P<type>\w+)\s*(?:<(?P<options>.*)>)?", dtype)
    if not match:
        logger.warning(f"Could not parse type name '{dtype}'")
        return pa.null()
    type_name = match.group("type")
    type_opts = match.group("options")

    # === Structural ===
    if type_name == "ARRAY":
        item_type = parse_dtype(type_opts)
        return pa.list_(item_type)
    elif type_name == "MAP":
        key_type_str, value_type_str = aware_split(type_opts)
        key_type = parse_dtype(key_type_str)
        value_type = parse_dtype(value_type_str)
        return pa.map_(key_type, value_type)
    elif type_name == "STRUCT":
        fields: list[pa.Field] = []
        for attr in aware_split(type_opts):
            attr_name, attr_type_str = aware_split(attr.strip(), delimiter=" ", max_split=1)
            attr_name = unquote(attr_name)
            attr_type = parse_dtype(attr_type_str)
            fields.append(pa.field(attr_name, attr_type))
        return pa.struct(fields)
    # === Date and time ===
    elif type_name == "DATE":
        return pa.date32()
    elif type_name == "TIMESTAMP":
        return pa.timestamp(unit="us", tz=None)
    elif type_name == "INTERVAL":
        return pa.duration(unit="s")  # TODO
    # === String & Binary ===
    elif type_name in ("STRING", "VARCHAR", "CHAR"):
        return pa.string()  # Hive support VARCHAR, CHAR length, but Arrow doesn't
    elif type_name == "BINARY":
        return pa.binary()
    # === Fixed-precision ===
    elif type_name in ("DECIMAL", "NUMERIC"):
        precision, scale = 10, 0
        if type_opts:
            itr = aware_split(type_opts)
            precision = next(itr)
            scale = next(itr, 0)
        return pa.decimal128(precision, scale)
    # === Floating-point ===
    elif type_name == "FLOAT":
        return pa.float32()
    elif type_name in ("DOUBLE", "DOUBLE PRECISION"):
        return pa.float64()
    # === Integer ===
    elif type_name == "TINYINT":
        return pa.int8()
    elif type_name == "SMALLINT":
        return pa.int16()
    elif type_name in ("INT", "INTEGER"):
        return pa.int32()
    elif type_name == "BIGINT":
        return pa.int64()
    # === Boolean ===
    elif type_name == "BOOLEAN":
        return pa.bool_()
    # === Unknown datatype ===
    else:
        logger.warning(f"Unsupported datatype '{dtype}'")
        return pa.null()


def convert_fields(field: FieldSchema) -> pa.Field:
    dtype = parse_dtype(field.type)
    metadata = dict(comment=field.comment) if field.comment else None
    return pa.field(field.name, dtype, True, metadata)


def convert_schema(cols: list[FieldSchema], part_cols: list[FieldSchema]) -> pa.Schema:
    fields = [convert_fields(c) for c in cols]
    part_fields = [convert_fields(c) for c in part_cols]
    return pa.schema(fields + part_fields)

