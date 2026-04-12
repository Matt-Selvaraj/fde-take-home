import fsspec
import polars as pl


def open_uri(uri: str):
    """
    Opens a URI (file://, gs://, s3://) and returns a file-like object or a path
    that can be used by polars.
    """
    return fsspec.open(uri, mode='rb')


def read_parquet(uri: str) -> pl.DataFrame:
    """
    Reads parquet using polars and fsspec.
    """
    with open_uri(uri) as f:
        return pl.read_parquet(f)
