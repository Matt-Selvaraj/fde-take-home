import fsspec
import polars as pl


def open_uri(uri: str):
    """
    Opens a URI (file://, gs://, s3://) and returns a file-like object or a path
    that can be used by polars.
    """
    # fsspec handles file://, gs://, s3:// out of the box
    return fsspec.open(uri, mode='rb')


def read_parquet(uri: str) -> pl.DataFrame:
    """
    Reads parquet using polars and fsspec.
    """
    # Polars can read from fsspec file objects
    with open_uri(uri) as f:
        return pl.read_parquet(f)
