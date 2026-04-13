import fsspec
import polars as pl


def open_uri(uri: str):
    """
    Opens a URI (file://, gs://, s3://) and returns a file-like object or a path
    that can be used by polars.
    """
    if uri.startswith("file://"):
        return uri.replace("file://", "")
    return fsspec.open(uri, mode='rb')


def scan_parquet(uri: str) -> pl.LazyFrame:
    """
    Scans parquet using polars and fsspec.
    Returns a LazyFrame for scale-aware processing.
    """
    if uri.startswith("file://"):
        return pl.scan_parquet(uri.replace("file://", ""))
    
    # For cloud URIs, we can't always use scan_parquet directly with fsspec in all versions/setups
    # but polars supports fsspec-like paths if fsspec is installed.
    # Alternatively, use read_parquet with columns pushdown if scan is not feasible.
    try:
        return pl.scan_parquet(uri)
    except Exception:
        # Fallback to read_parquet if scan fails for the URI
        with open_uri(uri) as f:
            return pl.read_parquet(f).lazy()
