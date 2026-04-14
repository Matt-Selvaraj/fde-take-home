from unittest.mock import patch, MagicMock

from app.utils.storage import open_uri, scan_parquet


def test_open_uri_file():
    uri = "file:///path/to/file.parquet"
    result = open_uri(uri)
    assert result == "/path/to/file.parquet"


@patch('fsspec.open')
def test_open_uri_cloud(mock_fsspec_open):
    uri = "gs://bucket/file.parquet"
    open_uri(uri)
    mock_fsspec_open.assert_called_once_with(uri, mode='rb')


@patch('polars.scan_parquet')
def test_scan_parquet_file(mock_scan):
    uri = "file:///path/to/file.parquet"
    scan_parquet(uri)
    mock_scan.assert_called_once_with("/path/to/file.parquet")


@patch('polars.scan_parquet')
def test_scan_parquet_cloud_success(mock_scan):
    uri = "gs://bucket/file.parquet"
    scan_parquet(uri)
    mock_scan.assert_called_once_with(uri)


@patch('polars.scan_parquet')
@patch('app.utils.storage.open_uri')
@patch('polars.read_parquet')
def test_scan_parquet_cloud_fallback(mock_read, mock_open_uri, mock_scan):
    uri = "gs://bucket/file.parquet"
    # First call to scan_parquet fails
    mock_scan.side_effect = [Exception("Scan failed"), MagicMock()]

    mock_file = MagicMock()
    mock_open_uri.return_value.__enter__.return_value = mock_file

    mock_df = MagicMock()
    mock_read.return_value = mock_df

    scan_parquet(uri)

    mock_open_uri.assert_called_once_with(uri)
    mock_read.assert_called_once_with(mock_file)
    mock_df.lazy.assert_called_once()
