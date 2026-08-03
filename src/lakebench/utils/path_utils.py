def abfss_to_https(abfss_path: str) -> str:
    """
    Convert an ABFSS path to an HTTPS URL.

    Example:
        abfss_path = "abfss://
    """
    import posixpath

    storage_account_endpoint = abfss_path.split("@")[1].split("/")[0]
    container = abfss_path.split("@")[0].split("abfss://")[1]
    file_path = abfss_path.split("@")[1].split("/")[1:]
    https_parquet_folder_path = posixpath.join("https://", storage_account_endpoint, container, "/".join(file_path))

    return https_parquet_folder_path


def to_unix_path(path_str) -> str:
    # Handle Windows drive letters and backslashes
    result = path_str.replace("\\", "/")

    # Remove Windows drive letters (C:, D:, etc.)
    if len(result) >= 2 and result[1] == ":":
        result = result[2:]

    # Ensure it starts with '/'
    if not result.startswith("/"):
        result = "/" + result

    return result


_REMOTE_SCHEMES = ("abfss://", "wasbs://", "az://", "s3://", "gs://", "file://")


def is_remote_uri(path: str) -> bool:
    """Return True if *path* already carries a recognised remote/URI scheme."""
    return any(path.startswith(s) for s in _REMOTE_SCHEMES)


def to_local_path(path: str) -> str:
    """Normalise a local filesystem path for engines that reject ``file://`` URIs.

    Remote URIs (``abfss://``, ``s3://``, ``file://``, ...) are passed through
    unchanged.  Local paths are returned as bare paths with forward slashes.

    Daft's object store mis-parses ``file:///C:/...`` on Windows (it strips the
    scheme but keeps the leading slash, yielding ``/C:``), while bare
    drive-letter paths round-trip correctly.

    Examples::

        to_local_path(r"C:\\Users\\foo\\data")       # -> "C:/Users/foo/data"
        to_local_path("abfss://container@acct/x")   # -> unchanged
    """
    if is_remote_uri(path):
        return path
    return path.replace("\\", "/")


def to_file_uri(path: str) -> str:
    """Convert a local filesystem path to a ``file:///`` URI.

    Passes through paths that already start with a recognised remote scheme
    (``abfss://``, ``s3://``, ``file://``, etc.) unchanged.  Useful when an
    engine requires a proper URI rather than a bare Windows drive-letter path.

    Examples::

        to_file_uri(r"C:\\Users\\foo\\data")  # -> "file:///C:/Users/foo/data"
        to_file_uri("abfss://container@acct/path")  # -> unchanged
    """
    if is_remote_uri(path):
        return path
    import pathlib

    return pathlib.Path(path).as_uri()
