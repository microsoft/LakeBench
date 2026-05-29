import pytest

from lakebench.utils.path_utils import abfss_to_https, to_unix_path


class TestAbfssToHttps:
    def test_basic_conversion(self):
        abfss = "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.parquet"
        result = abfss_to_https(abfss)
        assert "myaccount.dfs.core.windows.net" in result
        assert "mycontainer" in result
        assert "folder/file.parquet" in result

    def test_no_abfss_scheme_in_result(self):
        abfss = "abfss://data@storage.dfs.fabric.microsoft.com/path/to/file"
        result = abfss_to_https(abfss)
        assert result.startswith("https://")

    def test_container_and_path_preserved(self):
        abfss = "abfss://bronze@account.dfs.core.windows.net/tables/orders"
        result = abfss_to_https(abfss)
        assert "bronze" in result
        assert "tables/orders" in result


class TestToUnixPath:
    def test_backslashes_converted(self):
        assert to_unix_path("C:\\Users\\foo\\bar") == "/Users/foo/bar"

    def test_forward_slashes_unchanged(self):
        assert to_unix_path("/home/user/data") == "/home/user/data"

    def test_windows_drive_stripped(self):
        result = to_unix_path("D:\\data\\lake")
        assert not result.startswith("D:")
        assert result.startswith("/")

    def test_no_leading_slash_gets_one(self):
        result = to_unix_path("relative/path")
        assert result.startswith("/")

    def test_empty_relative_path(self):
        result = to_unix_path("folder")
        assert result == "/folder"
