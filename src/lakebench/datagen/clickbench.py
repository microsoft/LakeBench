import logging
import posixpath
from typing import Optional

logger = logging.getLogger(__name__)


class ClickBenchDataGenerator:
    def __init__(self, target_mount_folder_uri: str = None, partitioned_files: bool = True):
        """
        Initialize the ClickBench data generator. Technically, this just downloads the ClickBench data from the ClickHouse datasets repository.

        :param partitioned_files: If True, the downloaded data will be 100 partitioned files, otherwise it is one massive file. Use partitioned files for better download performance.
        """
        self.target_mount_folder_path = target_mount_folder_uri
        self.partitioned_files = partitioned_files

    def run(self):
        """
        Download ClickBench Parquet files to the target folder.

        This method fetches data from the ClickHouse datasets repository and writes
        it to `self.target_mount_folder_path`. If `self.partitioned_files` is True,
        it will download 100 partitioned files using multithreading (hits_0.parquet … hits_99.parquet);
        otherwise it downloads a single file (hits.parquet).

        Notes
        -----
        - Use self.partitioned_files = True for better download performance.
        - Both the partitioned and non-partition parquet data is made up of ~ 180MB row groups so it should
        have little impact on performance for most systems.
        """

        if self.partitioned_files:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor() as executor:
                executor.map(self.__download_parquet, range(100))
        else:
            self.__download_parquet(None)

    def __download_parquet(self, file_index: Optional[int] = None):
        file_name = f"hits_{file_index}.parquet" if file_index is not None else "hits.parquet"
        source_folder = "athena_partitioned" if file_index is not None else "athena"

        import urllib.request

        url = f"https://datasets.clickhouse.com/hits_compatible/{source_folder}/{file_name}"
        local_path = posixpath.join(self.target_mount_folder_path, file_name)

        headers = {"User-Agent": "Mozilla/5.0"}
        req = urllib.request.Request(url, headers=headers)

        try:
            with urllib.request.urlopen(req) as response, open(local_path, "wb") as out_file:
                out_file.write(response.read())
            logger.info("Downloaded %s", file_name)
        except Exception as e:
            logger.error("Failed to download %s: %s", file_name, e)
