import logging
import os
import subprocess

logger = logging.getLogger(__name__)


class TPCDIDataGenerator:
    """
    Wrapper for the TPC-DI data generator (DIGen.jar).

    Generates TPC-DI source data files (CSV, XML, fixed-width, pipe-delimited)
    organized into Batch1/ (historical), Batch2/, Batch3/ (incremental) directories.

    Requires Java to be installed and accessible on the system PATH.

    Parameters
    ----------
    scale_factor : int
        The TPC-DI scale factor (e.g., 5, 10, 100, 1000). Determines dataset size.
    target_folder : str
        The output directory where generated data will be stored.
    digen_jar_path : str, optional
        Path to DIGen.jar. If not provided, searches for it in common locations.

    Methods
    -------
    run()
        Generates TPC-DI data files based on the specified scale factor.
    """

    def __init__(self, scale_factor: int, target_folder: str, digen_jar_path: str = None):
        self.scale_factor = scale_factor
        self.target_folder = target_folder

        if digen_jar_path:
            self.digen_jar_path = digen_jar_path
        else:
            # Search common locations
            search_paths = [
                os.path.join(os.getcwd(), "TPC-DI", "DIGen.jar"),
                os.path.join(os.path.dirname(__file__), "..", "..", "..", "TPC-DI", "DIGen.jar"),
                os.path.expanduser("~/TPC-DI/DIGen.jar"),
            ]
            for path in search_paths:
                if os.path.exists(path):
                    self.digen_jar_path = os.path.abspath(path)
                    break
            else:
                raise FileNotFoundError(
                    "DIGen.jar not found. Please provide the path via digen_jar_path parameter. "
                    "Search paths: " + ", ".join(search_paths)
                )

    def run(self):
        """
        Generates TPC-DI data files based on the specified scale factor.

        The output directory will contain:
        - Batch1/: Historical load data (CSV, XML, fixed-width, pipe-delimited files)
        - Batch2/: First incremental batch
        - Batch3/: Second incremental batch
        - Batch1_audit.csv, Batch2_audit.csv, Batch3_audit.csv: Audit validation files
        - Generator_audit.csv: Scale factor parameters

        Returns
        -------
        str
            Path to the output directory containing generated data.

        Raises
        ------
        subprocess.CalledProcessError
            If the data generation process fails.
        RuntimeError
            If Java is not installed or DIGen.jar is not found.
        """
        # Verify Java is available
        try:
            subprocess.run(["java", "-version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError(
                "Java is required to run DIGen.jar but was not found on PATH. "
                "Please install Java (JDK 8+) and ensure it is on your PATH."
            )

        # Create output directory
        output_dir = os.path.join(self.target_folder, f"sf{self.scale_factor}")
        os.makedirs(output_dir, exist_ok=True)

        # Run DIGen
        digen_dir = os.path.dirname(self.digen_jar_path)
        cmd = [
            "java",
            "-jar",
            self.digen_jar_path,
            "-sf",
            str(self.scale_factor),
            "-o",
            output_dir,
        ]

        logger.info("Generating TPC-DI data with scale factor %s...", self.scale_factor)
        logger.info("Output directory: %s", output_dir)
        logger.info("Command: %s", " ".join(cmd))

        result = subprocess.run(
            cmd,
            cwd=digen_dir,
            capture_output=True,
            text=True,
            timeout=7200,  # 2 hour timeout for large scale factors
        )

        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)

        logger.info("TPC-DI data generation complete. Output: %s", output_dir)

        # Verify expected directories exist
        for batch in ["Batch1", "Batch2", "Batch3"]:
            batch_dir = os.path.join(output_dir, batch)
            if not os.path.isdir(batch_dir):
                raise RuntimeError(
                    f"Expected batch directory not found: {batch_dir}. Data generation may have failed silently."
                )

        return output_dir
