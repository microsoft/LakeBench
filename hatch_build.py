import os
import platform
import stat
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    VENDORED_BINARIES = {
        ("windows", "amd64"): ("windows-x86_64/tpcgen-cli.exe", "py3-none-win_amd64"),
        ("windows", "x86_64"): ("windows-x86_64/tpcgen-cli.exe", "py3-none-win_amd64"),
        ("linux", "amd64"): ("linux-x86_64/tpcgen-cli", "py3-none-manylinux_2_17_x86_64"),
        ("linux", "x86_64"): ("linux-x86_64/tpcgen-cli", "py3-none-manylinux_2_17_x86_64"),
    }

    def initialize(self, version, build_data):
        if self.target_name != "wheel":
            return

        binary = os.environ.get("LAKEBENCH_TPCGEN_BINARY")
        if not binary:
            platform_key = (platform.system().lower(), platform.machine().lower())
            vendored = self.VENDORED_BINARIES.get(platform_key)
            if vendored is None:
                return

            relative_binary, default_wheel_tag = vendored
            bundle_root = Path(self.root) / "native" / "tpcgen"
            binary = bundle_root / relative_binary
            provenance = binary.parent / "provenance.json"
            license_file = bundle_root / "LICENSE.tpcgen-rs"
            wheel_tag = default_wheel_tag
        else:
            wheel_tag = os.environ.get("LAKEBENCH_WHEEL_TAG")
            provenance = os.environ.get("LAKEBENCH_TPCGEN_PROVENANCE")
            license_file = os.environ.get("LAKEBENCH_TPCGEN_LICENSE")
            if not wheel_tag or not provenance or not license_file:
                raise RuntimeError(
                    "Native wheel builds require LAKEBENCH_WHEEL_TAG, "
                    "LAKEBENCH_TPCGEN_PROVENANCE, and LAKEBENCH_TPCGEN_LICENSE."
                )

        binary_path = Path(binary).resolve()
        provenance_path = Path(provenance).resolve()
        license_path = Path(license_file).resolve()
        for required_path in (binary_path, provenance_path, license_path):
            if not required_path.is_file():
                raise RuntimeError(f"Native wheel input is missing: {required_path}")

        binary_path.chmod(binary_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        build_data["pure_python"] = False
        build_data["tag"] = wheel_tag
        build_data["force_include"][str(binary_path)] = f"lakebench/datagen/_bin/{binary_path.name}"
        build_data["force_include"][str(provenance_path)] = "lakebench/datagen/_bin/provenance.json"
        build_data["force_include"][str(license_path)] = "lakebench/datagen/_bin/LICENSE.tpcgen-rs"
