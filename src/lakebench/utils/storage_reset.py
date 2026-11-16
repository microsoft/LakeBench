"""Fail-closed boundaries for explicitly authorized benchmark resets."""

from __future__ import annotations

import ntpath
import os
import posixpath
import re
import sys
from typing import Iterable, NamedTuple, Optional, Sequence
from urllib.parse import unquote, urlsplit


class _StoragePath(NamedTuple):
    namespace: tuple[str, str]
    parts: tuple[str, ...]
    local_path: Optional[str] = None


def local_storage_path(uri: str) -> str:
    """Keep absolute file URIs absolute when handing them to local engines."""
    path = uri.replace("\\", "/")
    if path.lower().startswith("file:"):
        parsed = urlsplit(path)
        if parsed.query or parsed.fragment or not parsed.path.startswith("/"):
            raise ValueError("File storage URIs must be absolute and contain no query or fragment.")
        path = unquote(parsed.path)
        if parsed.netloc and parsed.netloc.lower() != "localhost":
            path = f"//{parsed.netloc}{path}"
        if re.match(r"^/[A-Za-z]:/", path):
            path = path[1:]
    return path


def _storage_path(uri: str) -> _StoragePath:
    if not isinstance(uri, str) or not uri.strip():
        raise ValueError("A non-empty, dedicated benchmark storage path is required.")
    path = local_storage_path(uri)
    if any(character in path for character in ("*", "?", "[", "]", "\x00", "\n", "\r")):
        raise ValueError("Benchmark storage paths must not contain globs, queries or control characters.")

    if re.match(r"^[A-Za-z]:", path) or path.startswith("//"):
        if re.match(r"^[A-Za-z]:", path) and not path[2:].startswith("/"):
            raise ValueError("Windows benchmark storage paths must be absolute.")
        if os.name == "nt":
            path = os.path.realpath(path).replace("\\", "/")
        drive, tail = ntpath.splitdrive(path)
        if not drive or not tail.startswith("/"):
            # A UNC share itself has no tail and is still represented as a root.
            if not (drive.startswith("//") and not tail):
                raise ValueError("Windows benchmark storage paths must be absolute.")
        normalized = ntpath.normpath(tail).replace("\\", "/").strip("/")
        parts = tuple(part.casefold() for part in normalized.split("/") if part and part != ".")
        return _StoragePath(("windows", drive.casefold()), parts)

    parsed = urlsplit(path)
    scheme = parsed.scheme.lower()
    if not scheme:
        real_path = os.path.realpath(os.path.expanduser(path))
        comparison_path = real_path.casefold() if sys.platform == "darwin" else real_path
        return _StoragePath(("file", ""), tuple(part for part in comparison_path.split(os.sep) if part), real_path)
    if parsed.query or parsed.fragment:
        raise ValueError("Benchmark storage URIs must not contain a query or fragment; use storage_options.")

    authority = unquote(parsed.netloc).lower()
    remote_path = unquote(parsed.path)
    if any(character in remote_path for character in ("*", "?", "[", "]", "\x00", "\\", "\n", "\r")):
        raise ValueError("Ambiguous benchmark storage URI.")
    scheme = {
        "s3a": "s3",
        "s3n": "s3",
        "gcs": "gs",
        "abfs": "abfss",
        "wasb": "abfss",
        "wasbs": "abfss",
    }.get(scheme, scheme)
    if scheme == "abfss":
        if "@" not in authority:
            raise ValueError("ABFS storage requires a container and account.")
        container, authority = authority.split("@", 1)
        remote_path = f"/{container}/{remote_path.lstrip('/')}"
        scheme = "azure"
    elif scheme == "https" and authority.endswith(
        (".dfs.core.windows.net", ".blob.core.windows.net", ".dfs.fabric.microsoft.com")
    ):
        scheme = "azure"
    elif scheme not in ("s3", "gs", "dbfs", "hdfs"):
        raise ValueError(f"Unsupported reset storage scheme: {scheme!r}.")

    if scheme not in ("dbfs",) and not authority:
        raise ValueError("Remote benchmark storage requires an authority.")
    if scheme == "azure":
        authority = authority.replace(".blob.core.windows.net", ".dfs.core.windows.net")
        # Azure/OneLake aliases must not evade conservative overlap checks.
        remote_path = remote_path.casefold()
    normalized = posixpath.normpath("/" + remote_path.lstrip("/"))
    parts = tuple(part for part in normalized.split("/") if part)
    if scheme == "azure" and (not parts or parts[0] != remote_path.strip("/").split("/")[0]):
        raise ValueError("An Azure storage path must stay within its container/workspace.")
    return _StoragePath((scheme, authority), parts)


def _contains(parent: _StoragePath, child: _StoragePath) -> bool:
    return parent.namespace == child.namespace and child.parts[: len(parent.parts)] == parent.parts


def validate_reset_path(target_uri: str, protected_uris: Sequence[str] = ()) -> None:
    """Refuse roots, known shared boundaries, and ancestor/descendant overlaps."""
    target = _storage_path(target_uri)
    scheme, authority = target.namespace
    minimum_parts = 1
    if scheme == "file":
        minimum_parts = 2
        shared = (
            "/",
            "/tmp",
            "/var/tmp",
            "/private/tmp",
            "/Users",
            "/home",
            "/mnt",
            "/media",
            "/Volumes",
            "/lakehouse",
            "/lakehouse/default",
            "/lakehouse/default/Files",
            "/lakehouse/default/Tables",
            "/synfs",
        )
        if (
            any(target.local_path == os.path.realpath(path) for path in shared)
            or os.path.ismount(target.local_path)
            or any(_contains(target, _storage_path(path)) for path in (os.getcwd(), os.path.expanduser("~")))
            or any(
                os.path.dirname(target.local_path) == os.path.realpath(parent)
                for parent in ("/Users", "/home", "/mnt", "/media", "/Volumes")
            )
        ):
            raise ValueError("Refusing to reset a root, shared location, home or working-directory ancestor.")
    elif scheme == "windows":
        minimum_parts = 1 if authority.startswith("//") else 2
        if (len(target.parts) == 2 and target.parts[0] in ("users", "documents and settings")) or any(
            _contains(target, _storage_path(path)) for path in (os.getcwd(), os.path.expanduser("~"))
        ):
            raise ValueError("Refusing to reset a shared location, home or working-directory ancestor.")
    elif scheme == "azure":
        minimum_parts = 2  # container plus dedicated prefix
        if "onelake." in authority:
            minimum_parts = 4  # workspace / item / Files-or-Tables / dedicated prefix
    elif scheme in ("dbfs", "hdfs"):
        minimum_parts = 2
        if target.parts == ("user", "hive", "warehouse") or (
            scheme == "dbfs" and len(target.parts) == 2 and target.parts[0] == "mnt"
        ):
            raise ValueError("Refusing to reset a shared warehouse or mount root.")
    if len(target.parts) < minimum_parts:
        raise ValueError("Refusing to reset a storage root or shared location; select a dedicated benchmark child.")

    for protected_uri in protected_uris:
        if protected_uri is None:
            continue
        protected = _storage_path(protected_uri)
        if _contains(target, protected) or _contains(protected, target):
            raise ValueError("Benchmark reset storage must not overlap input or result storage (in either direction).")


def validate_reset_schema(schema_name: str, catalog_name: Optional[str] = None) -> None:
    """A reset owns exactly one dedicated, non-system schema, never a catalog."""
    for name in (schema_name, catalog_name):
        if name is not None and (
            not isinstance(name, str) or not name.strip() or re.search(r"""[.`'";/\\*?\[\]\x00-\x1f]""", name)
        ):
            raise ValueError("Reset schema/catalog names must be single, unquoted identifiers.")
    if catalog_name and catalog_name.casefold() in {"system", "sys", "information_schema"}:
        raise ValueError("Refusing to reset a schema in a system catalog.")
    if (
        not schema_name
        or schema_name.casefold()
        in {
            "default",
            "dbo",
            "public",
            "main",
            "information_schema",
            "sys",
            "pg_catalog",
            "guest",
        }
        or schema_name.casefold().startswith(("pg_", "db_"))
    ):
        raise ValueError("Refusing to reset a shared/default/system schema; select a dedicated benchmark schema.")


def validate_metadata_storage(
    details: Iterable[Sequence[str]], protected_uris: Sequence[str], *, allow_view: bool = False
) -> None:
    """Validate DESCRIBE metadata before dropping a schema or one of its objects."""
    metadata = {str(row[0]).strip().casefold(): row[1] for row in details if len(row) >= 2}
    location = metadata.get("location")
    if location:
        validate_reset_path(location, protected_uris)
    elif not (allow_view and str(metadata.get("type")).upper() == "VIEW"):
        raise ValueError("Cannot verify the existing schema/table storage location; refusing reset.")
