"""
CLI override application — `-E key=val` and `--conf key=val`.

Extracted from the monolithic cli.py so the precedence logic is testable and
reusable without importing argparse glue.
"""

from __future__ import annotations

import json
import os
from typing import List


def parse_value(raw: str):
    """Parse a CLI value as JSON if it looks like JSON; otherwise return the raw string.

    Accepts: {..}, [..], "..", numbers, true/false/null. Falls back to string on
    any JSON decode error so ``--conf spark.sql.foo=bar`` still works.
    """
    s = raw.strip()
    if not s:
        return raw
    first = s[0]
    looks_jsonish = (
        first in '{["'
        or s in ("true", "false", "null")
        or (first == "-" and len(s) > 1 and s[1].isdigit())
        or first.isdigit()
    )
    if looks_jsonish:
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass
    return raw


def set_dotted(target: dict, dotted_key: str, value):
    """Set a value in a nested dict using a dotted path.

    Unknown spark.* keys stay as single literal keys (no nesting) because
    Spark conf keys naturally contain dots, but callers can force nesting with
    explicit bracket syntax later if ever needed. Here we only special-case:
    if the FIRST segment matches a known nestable container (session_conf,
    engine_options, benchmark_options), walk into it; after that, the rest of
    the key is used as a single flat key.

    Note: nesting is exactly one level deep beyond the NESTABLE head. Keys like
    ``benchmark_options.scenarios.foo.bar`` set the literal key
    ``"scenarios.foo.bar"`` on ``benchmark_options`` rather than recursively
    descending. Use ``-E benchmark_options={...}`` with a JSON value if you
    need deeper structure.
    """
    NESTABLE = {"session_conf", "engine_options", "benchmark_options"}
    if "." not in dotted_key:
        target[dotted_key] = value
        return
    head, rest = dotted_key.split(".", 1)
    if head in NESTABLE:
        sub = target.setdefault(head, {})
        if not isinstance(sub, dict):
            raise ValueError(f"Cannot overlay into '{head}' — existing value is not a dict")
        sub[rest] = value
    else:
        # Flat: spark.sql.foo stays as the literal key
        target[dotted_key] = value


def apply_overrides(profile: dict, eopts: list, confs: list):
    """Apply -E / --conf overrides onto the profile dict.

    -E KEY=VALUE overlays onto profile['engine_options']. KEY may be dotted to
    reach into session_conf (e.g. session_conf.spark.sql.shuffle.partitions).
    VALUE is parsed as JSON when it looks like JSON, otherwise as a string.

    --conf KEY=VALUE is a shortcut that always targets
    engine_options.session_conf[KEY] with VALUE kept as a string (Spark confs
    are typed at use-time).

    Precedence (last wins): profile defaults < -E overlays < --conf overlays.
    Within the same flag, later occurrences win. This means if both flags
    target the same session_conf key, --conf is the final word.
    """
    engine_options = profile.setdefault("engine_options", {})

    for opt in eopts:
        if "=" not in opt:
            raise ValueError(f"--engine-option must be KEY=VALUE, got: {opt}")
        k, v = opt.split("=", 1)
        set_dotted(engine_options, k, parse_value(v))

    if confs:
        session_conf = engine_options.setdefault("session_conf", {})
        if not isinstance(session_conf, dict):
            raise ValueError("engine_options.session_conf must be a dict to apply --conf")
        for opt in confs:
            if "=" not in opt:
                raise ValueError(f"--conf must be KEY=VALUE, got: {opt}")
            k, v = opt.split("=", 1)
            session_conf[k] = v  # Spark confs are stringly-typed by convention


def load_eopts_file(path: str) -> List[str]:
    """Load -E overrides from a JSON file (object of KEY:VALUE) into KEY=VALUE strings.

    Values are JSON-serialized so parse_value's JSON path picks them back up.
    Strings stay as bare strings so spark.foo=bar works.
    """
    with open(os.path.expanduser(path)) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"--engine-options-file must contain a JSON object, got {type(data).__name__}")
    out = []
    for k, v in data.items():
        if isinstance(v, str):
            out.append(f"{k}={v}")
        else:
            out.append(f"{k}={json.dumps(v)}")
    return out


def load_conf_file(path: str) -> List[str]:
    """Load --conf overrides from a Java .properties-style or JSON file."""
    p = os.path.expanduser(path)
    with open(p) as f:
        text = f.read()
    out = []
    if text.lstrip().startswith("{"):
        data = json.loads(text)
        for k, v in data.items():
            out.append(f"{k}={v}")
        return out
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        if "=" not in line:
            raise ValueError(f"--conf-file entry missing '=': {line!r}")
        out.append(line)
    return out
