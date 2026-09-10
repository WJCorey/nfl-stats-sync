from __future__ import annotations

import json
import os
import re
from pathlib import Path

TARGET = "example/mushroom-starter"
MANAGED_SCOPE = "ExampleRecord/example/**"
ABSENCE_POLICY = "preserve"

_FIXTURES = Path(__file__).parent / "fixtures"
_IDENTIFIER = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")


def target() -> str:
    return os.environ.get("MUSHROOM_TARGET", TARGET)


def source_fixture() -> Path:
    return _FIXTURES / "source.json"


def current_state_fixture() -> Path:
    return _FIXTURES / "current.jsonl"


def transform(source: bytes) -> list[dict[str, object]]:
    """Map the synthetic snapshot into deterministic operation-neutral records."""

    payload = json.loads(source)
    if not isinstance(payload, dict) or payload.get("complete") is not False:
        raise ValueError("the synthetic snapshot must declare complete=false")
    rows = payload.get("records")
    if not isinstance(rows, list) or not rows:
        raise ValueError("the synthetic snapshot must contain records")

    records: dict[str, dict[str, object]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("each synthetic record must be an object")
        identifier, label = row.get("id"), row.get("label")
        if not isinstance(identifier, str) or not _IDENTIFIER.fullmatch(identifier):
            raise ValueError(f"invalid synthetic record id: {identifier!r}")
        if not isinstance(label, str) or not label.strip():
            raise ValueError(f"invalid synthetic record label for {identifier}")
        if identifier in records:
            raise ValueError(f"duplicate synthetic record id: {identifier}")
        records[identifier] = {
            "kind": "thing",
            "shape": "ExampleRecord",
            "wref": f"ExampleRecord/example/{identifier}",
            "data": {"label": label},
        }
    return [records[key] for key in sorted(records)]
