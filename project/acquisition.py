"""Synthetic acquisition boundary for the runnable starter."""


def acquire() -> bytes:
    """Require a real project-owned acquisition before any fresh-source job."""

    raise RuntimeError("replace the synthetic acquisition stub before running a fresh-source job")
