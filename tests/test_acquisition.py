import pytest

from project import acquisition


def test_synthetic_acquisition_requires_replacement() -> None:
    with pytest.raises(RuntimeError, match="replace the synthetic acquisition stub"):
        acquisition.acquire()
