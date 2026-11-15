import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from src.common.spark import get_spark
    s = get_spark("tp-tests", shuffle_partitions=2)
    yield s
    s.stop()
