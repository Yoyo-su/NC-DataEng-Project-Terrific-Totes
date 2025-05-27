import pytest  # type: ignore
from src.sample import sample


@pytest.mark.it("Sample test")
def test_sample():
    result = sample()
    assert result == "hello"
