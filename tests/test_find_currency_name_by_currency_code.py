import pytest
from src.utils.find_currency_name_by_currency_code import (
    find_currency_name_by_currency_code,
)


class TestFindCurrencyNameByCurrencyCode:

    @pytest.mark.it("when passed a known currency code, returns correct currency name")
    def test_returns_currency_name(self):
        assert find_currency_name_by_currency_code("HKD") == "Hong Kong dollar"
        assert find_currency_name_by_currency_code("USD") == "United States dollar"
        assert find_currency_name_by_currency_code("GBP") == "British pound"

    @pytest.mark.it(
        "when passed an unknown currency code, raises KeyError with appropriate message"
    )
    def test_raises_key_error(self):
        with pytest.raises(KeyError, match="Currency code not found"):
            find_currency_name_by_currency_code("ABC")
