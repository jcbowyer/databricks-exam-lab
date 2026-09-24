import pytest

from common.transforms import balance_band, mask_phone


def test_mask_phone_keeps_last_four():
    assert mask_phone("25-989-741-2988") == "***********2988"


def test_mask_phone_handles_none_and_short_values():
    assert mask_phone(None) is None
    assert mask_phone("12") == "12"
    assert mask_phone("1234", visible=0) == "****"


@pytest.mark.parametrize(
    "balance, expected",
    [(None, "unknown"), (-10.5, "negative"), (0.0, "standard"), (4999.99, "standard"), (5000, "premium")],
)
def test_balance_band(balance, expected):
    assert balance_band(balance) == expected
