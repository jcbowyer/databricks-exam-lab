import pytest

from common.naming import rename_map, strip_tpch_prefix, to_snake_case


@pytest.mark.parametrize(
    "column, expected",
    [("c_custkey", "custkey"), ("ps_supplycost", "supplycost"), ("p_name", "name"), ("total", "total"), ("c_", "c_")],
)
def test_strip_tpch_prefix(column, expected):
    assert strip_tpch_prefix(column) == expected


@pytest.mark.parametrize(
    "name, expected",
    [("Total Spend", "total_spend"), ("marketSegment", "market_segment"), ("  already_snake ", "already_snake")],
)
def test_to_snake_case(name, expected):
    assert to_snake_case(name) == expected


def test_rename_map_customer_columns():
    assert rename_map(["c_custkey", "c_mktsegment"]) == {"c_custkey": "custkey", "c_mktsegment": "mktsegment"}


def test_rename_map_rejects_collisions():
    with pytest.raises(ValueError, match="name"):
        rename_map(["c_name", "n_name"])
