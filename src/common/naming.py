"""Column naming helpers for TPC-H tables."""
import re

# TPC-H prefixes every column with its table initial(s): c_custkey, ps_supplycost, n_name ...
TPCH_PREFIXES = ("c_", "o_", "l_", "p_", "s_", "ps_", "n_", "r_")


def strip_tpch_prefix(column: str) -> str:
    """c_custkey -> custkey, ps_supplycost -> supplycost. Other names are returned unchanged."""
    for prefix in sorted(TPCH_PREFIXES, key=len, reverse=True):  # try "ps_" before "p_"
        if column.startswith(prefix) and len(column) > len(prefix):
            return column[len(prefix):]
    return column


def to_snake_case(name: str) -> str:
    """'Total Spend' -> total_spend, 'marketSegment' -> market_segment."""
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    return name.strip("_").lower()


def rename_map(columns: list[str]) -> dict[str, str]:
    """Build {old: new} for DataFrame.withColumnsRenamed(); raises if two columns collide."""
    mapping = {c: to_snake_case(strip_tpch_prefix(c)) for c in columns}
    targets = list(mapping.values())
    duplicates = {t for t in targets if targets.count(t) > 1}
    if duplicates:
        raise ValueError(f"Renaming would create duplicate columns: {sorted(duplicates)}")
    return mapping
