"""Row-level transforms written as plain Python so they can be unit tested and wrapped in UDFs."""
from typing import Optional


def mask_phone(phone: Optional[str], visible: int = 4) -> Optional[str]:
    """'25-989-741-2988' -> '***********2988'. Keeps the last `visible` characters."""
    if phone is None:
        return None
    if visible <= 0:
        return "*" * len(phone)
    return "*" * max(len(phone) - visible, 0) + phone[-visible:]


def balance_band(balance: Optional[float]) -> str:
    """Bucket a TPC-H account balance (c_acctbal ranges roughly -1000..10000)."""
    if balance is None:
        return "unknown"
    if balance < 0:
        return "negative"
    if balance < 5000:
        return "standard"
    return "premium"
