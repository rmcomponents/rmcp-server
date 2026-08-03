import re

def normalize_mpn(mpn: str) -> str:
    """
    Normalize a manufacturer part number (MPN) for deduplication and cache keys.

    The normalization rule is:
    - Remove all whitespace (spaces, tabs, newlines).
    - Convert to uppercase.
    - Remove any character that is not alphanumeric, '-', '_', or '.'.

    This is intentionally lossy; use it only for internal lookups, not for
    display or response keys.
    """
    if not mpn:
        return ""
    # Remove all whitespace
    mpn = re.sub(r"\s+", "", mpn)
    # Convert to uppercase
    mpn = mpn.upper()
    # Remove any character that is not alphanumeric, '-', '_', or '.'
    mpn = re.sub(r"[^A-Z0-9\-_.]", "", mpn)
    return mpn
