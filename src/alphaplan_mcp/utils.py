import re

def normalize_mpn(mpn: str) -> str:
    """
    Normalize a manufacturer part number (MPN).
    Converts to uppercase, removes all whitespace, and strips any characters
    that are not alphanumeric, '-', '_', or '.'.
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
