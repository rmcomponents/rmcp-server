import re
import unicodedata

# Dash-like code points collapsed to ASCII HYPHEN-MINUS (U+002D).
# Kept as a separate step: NFKC maps U+2011 to U+2010, not to U+002D.
_DASH_CODEPOINTS = (
    "\u2010\u2011\u2012\u2013\u2014\u2015"  # HYPHEN, NON-BREAKING HYPHEN, ..., EM DASH, HORIZONTAL BAR
    "\u2212"                                # MINUS SIGN
    "\ufe58\ufe63\uff0d"                    # SMALL EM DASH, SMALL HYPHEN-MINUS, FULLWIDTH HYPHEN-MINUS
)
_FOLD_DASHES_TRANS = str.maketrans({c: "-" for c in _DASH_CODEPOINTS})

# Fixed set of normalization steps applied by normalize_mpn (reported by join).
NORMALIZE_STEPS = ("trim_whitespace", "unicode_nfkc", "fold_dashes", "uppercase", "strip")


def normalize_mpn(value):
    """
    Normalize a manufacturer part number / join key for comparison.

    Applies a single fixed pipeline (no configuration):
    1. Unicode NFKC normalization (fullwidth/compatibility forms -> ASCII where possible).
    2. Dash folding: map U+2010..U+2015, U+2212, U+FE58, U+FE63, U+FF0D -> '-' (done after
       NFKC because NFKC maps U+2011 to U+2010, not to U+002D).
    3. Remove *all* whitespace (interior and leading/trailing, incl. U+00A0).
    4. Convert to uppercase.
    5. Remove any character that is not alphanumeric, '-', '_', or '.'.

    This is intentionally lossy; use it only for internal matching / deduplication /
    cache keys, never for display or response keys.

    Returns the normalized string. `None` stays `None`; non-string values are returned
    unchanged (so numeric join keys are matched on raw equality).
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    if not value:
        return ""
    value = unicodedata.normalize("NFKC", value)
    value = value.translate(_FOLD_DASHES_TRANS)
    value = re.sub(r"\s+", "", value)
    value = value.upper()
    value = re.sub(r"[^A-Z0-9\-_.]", "", value)
    return value
