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


def normalize_mpn_with_steps(value):
    """
    Normalize a manufacturer part number / join key and report which steps changed it.

    Returns ``(normalized, steps_applied)`` where ``steps_applied`` lists the names of
    the pipeline stages (from ``NORMALIZE_STEPS``) that actually altered the input, in
    application order. A step is recorded when its output differs from its input.

    `None` stays ``None``; non-string values are returned unchanged (with empty steps),
    mirroring `normalize_mpn`.
    """
    if value is None or not isinstance(value, str) or not value:
        return value, []

    steps = []

    # 1. Whitespace: NFKC folds U+00A0 into a plain space, so record this step when
    #    whitespace is present before NFKC (the remove happens after NFKC below).
    has_ws = bool(re.search(r"\s", value))

    new = unicodedata.normalize("NFKC", value)
    if new != value:
        steps.append("unicode_nfkc")
        if not has_ws and re.search(r"\s", new):
            has_ws = True  # e.g. U+00A0 became a plain space
    value = new

    new = value.translate(_FOLD_DASHES_TRANS)
    if new != value:
        steps.append("fold_dashes")
    value = new

    if has_ws:
        steps.append("trim_whitespace")
        value = re.sub(r"\s+", "", value)

    new = value.upper()
    if new != value:
        steps.append("uppercase")
    value = new

    new = re.sub(r"[^A-Z0-9\-_.]", "", value)
    if new != value:
        steps.append("strip")
    value = new

    # Report steps in the canonical NORMALIZE_STEPS order for a stable surface.
    return value, [s for s in NORMALIZE_STEPS if s in steps]


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
    return normalize_mpn_with_steps(value)[0]
