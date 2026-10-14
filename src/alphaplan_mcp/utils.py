import codecs
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


# --- RTF -> plain text ------------------------------------------------------------------
# AlphaPlan stores long texts (ZusatzText1..3, Vorsatz, Nachsatz, ...) either as plain text or
# as RichEdit RTF. The tokenizer below follows the RTF 1.9 spec closely enough for RichEdit /
# Word output; anything it cannot make sense of is returned unchanged.

_RTF_TOKEN = re.compile(
    r"\\([a-zA-Z]{1,32})(-?\d{1,10})? ?"  # control word with optional numeric arg
    r"|\\'([0-9a-fA-F]{2})"             # hex-escaped byte
    r"|\\([^a-zA-Z])"                   # control symbol
    r"|([{}])"                          # group start / end
    r"|[\r\n]+"                          # raw line breaks carry no meaning in RTF
    r"|(.)",                            # literal text
    re.S,
)

# Destinations whose content is never visible text.
_RTF_DESTINATIONS = frozenset({
    "aftncn", "aftnsep", "aftnsepc", "annotation", "atnauthor", "atndate", "atnicn", "atnid",
    "atnparent", "atnref", "atntime", "atrfend", "atrfstart", "author", "background",
    "bkmkend", "bkmkstart", "blipuid", "buptim", "category", "colorschememapping", "colortbl",
    "comment", "company", "creatim", "datafield", "datastore", "defchp", "defpap", "do",
    "doccomm", "docvar", "dptxbxtext", "ebcend", "ebcstart", "factoidname", "falt", "fchars",
    "ffdeftext", "ffentrymcr", "ffexitmcr", "ffformat", "ffhelptext", "ffl", "ffname",
    "ffstattext", "file", "filetbl", "fldinst", "fldtype", "fname", "fontemb", "fontfile",
    "fonttbl", "footer", "footerf", "footerl", "footerr", "footnote", "formfield", "ftncn",
    "ftnsep", "ftnsepc", "g", "generator", "gridtbl", "header", "headerf", "headerl",
    "headerr", "hl", "hlfr", "hlinkbase", "hlloc", "hlsrc", "hsv", "htmltag", "info",
    "keycode", "keywords", "latentstyles", "lchars", "levelnumbers", "leveltext", "lfolevel",
    "linkval", "list", "listlevel", "listname", "listoverride", "listoverridetable",
    "listpicture", "liststylename", "listtable", "listtext", "lsdlockedexcept", "manager",
    "mmathPr", "nesttableprops", "nextfile", "nonesttables", "objalias", "objclass",
    "objdata", "object", "objname", "objsect", "objtime", "oldcprops", "oldpprops",
    "oldsprops", "oldtprops", "oleclsid", "operator", "panose", "password", "passwordhash",
    "pgp", "pgptbl", "picprop", "pict", "pn", "pnseclvl", "pntext", "pntxta", "pntxtb",
    "printim", "private", "propname", "protend", "protstart", "protusertbl", "pxe", "result",
    "revtbl", "revtim", "rsidtbl", "rxe", "shp", "shpgrp", "shpinst", "shppict", "shprslt",
    "shptxt", "sn", "sp", "staticval", "stylesheet", "subject", "sv", "svb", "tc", "template",
    "themedata", "title", "txe", "ud", "upr", "userprops", "wgrffmtfilter", "windowcaption",
    "writereservation", "writereservhash", "xe", "xform", "xmlattrname", "xmlattrvalue",
    "xmlclose", "xmlname", "xmlnstbl", "xmlopen",
})

_RTF_SPECIAL_WORDS = {
    "par": "\n", "line": "\n", "sect": "\n\n", "page": "\n\n", "row": "\n",
    "tab": "\t", "cell": " | ", "nestcell": " | ",
    "emdash": "\u2014", "endash": "\u2013", "emspace": "\u2003", "enspace": "\u2002",
    "qmspace": "\u2005", "bullet": "\u2022", "lquote": "\u2018", "rquote": "\u2019",
    "ldblquote": "\u201c", "rdblquote": "\u201d",
}

_RTF_SPECIAL_SYMBOLS = {
    "~": "\u00a0", "_": "-", "-": "", "\\": "\\", "{": "{", "}": "}",
    "\n": "\n", "\r": "\n",  # backslash + line break is an alternative \par
}


def _rtf_codec(arg):
    try:
        return codecs.lookup(f"cp{int(arg)}").name
    except (LookupError, TypeError, ValueError):
        return "cp1252"


def _strip_rtf(rtf):
    out = []
    pending = bytearray()      # consecutive \'hh bytes, decoded together (DBCS code pages)
    encoding = "cp1252"
    stack = []
    ignorable = False
    ucskip = 1                 # characters to skip after \uN (\ucN)
    curskip = 0

    def emit(text):
        if pending:
            out.append(pending.decode(encoding, errors="replace"))
            pending.clear()
        out.append(text)

    for match in _RTF_TOKEN.finditer(rtf):
        word, arg, hexbyte, symbol, brace, char = match.groups()
        if brace:
            curskip = 0
            if brace == "{":
                stack.append((ucskip, ignorable))
            elif stack:
                ucskip, ignorable = stack.pop()
            else:
                break  # unbalanced closing brace: end of document
        elif symbol:
            curskip = 0
            if symbol == "*":
                ignorable = True
            elif not ignorable and symbol in _RTF_SPECIAL_SYMBOLS:
                emit(_RTF_SPECIAL_SYMBOLS[symbol])
        elif word:
            curskip = 0
            if word in _RTF_DESTINATIONS:
                ignorable = True
            elif word == "ansicpg":
                encoding = _rtf_codec(arg)
            elif ignorable:
                pass
            elif word in _RTF_SPECIAL_WORDS:
                emit(_RTF_SPECIAL_WORDS[word])
            elif word == "uc" and arg is not None:
                ucskip = max(int(arg), 0)
            elif word == "u" and arg is not None:
                code = int(arg)
                if code < 0:
                    code += 0x10000
                emit(chr(code))
                curskip = ucskip
        elif hexbyte:
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                pending.append(int(hexbyte, 16))
        elif char:
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                emit(char)
    emit("")
    text = "".join(out)
    # \uN can yield UTF-16 surrogate halves (characters outside the BMP); recombine them.
    return text.encode("utf-16", "surrogatepass").decode("utf-16", errors="replace")


def rtf_to_text(value):
    """
    Convert an AlphaPlan long-text value to plain text.

    RTF values (starting with ``{\\rtf``) are converted: formatting, font/colour tables and
    other invisible destinations are dropped, ``\\par``/``\\line`` become line breaks and
    hex/unicode escapes are decoded using the document's code page. Line endings are
    normalized to ``\\n`` and surrounding whitespace is trimmed; whitespace-only results
    become ``None``.

    Non-RTF strings are returned with the same line-ending/trim normalization but otherwise
    untouched – these fields often hold free text or garbage, so nothing is parsed out of
    them. Non-string values (incl. ``None``) are returned unchanged. If an RTF value cannot
    be converted, the original string is returned rather than raising.
    """
    if not isinstance(value, str):
        return value
    text = value
    if value.lstrip().startswith("{\\rtf"):
        try:
            text = _strip_rtf(value.lstrip())
        except Exception:  # never fail a whole result set over one malformed text
            return value
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "\n".join(line.rstrip() for line in text.split("\n")).strip()
    return text or None
