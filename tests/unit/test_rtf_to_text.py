from alphaplan_mcp.utils import rtf_to_text

RICHEDIT = (
    "{\\rtf1\\ansi\\deff0\\nouicompat{\\fonttbl{\\f0\\fnil\\fcharset0 Microsoft Sans Serif;}}\r\n"
    "{\\colortbl ;\\red255\\green0\\blue0;}\r\n"
    "{\\*\\generator Riched20 10.0.26100}\\viewkind4\\uc1 \r\n"
    "\\pard\\cf1\\b\\f0\\fs16\\lang1031 Kurzfristig vom StraLa umbuchen lassen AV/NR/SeS\\par\r\n}\r\n"
)


def test_richedit_rtf_to_plain_text():
    assert rtf_to_text(RICHEDIT) == "Kurzfristig vom StraLa umbuchen lassen AV/NR/SeS"


def test_plain_text_passthrough_trimmed():
    assert rtf_to_text("Landed Cost Price: 13,47 $ / 1000  ") == "Landed Cost Price: 13,47 $ / 1000"
    assert rtf_to_text("a\r\nb") == "a\nb"


def test_none_empty_and_non_strings():
    assert rtf_to_text(None) is None
    assert rtf_to_text("") is None
    assert rtf_to_text("   ") is None
    assert rtf_to_text(42) == 42
    assert rtf_to_text("{\\rtf1\\ansi\\pard\\par}") is None


def test_hex_escapes_use_code_page():
    assert rtf_to_text("{\\rtf1\\ansi\\ansicpg1252 R\\'fcckstand \\'80}") == "Rückstand €"
    # double-byte code page: consecutive bytes are decoded together
    assert rtf_to_text("{\\rtf1\\ansi\\ansicpg932 \\'82\\'a0}") == "あ"


def test_unicode_escapes_skip_fallback():
    assert rtf_to_text("{\\rtf1\\ansi\\uc1 Preis \\u8364?}") == "Preis €"
    assert rtf_to_text("{\\rtf1\\ansi\\uc1 \\u-10179?\\u-8704?}") == "\U0001f600"


def test_paragraphs_tabs_and_escaped_symbols():
    rtf = "{\\rtf1\\ansi Zeile 1\\par Zeile 2\\line A\\tab B \\{x\\} \\\\}"
    assert rtf_to_text(rtf) == "Zeile 1\nZeile 2\nA\tB {x} \\"


def test_ignorable_destinations_dropped():
    rtf = "{\\rtf1{\\info{\\title Geheim}}{\\*\\unknowndest stuff}Sichtbar}"
    assert rtf_to_text(rtf) == "Sichtbar"


def test_garbage_and_broken_rtf_never_raise():
    for value in ["{\\rtf", "{\\rtf1 unbalanced {{{", "{\\rtf1}}}} trailing", "\\'zz", "{\\rtf1\\ansicpg99999 x}"]:
        out = rtf_to_text(value)
        assert out is None or isinstance(out, str)
