"""Unit tests for the normalize_preview tool (pure string normalization preview)."""

import pytest

from alphaplan_mcp.tools.result_sets import normalize_preview, MAX_PREVIEW_VALUES


@pytest.mark.asyncio
async def test_preview_normalization_and_steps():
    out = await normalize_preview(["SMAJ6,0CA-H-HS", "SMAJ60CA-H-HS"])
    by_raw = {r["raw"]: r for r in out}
    assert by_raw["SMAJ6,0CA-H-HS"]["normalized"] == "SMAJ60CA-H-HS"
    assert by_raw["SMAJ6,0CA-H-HS"]["steps"] == ["strip"]
    assert by_raw["SMAJ60CA-H-HS"]["normalized"] == "SMAJ60CA-H-HS"
    assert by_raw["SMAJ60CA-H-HS"]["steps"] == []


@pytest.mark.asyncio
async def test_preview_handles_each_step():
    out = await normalize_preview(["fm-2150x", "FM\u20112150\u00a0X", "FM2150-M-HS "])
    by_raw = {r["raw"]: r for r in out}
    assert by_raw["fm-2150x"]["steps"] == ["uppercase"]
    assert by_raw["FM\u20112150\u00a0X"]["steps"] == [
        "trim_whitespace", "unicode_nfkc", "fold_dashes",
    ]
    assert by_raw["FM2150-M-HS "]["steps"] == ["trim_whitespace"]


@pytest.mark.asyncio
async def test_preview_preserves_non_strings():
    out = await normalize_preview([None, 42, ""])
    assert out[0] == {"raw": None, "normalized": None, "steps": []}
    assert out[1] == {"raw": 42, "normalized": 42, "steps": []}
    assert out[2] == {"raw": "", "normalized": "", "steps": []}


@pytest.mark.asyncio
async def test_preview_caps_input_length():
    out = await normalize_preview(["x"] * (MAX_PREVIEW_VALUES + 1))
    assert isinstance(out, dict)
    assert out["error"] == "INVALID_ARGUMENT"
