"""Tests that the AlphaPlan schema resources stay in sync with the folder."""

import re

import pytest

from alphaplan_mcp.alphaplan_resources import ALPHAPLAN_RESOURCES, RESOURCE_DIR


def test_alle_markdown_dateien_sind_registriert():
    dateien = {p.name for p in RESOURCE_DIR.glob("*.md")} - {"IMPLEMENTATION.md"}
    registriert = {eintrag[0] for eintrag in ALPHAPLAN_RESOURCES}
    assert dateien == registriert


def test_uris_sind_eindeutig():
    uris = [eintrag[1] for eintrag in ALPHAPLAN_RESOURCES]
    assert len(uris) == len(set(uris))


@pytest.mark.asyncio
async def test_resources_lesbar(mcp_server):
    for _, uri, _, _ in ALPHAPLAN_RESOURCES:
        inhalt = await mcp_server.read_resource(uri)
        assert inhalt, f"leere Resource: {uri}"


def test_querverweise_zeigen_auf_existierende_resources():
    bekannt = {eintrag[1] for eintrag in ALPHAPLAN_RESOURCES}
    for pfad in RESOURCE_DIR.glob("*.md"):
        for treffer in re.findall(r"alphaplan://[a-z-]+", pfad.read_text(encoding="utf-8")):
            assert treffer in bekannt, f"{pfad.name}: unbekannte URI {treffer}"