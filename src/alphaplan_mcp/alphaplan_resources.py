# Registers the AlphaPlan schema documentation as FastMCP resources.
# Data-driven from a single constant so that file layout and registration
# cannot drift apart.
from pathlib import Path

from fastmcp.resources import FileResource

RESOURCE_DIR = Path(__file__).parent / "resources" / "alphaplan"

ALPHAPLAN_RESOURCES = [
    (
        "00-start.md",
        "alphaplan://start",
        "AlphaPlan: Start hier",
        "Pflichtlektüre vor jeder Query gegen die AlphaPlan-Datenbank. "
        "Konventionen, alle Belegtypen mit Kürzeln, Join-Pfade und die "
        "Fallstricke, die sonst stille Fehler verursachen.",
    ),
    (
        "01-belege.md",
        "alphaplan://belege",
        "AlphaPlan: Aufträge, Angebote, Bestellungen, Rechnungen",
        "Belegköpfe und Positionen: Kundenaufträge, Angebote, "
        "Lieferantenbestellungen, Lieferscheine, Rechnungen. Mengen, offene "
        "Restmengen, Liefertermine, Preise und die Belegkette.",
    ),
    (
        "02-stammdaten.md",
        "alphaplan://stammdaten",
        "AlphaPlan: Artikel, Kunden, Lieferanten, Lagerbestand",
        "Artikelstamm inklusive MPN, Verpackungseinheit, EOL und "
        "Lieferfähigkeit; Adressen und ihre Klassifikation; Lagerorte und "
        "Bestände; Länder und Währungen; Auflösung von Codes und Wertelisten.",
    ),
    (
        "03-preise-und-zahlen.md",
        "alphaplan://preise-und-zahlen",
        "AlphaPlan: Preise, Umsatzzahlen, Verfügbarkeitsvorschau",
        "Einkaufs- und Verkaufspreise mit Mengenstaffeln, kundenspezifische "
        "Teilenummern, vorberechnete Umsatz- und Absatzstatistik je Artikel, "
        "Kunde und Monat, sowie Reichweiten- und Bedarfsvorschau.",
    ),
    (
        "04-historie.md",
        "alphaplan://historie",
        "AlphaPlan: Belegherkunft und Änderungshistorie",
        "Woher ein Beleg kam (EDI, Webshop, manuell) und wer wann welches "
        "Feld geändert hat. Änderungsprotokoll und Lagerbewegungsprotokoll.",
    ),
    (
        "05-sql-muster.md",
        "alphaplan://sql-muster",
        "AlphaPlan: SQL-Muster und Performance-Regeln",
        "Erprobte Queries für Angebotsauswertung, offene Wareneingänge, "
        "Lagerbestand und Währungsumrechnung. Dazu die Indexlage – hier "
        "nachsehen, bevor eine Query über Millionen Zeilen läuft.",
    ),
    (
        "06-wissensluecken.md",
        "alphaplan://wissensluecken",
        "AlphaPlan: Bekannte Wissenslücken",
        "Felder mit ungeklärter Bedeutung und noch nicht erschlossene "
        "Bereiche der Datenbank. Hier nachsehen, wenn eine Frage ins Leere "
        "läuft, statt zu raten.",
    ),
]


def register_alphaplan_resources(mcp) -> None:
    """Registriert die AlphaPlan-Schemadokumentation als MCP-Resources."""
    for filename, uri, name, description in ALPHAPLAN_RESOURCES:
        path = (RESOURCE_DIR / filename).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"AlphaPlan-Resource fehlt: {path}")
        mcp.add_resource(
            FileResource(
                uri=uri,
                path=path,
                name=name,
                description=description,
                mime_type="text/markdown",
                tags={"alphaplan", "schema"},
            )
        )