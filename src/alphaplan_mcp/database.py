# Manages SQL Server connections via pyodbc.
# Consumes application settings, enforces timeouts,
# and runs blocking calls asynchronously.
"""SQL Server connection and customer data fetching."""

import datetime
import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Any, AsyncGenerator, Callable, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa
import pyodbc

from .config import settings
from .utils import rtf_to_text

logger = logging.getLogger(__name__)

# Fixed query – no user‑supplied table or column names.
_CUSTOMER_QUERY = """
    SELECT
        NULLIF(LTRIM(RTRIM(A.AdressNummer)), '') AS adress_nummer,
        NULLIF(LTRIM(RTRIM(A.SuchName)), '') AS name_intern,
        NULLIF(LTRIM(RTRIM(A.SuchName)), '') AS name,
        NULLIF(LTRIM(RTRIM(A.Branche)), '') AS branche,
        NULLIF(LTRIM(RTRIM(A.K_KundenTypNeu)), '') AS kunden_typ,
        NULLIF(LTRIM(RTRIM(A.KundenTyp)), '') AS finale_klassifizierung,
        NULLIF(LTRIM(RTRIM(A.K_Vertriebskanal)), '') AS vertriebskanal,
        NULLIF(LTRIM(RTRIM(A.Kategorie)), '') AS potential_zu_kategorie,
        NULLIF(LTRIM(RTRIM(A.Beruf)), '') AS umsatz_level_vorjahr,
        NULLIF(LTRIM(RTRIM(A.K_VKID)), '') AS sachbearbeiter_innendienst,
        NULLIF(LTRIM(RTRIM(A.K_VKAD)), '') AS sachbearbeiter_aussendienst,
        NULLIF(LTRIM(RTRIM(A.K_AB)), '') AS sachbearbeiter_auftragsbearbeitung,
        NULLIF(LTRIM(RTRIM(ZB.BedingungsText)), '') AS zahlungsbedingung,
        NULLIF(LTRIM(RTRIM(W.WaehrungsKuerzel)), '') AS waehrung,
        NULLIF(LTRIM(RTRIM(A.PreisStufe1)), '') AS preisstufe_standard,
        NULLIF(LTRIM(RTRIM(A.PreisStufe2)), '') AS preisstufe_vorrangig,
        NULLIF(LTRIM(RTRIM(VB.BedingungsText)), '') AS versandbedingung,
        NULLIF(LTRIM(RTRIM(VA.Bezeichnung)), '') AS versandart,
        LA.lieferadresse,
        COALESCE(LA.lieferadressen_anzahl, 0) AS lieferadressen_anzahl
    FROM
        ALPHAPLAN_EchtDaten.dbo.Adressen A
        LEFT JOIN ALPHAPLAN_EchtDaten.dbo.ZahlungsBedingungen ZB
            ON ZB.ZahlungsBedingungenID = A.ZahlungsBedingungenID
        LEFT JOIN ALPHAPLAN_EchtDaten.dbo.Waehrungen W
            ON W.WaehrungenID = A.WaehrungenID
        LEFT JOIN ALPHAPLAN_EchtDaten.dbo.VersandBedingungen VB
            ON VB.VersandBedingungenID = A.VersandBedingungenID
        LEFT JOIN ALPHAPLAN_EchtDaten.dbo.VersandArten VA
            ON VA.VersandArtenID = A.VersandArtenID
        OUTER APPLY (
            -- Delivery addresses on file that differ from the main address; the standard one
            -- (else the oldest) is reported, the count tells whether there are more.
            SELECT TOP (1)
                STUFF(CONCAT(
                    ', ' + NULLIF(LTRIM(RTRIM(AN.Name1)), ''),
                    ', ' + NULLIF(LTRIM(RTRIM(AN.Name2)), ''),
                    ', ' + NULLIF(LTRIM(RTRIM(CONCAT(AN.Strasse, ' ', AN.HausNummer))), ''),
                    ', ' + NULLIF(LTRIM(RTRIM(CONCAT(AN.Postleitzahl, ' ', AN.Ort))), ''),
                    ', ' + NULLIF(LTRIM(RTRIM(LD.LaenderText)), '')
                ), 1, 2, '') AS lieferadresse,
                COUNT(*) OVER () AS lieferadressen_anzahl
            FROM ALPHAPLAN_EchtDaten.dbo.LieferAnschriften LAS
                JOIN ALPHAPLAN_EchtDaten.dbo.Anschriften AN
                    ON AN.AnschriftenID = LAS.AnschriftenID
                LEFT JOIN ALPHAPLAN_EchtDaten.dbo.Laender LD
                    ON LD.LaenderID = AN.LaenderID
            WHERE LAS.AdressenID = A.AdressenID
                AND LAS.IstArchiviert = 0
                AND LAS.AnschriftenID <> A.AnschriftenID
            ORDER BY LAS.IstStandard DESC, LAS.LieferAnschriftenID
        ) LA
    WHERE
        A.IstArchiviert = 0
        AND A.AdressenID > 0
        AND A.SuchName LIKE ?
    ORDER BY A.AdressNummer
"""

_CUSTOMER_COLUMN_DESCRIPTIONS = {
    "adress_nummer": "Address number (AdressNummer) of the customer or supplier, e.g. 'D010187'; the adress_nummer parameter of list_beleg_positionen.",
    "name_intern": "(Company Internal) customer name",
    "name": "customer name",
    "branche": "Industry sector of the customer.",
    "kunden_typ": "Customer type(s) (K_KundenTypNeu), comma-separated, e.g. 'Entwicklung, Fertigung', 'EMS-Dienstleister', 'Distributor', 'Handel'.",
    "finale_klassifizierung": "Final customer classification (KundenTyp), e.g. 'Finale Klassifizierung - 1 - KeyAccount'.",
    "vertriebskanal": "Sales channel(s) (K_Vertriebskanal), comma-separated, e.g. 'EDI', 'Plattform Luminovo', 'Webshop', 'API'. Mostly empty.",
    "potential_zu_kategorie": "Potential category letter (Kategorie): K (key account), A, B, C, D, 0; occasionally T or '?'.",
    "umsatz_level_vorjahr": "Revenue level of the previous year (Beruf): K (key account), A, B, C, D, 0 (no revenue).",
    "sachbearbeiter_innendienst": "Unique name abbreviation of the employee responsible for internal sales",
    "sachbearbeiter_aussendienst": "Unique name abbreviation of the employee responsible for field sales",
    "sachbearbeiter_auftragsbearbeitung": "Unique name abbreviation of the employee responsible for order processing",
    "zahlungsbedingung": "Payment terms (Zahlungsbedingung) from the customer master, e.g. 'until 30th of the following month net'.",
    "waehrung": "Default currency code of the customer (e.g. EUR, JPY, USD).",
    "preisstufe_standard": "Standard price level (Standard-Preisstufe, PreisStufe1), e.g. '01'.",
    "preisstufe_vorrangig": "Priority price level (vorrangige Preisstufe, PreisStufe2), e.g. '01'.",
    "versandbedingung": "Shipping terms / Incoterm text (Versandbedingung), e.g. 'DAP delivery address Incoterms 2020'.",
    "versandart": "Shipping method (Versandart), e.g. 'DHL International'.",
    "lieferadresse": "Delivery address if it differs from the main address (standard delivery address, else the first one on file), formatted as one line. Null when no different delivery address exists.",
    "lieferadressen_anzahl": "Number of active delivery addresses on file that differ from the main address. Greater than 1 means lieferadresse shows only one of several."
}

_PART_COLUMN_DESCRIPTIONS = {
    "ArtikelNummer": "Unique article/part number.",
    "RollengroesseZoll": "Roll size in inches.",
    "Beschreibung": "Combined description (short text + long text).",
    "MatchCode": "Search match code for the part.",
    "Status": "Supplier ability-to-supply indicator (Feld02). Mostly 'rot' (red = cannot supply), 'gelb' (yellow = limited), or 'gruen' (green = no problem). Other values are possible due to data-quality issues.",
    "VPE": "Sales unit (Verpackungseinheit).",
    "HauptLieferant": "Main supplier name.",
    "NachfolgeArtikel": "Successor article number (Feld03).",
    "Wiederbeschaffungszeit_Tage": "Replenishment time in days.",
    "WarenGruppe": "Product group description.",
    "WarenGruppenNummer": "Product group number."
}

_PART_STATS_COLUMN_DESCRIPTIONS = {
    "ArtikelNummer": "Unique article/part number.",
    "EinkaufVerkauf": "Sales or Purchasing indicator ('V' = Verkauf/Sales, 'E' = Einkauf/Purchasing).",
    "Jahr": "Year of the statistics.",
    "KW": "Calendar week (Kalenderwoche).",
    "Netto": "Net amount.",
    "Rohertrag": "Gross profit (Roh).",
    "Menge": "Quantity (BEEanzahl)."
}

_WEBSHOP_PRICE_COLUMN_DESCRIPTIONS = {
    "mpn": "Manufacturer part number.",
    "quantity_breakpoint": "Minimum quantity for this price tier.",
    "unit_price": "Unit price at the given quantity breakpoint.",
    "updated_at": "Timestamp when this price was last updated.",
}

@dataclass(frozen=True)
class LineItemColumn:
    """One output column of `list_beleg_positionen`.

    `beleg_typen` restricts the column to the given BelegTyp codes (None = every type).
    `joins` names entries of `_LINE_ITEM_JOINS` the SQL expression depends on; they are added
    to the FROM clause only when a selected column needs them. `type_descriptions` overrides
    `description` for individual BelegTypen when the field means something else there.
    `transform` post-processes each fetched value in Python (e.g. RTF -> plain text).
    """
    name: str
    sql: str
    description: str
    beleg_typen: Optional[FrozenSet[int]] = None
    joins: Tuple[str, ...] = ()
    type_descriptions: Mapping[int, str] = dataclass_field(default_factory=dict)
    transform: Optional[Callable[[Any], Any]] = None

    def applies_to(self, beleg_typ_id: int) -> bool:
        return self.beleg_typen is None or beleg_typ_id in self.beleg_typen

    def description_for(self, beleg_typ_id: int) -> str:
        return self.type_descriptions.get(beleg_typ_id, self.description)


# BelegTyp codes used for type-specific columns (see tools/documents.py DOCUMENT_TYPE_BELEG_TYP).
_AN = 2
_AB = 3
_EA = 8
_EB = 9
_ONLY_AB = frozenset({_AB})
_ONLY_EB = frozenset({_EB})
_ALL_EXCEPT_EB = frozenset({1, 2, 3, 4, 5, 8, 10})
_ALL_EXCEPT_AB_EB = frozenset({1, 2, 4, 5, 8, 10})


def _as_date(expr: str) -> str:
    """Return a datetime column as a plain date (time of day dropped); the 1900-01-01
    'no date' placeholder AlphaPlan stores is mapped to NULL."""
    return f"CASE WHEN {expr} < '19010101' THEN NULL ELSE CAST({expr} AS date) END"


def _trimmed_text(expr: str) -> str:
    """Trim surrounding blanks and map empty strings to NULL (for matching keys / free text)."""
    return f"NULLIF(LTRIM(RTRIM({expr})), '')"


# Optional joins, keyed by name. BP (BelegePositionen), B (Belege), W (Waehrungen) and
# A (Adressen, counterparty) are always joined. Always LEFT JOIN so rows are never dropped.
# Join SQL must not contain user input.
_LINE_ITEM_JOINS: Dict[str, str] = {
    "artikel": "LEFT JOIN ALPHAPLAN_EchtDaten.dbo.Artikel ART ON ART.ArtikelID = BP.ArtikelID",
    "lager_abbuchung": "LEFT JOIN ALPHAPLAN_EchtDaten.dbo.Lager LG ON LG.LagerID = BP.AbbuchungsLagerID",
    # needs "artikel" first; a column listing both joins gets them in this order
    "warengruppe": "LEFT JOIN ALPHAPLAN_EchtDaten.dbo.WarenGruppen WG ON WG.WarenGruppenID = ART.WarenGruppenID",
}

# Price per 1000 pieces in document currency, from the price-unit price; falls back to the
# per-piece price when the price unit is missing. Not rounded - the columns round it.
_PRO1000_EXPR = ("CASE WHEN BP.PEsProBE > 0"
                 " THEN BP.PENettoPreis * BP.PEsProBE * COALESCE(NULLIF(BP.BEsProVE, 0), 1) * 1000"
                 " ELSE BP.NettoPreisEinzel * 1000 END")
_EUR_FACTOR = "B.KursFaktor / NULLIF(B.KursDivisor, 0)"

# Column order here is the output column order. Add type-specific columns with `beleg_typen`
# (e.g. frozenset({3}) for AB only); they are silently omitted for all other document types.
LINE_ITEM_COLUMNS: Tuple[LineItemColumn, ...] = (
    LineItemColumn("ArtikelNummer", _trimmed_text("BP.ArtikelNummer"),
                   "Unique article/part number of the line item."),
    LineItemColumn("BelegNummer", "B.BelegNummer",
                   "Document number (prefix reflects the document type, e.g. 'ER250025')."),
    LineItemColumn("PositionsTyp", "BP.PositionsTyp",
                   "Line type: 0 = article line, 2 = text/comment line (no article, quantity and "
                   "price 0; the text is in ArtikelBezeichnung). Other values are rare."),
    LineItemColumn("KundenNummer", _trimmed_text("A.AdressNummer"),
                   "Counterparty address number (AdressNummer)."),
    LineItemColumn("KundeOderLieferant", _trimmed_text("A.SuchName"),
                   "Counterparty name (customer for outgoing documents, supplier for purchasing documents)."),
    LineItemColumn("KundeKlassifizierung", _trimmed_text("A.KundenTyp"),
                   "Counterparty classification code (KundenTyp)."),
    LineItemColumn("KundeBranche", _trimmed_text("A.Branche"),
                   "Counterparty industry sector (Branche)."),
    LineItemColumn("Menge", "BP.VEAnzahl",
                   "Quantity (VEAnzahl / packaging-unit count)."),
    LineItemColumn("MengeOffen", "BP.NochZuUebernehmen",
                   "Open/remaining quantity still to be taken over (NochZuUebernehmen). Only "
                   "meaningful for AA, AB and EB; for end documents it stays at the full quantity.",
                   type_descriptions={_EB: "Open quantity of the purchase order still to be "
                                           "received (NochZuUebernehmen), as in AlphaPlan F10."}),
    LineItemColumn("NettoPreisStueckEUR",
                   f"ROUND(BP.NettoPreisEinzelX * {_EUR_FACTOR}, 8)",
                   "Net unit price per piece converted to EUR with the document's exchange rate. "
                   "For small prices prefer NettoPreisPro1000EUR (more significant digits)."),
    LineItemColumn("NettoPreisStueck", "ROUND(BP.NettoPreisEinzel, 8)",
                   "Net unit price per piece in the document currency (BelegWaehrung), unconverted "
                   "(NettoPreisEinzel). Prefer NettoPreisPro1000 for exact comparisons."),
    LineItemColumn("NettoPreisPro1000", f"ROUND({_PRO1000_EXPR}, 6)",
                   "Net price per 1000 pieces in the document currency (BelegWaehrung), unconverted. "
                   "Calculated from the price-unit price (PENettoPreis) and the price unit, so it is "
                   "per 1000 regardless of whether the line is priced '/1000', '/100' or per piece ('x')."),
    LineItemColumn("NettoPreisPro1000EUR", f"ROUND({_PRO1000_EXPR} * {_EUR_FACTOR}, 6)",
                   "Net price per 1000 pieces converted to EUR with the document's exchange rate "
                   "(NettoPreisPro1000 x BelegKursFaktor, unrounded factor). Best column for comparing "
                   "purchase prices across currencies."),
    LineItemColumn("Preiseinheit", _trimmed_text("BP.PEText"),
                   "Price unit of the line as shown in AlphaPlan (PEText): usually '/1000', rarely "
                   "'/100'; 'x' means priced per piece."),
    LineItemColumn("BelegDatum", _as_date("B.Datum"),
                   "Date of the document (B.Datum, date only). One of the date fields the range filter matches on."),
    LineItemColumn("IstArchiviert", "B.IstArchiviert",
                   "Whether the document is archived (1 = archived, 0 = not archived)."),
    LineItemColumn("LieferDatum", _as_date("BP.LieferDatum"),
                   "Delivery date (LieferDatum). The 1900-01-01 'no date' placeholder is returned as null."
                   " 7777-01-01 and 9999-xx-xx are deliberate team placeholders, passed through as-is.",
                   beleg_typen=_ALL_EXCEPT_EB),
    LineItemColumn("LieferDatumBestaetigt", _as_date("BP.AnlieferungsDatumBestaetigt"),
                   "Confirmed delivery date (AnlieferungsDatumBestaetigt); purchasing-side field, "
                   "usually empty for sales documents. The 1900-01-01 placeholder is returned as null."
                   " 7777-01-01 and 9999-xx-xx are deliberate team placeholders, passed through as-is.",
                   beleg_typen=_ALL_EXCEPT_EB),
    LineItemColumn("OriginalDatum", _as_date("BP.AnlieferungsDatumWunsch"),
                   "Original/requested delivery date (AnlieferungsDatumWunsch). The 1900-01-01 "
                   "placeholder is returned as null."
                   " 7777-01-01 and 9999-xx-xx are deliberate team placeholders, passed through as-is.",
                   beleg_typen=_ALL_EXCEPT_AB_EB),
    LineItemColumn("RequestedDate", _as_date("BP.LieferDatum"),
                   "Requested delivery date of the purchase order line (LieferDatum). "
                   "The 1900-01-01 'no date' placeholder is returned as null."
                   " 7777-01-01 and 9999-xx-xx are deliberate team placeholders, passed through as-is.",
                   beleg_typen=_ONLY_EB),
    LineItemColumn("BestaetigtZum", _as_date("BP.AnlieferungsDatumBestaetigt"),
                   "Delivery date confirmed by the supplier (AnlieferungsDatumBestaetigt), as in "
                   "AlphaPlan F10. Often empty; the 1900-01-01 placeholder is returned as null."
                   " 7777-01-01 and 9999-xx-xx are deliberate team placeholders, passed through as-is.",
                   beleg_typen=_ONLY_EB),
    LineItemColumn("KDWunsch", _as_date("BP.AnlieferungsDatumWunsch"),
                   "Customer's requested delivery date (Kundenwunschtermin, AnlieferungsDatumWunsch). "
                   "A 31.12. date usually marks a call-off collective position, not a real date; "
                   "the 1900-01-01 placeholder is returned as null."
                   " 7777-01-01 and 9999-xx-xx are deliberate team placeholders, passed through as-is.",
                   beleg_typen=_ONLY_AB),
    LineItemColumn("BelegKursFaktor", f"ROUND({_EUR_FACTOR}, 10)",
                   "Exchange-rate factor of the document (KursFaktor / KursDivisor): document "
                   "currency x factor = EUR."),
    LineItemColumn("BelegWaehrung", _trimmed_text("W.WaehrungsKuerzel"),
                   "ISO currency code of the document (e.g. JPY)."),
    LineItemColumn("Titel", _trimmed_text("B.BelegTitel"),
                   "Document title (BelegTitel). For purchasing documents usually an order type, "
                   "for quotes (AN) the end customer.",
                   type_descriptions={_EA: "Document title (BelegTitel). For requests to HolyStone / "
                                           "Walsin this holds the end customer, which separates "
                                           "project-specific from general purchase prices; for other "
                                           "suppliers often a price-list or request name.",
                                      _AN: "Document title (BelegTitel): the end customer of the quote.",
                                      _AB: "Document title (BelegTitel), e.g. 'EDI' for orders received "
                                           "via EDI; often empty."}),
    LineItemColumn("Kommission", _trimmed_text("B.Kommission"),
                   "Commission / reference of the document (Kommission), free text."),
    LineItemColumn("ZusatzText1", "BP.ZusatzText1",
                   "Additional text 1 of the line as plain text (RTF formatting removed). Free text "
                   "whose meaning depends on the document type and the person entering it; for CCTC "
                   "supplier lines it holds the landed cost (e.g. 'Landed Cost Price: 13,47 $ / "
                   "1000'). May contain unrelated notes or garbage - do not assume a fixed format.",
                   type_descriptions={_AN: "Quoted lead time of the quote line (ZusatzText1), plain "
                                           "text; free text without a fixed format."},
                   transform=rtf_to_text),
    LineItemColumn("ZusatzText2", "BP.ZusatzText2",
                   "Additional text 2 of the line as plain text (RTF formatting removed). Free text "
                   "with no fixed format; may contain unrelated notes or garbage.",
                   type_descriptions={_AN: "Quote status (ZusatzText2), e.g. 'nominiert', 'zu teuer', "
                                           "'second source (keine Beauftragung)', 'kein Feedback', "
                                           "'kein FUP', 'technisch passend', 'technisch nicht passend'."},
                   transform=rtf_to_text),
    LineItemColumn("ArtikelBezeichnung", "BP.ArtikelBezeichnung",
                   "Line description as plain text (RTF formatting removed). For text/comment lines "
                   "(PositionsTyp 2) this is the comment itself; for article lines the article "
                   "description, sometimes the MPN for imported lines.",
                   transform=rtf_to_text),
    LineItemColumn("WarenGruppe", _trimmed_text("WG.Bezeichnung"),
                   "Article group of the line's article (WarenGruppen.Bezeichnung). Null for lines "
                   "without an article.", joins=("artikel", "warengruppe")),
    LineItemColumn("WarenGruppenNummer", _trimmed_text("WG.WarenGruppenNummer"),
                   "Article group number. Groups starting with 'RMC_' are internal non-part items "
                   "(freight, packaging, services, catalogues, giveaways ...); they are left out of "
                   "price_summary.", joins=("artikel", "warengruppe")),
    LineItemColumn("IhrAuftrag", _trimmed_text("B.IhrAuftrag"),
                   "Customer's purchase order number (IhrAuftrag). Together with IhrAuftragPos the "
                   "key for matching real orders against a customer backlog. Forecast lines "
                   "(AuftragsTyp 'ForeCast') all carry 'Forecast' here - match those via "
                   "KundenArtNr + KDWunsch.", beleg_typen=_ONLY_AB),
    LineItemColumn("IhrAuftragPos", _trimmed_text("BP.PositionsText"),
                   "Customer's purchase order line/position (PositionsText, e.g. '000010'); 'FC' "
                   "on forecast lines.",
                   beleg_typen=_ONLY_AB),
    LineItemColumn("KundenArtNr", _trimmed_text("BP.KundenArtikelNummer"),
                   "Customer part number (CPN, KundenArtikelNummer).", beleg_typen=_ONLY_AB),
    LineItemColumn("Bestand", "LG.Bestand",
                   "Current stock level of the article in the line's issuing warehouse "
                   "(Lager.Bestand via AbbuchungsLagerID). Current value, not historic.",
                   beleg_typen=_ONLY_AB, joins=("lager_abbuchung",)),
    LineItemColumn("Status", _trimmed_text("ART.Feld02"),
                   "Article's supplier ability-to-supply status (Artikel.Feld02): mostly 'rot' / "
                   "'gelb' / 'grün'; free text, other values occur.",
                   beleg_typen=_ONLY_AB, joins=("artikel",)),
    LineItemColumn("AuftragsTyp", _trimmed_text("B.AuftragsTyp"),
                   "Order type of the document (AuftragsTyp), e.g. 'Rahmen', 'Lieferplan', 'Konsi', "
                   "'ForeCast', 'Einzelbestellung'.", beleg_typen=_ONLY_AB),
)


def line_item_columns_for(
    beleg_typ_id: int,
    columns: Sequence[LineItemColumn] = LINE_ITEM_COLUMNS,
) -> List[LineItemColumn]:
    """Return the columns `list_beleg_positionen` emits for one BelegTyp, in output order."""
    return [col for col in columns if col.applies_to(beleg_typ_id)]


@dataclass(frozen=True)
class LineItemQuery:
    sql: str
    params: List[Any]
    columns: List[LineItemColumn]


def build_line_items_query(
    beleg_typ_id: int,
    *,
    artikel_nummer: Optional[str] = None,
    adress_nummer: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_archived: bool = False,
    open_only: bool = False,
    include_text_lines: bool = True,
    columns: Sequence[LineItemColumn] = LINE_ITEM_COLUMNS,
    joins: Mapping[str, str] = _LINE_ITEM_JOINS,
) -> LineItemQuery:
    """Build the line-item SQL for one BelegTyp with its parameters and selected columns.

    Only fixed, code-defined fragments are interpolated; every user value is a `?` parameter.
    Filters are ANDed: `artikel_nummer` (exact), `adress_nummer` (exact AdressNummer of the
    counterparty), the date range (a row matches when ANY of the four date fields falls on a day
    from `date_from` through `date_to`, whole days inclusive; both bounds or neither), `open_only` (NochZuUebernehmen > 0), the
    archive flag and, unless `include_text_lines`, only article lines (PositionsTyp = 0).
    """
    if (date_from is None) != (date_to is None):
        raise ValueError("date_from and date_to must be given together")

    selected = line_item_columns_for(beleg_typ_id, columns)
    select_list = ",\n                    ".join(f"{col.sql} AS {col.name}" for col in selected)

    join_names: List[str] = []
    for col in selected:
        for name in col.joins:
            if name not in join_names:
                join_names.append(name)
    extra_joins = "".join(f"\n                    {joins[name]}" for name in join_names)

    predicates: List[str] = []
    params: List[Any] = [beleg_typ_id]
    if not include_text_lines:
        predicates.append("BP.PositionsTyp = 0")
    if not include_archived:
        predicates.append("B.IstArchiviert = 0")
    if artikel_nummer is not None:
        predicates.append("BP.ArtikelNummer = ?")
        params.append(artikel_nummer)
    if adress_nummer is not None:
        predicates.append("A.AdressNummer = ?")
        params.append(adress_nummer)
    if open_only:
        predicates.append("BP.NochZuUebernehmen > 0")
    if date_from is not None:
        # Whole days: the columns carry a time of day (B.Datum e.g. 21:45), so the upper bound
        # is "before the day after date_to"; written this way the predicates stay sargable.
        day_range = "({c} >= CAST(? AS date) AND {c} < DATEADD(day, 1, CAST(? AS date)))"
        predicates.append("(\n                        " + "\n                     OR ".join(
            day_range.format(c=c) for c in (
                "BP.LieferDatum", "BP.AnlieferungsDatumBestaetigt",
                "BP.AnlieferungsDatumWunsch", "B.Datum",
            )
        ) + "\n                    )")
        params.extend([date_from, date_to] * 4)
    where = "\n                    AND ".join(predicates) or "1 = 1"

    sql = f"""
                SELECT
                    {select_list}
                FROM ALPHAPLAN_EchtDaten.dbo.BelegePositionen BP
                    JOIN ALPHAPLAN_EchtDaten.dbo.Belege B
                        ON BP.BelegeID = B.BelegeID AND B.BelegTyp = ?
                    JOIN ALPHAPLAN_EchtDaten.dbo.Waehrungen W
                        ON W.WaehrungenID = B.WaehrungenID
                    JOIN ALPHAPLAN_EchtDaten.dbo.Adressen A
                        ON A.AdressenID = B.AuftragsAdressenID{extra_joins}
                WHERE {where}
                ORDER BY BP.ArtikelNummer, B.Datum ASC, B.BelegNummer, BP.ZeilenPosition
            """
    return LineItemQuery(sql=sql, params=params, columns=selected)


# pyodbc cursor.description type codes -> Arrow types. Declaring the schema up front keeps
# every chunk of a result set on the same schema even when a column is entirely NULL in one
# chunk (inference would yield `null` there and break concatenation on load).
_PYODBC_ARROW_TYPES = {
    str: pa.string(),
    float: pa.float64(),
    int: pa.int64(),
    bool: pa.bool_(),
    datetime.datetime: pa.timestamp("us"),
    datetime.date: pa.date32(),
}


def _arrow_schema(
    cursor_description,
    descriptions: Mapping[str, str],
) -> Optional[pa.Schema]:
    """Arrow schema (with description metadata) for a cursor, or None if a type is unknown."""
    fields = []
    for name, type_code, *_ in cursor_description:
        arrow_type = _PYODBC_ARROW_TYPES.get(type_code)
        if arrow_type is None:
            return None
        desc = descriptions.get(name)
        metadata = {b"description": desc.encode("utf-8")} if desc else None
        fields.append(pa.field(name, arrow_type, metadata=metadata))
    return pa.schema(fields)


def build_connection() -> pyodbc.Connection:
    """Create a new pyodbc connection using application settings."""
    conn_str = (
        f"DRIVER={{{settings.SQLSERVER_DRIVER}}};"
        f"SERVER={settings.SQLSERVER_HOST};"
        f"DATABASE={settings.SQLSERVER_DATABASE};"
        f"UID={settings.SQLSERVER_USER};"
        f"PWD={settings.SQLSERVER_PASSWORD};"
        f"TrustServerCertificate={settings.SQLSERVER_TRUST_CERTIFICATE};"
        f"ApplicationIntent=ReadOnly;"
        f"Encrypt=yes;"
        f"Connection Timeout={settings.SQLSERVER_QUERY_TIMEOUT_SECONDS};"
    )
    return pyodbc.connect(conn_str, autocommit=True, timeout=settings.SQLSERVER_QUERY_TIMEOUT_SECONDS)


async def fetch_customers_stream(
    customer_name: Optional[str] = None,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield customer rows as Arrow record batches.

    Runs the blocking ODBC calls inside a thread.
    """
    import asyncio
    from asyncio import Queue

    queue: Queue[Optional[pa.RecordBatch]] = asyncio.Queue()

    def _fetch():
        conn = None
        cursor = None
        try:
            conn = build_connection()
            cursor = conn.cursor()
            pattern = customer_name if customer_name else "%"
            cursor.execute(_CUSTOMER_QUERY, (pattern,))

            columns = [col[0] for col in cursor.description]
            schema = _arrow_schema(cursor.description, _CUSTOMER_COLUMN_DESCRIPTIONS)
            fetched_any = False
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    if not fetched_any and schema is not None:
                        # no matches: an empty batch keeps the columns of the stored result set
                        queue.put_nowait(pa.RecordBatch.from_pylist([], schema=schema))
                    break
                fetched_any = True
                dicts = [dict(zip(columns, row)) for row in rows]
                if schema is not None:
                    # declared types: an all-null column (e.g. lieferadresse) stays a string
                    queue.put_nowait(pa.RecordBatch.from_pylist(dicts, schema=schema))
                    continue
                batch = pa.RecordBatch.from_pylist(dicts)
                # Attach column descriptions to the schema metadata
                new_fields = []
                for field in batch.schema:
                    desc = _CUSTOMER_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                columns = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(columns, schema=new_schema)
                queue.put_nowait(batch)
        except Exception as e:
            logger.exception("Error fetching customers")
            queue.put_nowait(None)  # signal error
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)  # signal end

    import asyncio
    loop = asyncio.get_running_loop()
    # Run the blocking function in a thread
    task = loop.run_in_executor(None, _fetch)

    # Yield batches as they become available
    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task  # propagate any exception


async def fetch_webshop_prices_stream(
    mpns: List[str],
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield webshop price rows for multiple MPNs as Arrow record batches.
    """
    import asyncio
    from asyncio import Queue

    queue: Queue[Optional[pa.RecordBatch]] = asyncio.Queue()

    def _fetch():
        conn = None
        cursor = None
        try:
            conn = build_connection()
            cursor = conn.cursor()
            placeholders = ",".join("?" for _ in mpns)
            query = f"""
                SELECT mpn, quantity_breakpoint, unit_price, updated_at
                FROM [ApiPreise].[dbo].[WebshopPreise]
                WHERE mpn IN ({placeholders})
                ORDER BY mpn, quantity_breakpoint ASC
            """
            cursor.execute(query, mpns)

            columns = [col[0] for col in cursor.description]
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    break
                dicts = [dict(zip(columns, row)) for row in rows]
                batch = pa.RecordBatch.from_pylist(dicts)
                # Attach column descriptions
                new_fields = []
                for field in batch.schema:
                    desc = _WEBSHOP_PRICE_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                cols = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(cols, schema=new_schema)
                queue.put_nowait(batch)
        except Exception:
            logger.exception("Error fetching webshop prices")
            queue.put_nowait(None)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task


async def fetch_line_items_stream(
    beleg_typ_id: int,
    artikel_nummer: Optional[str] = None,
    adress_nummer: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    include_archived: bool = False,
    open_only: bool = False,
    include_text_lines: bool = True,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield line items of a single BelegTyp as Arrow record batches.

    Rows come from documents of one BelegTyp (resolved from the tool's `beleg_typ` code,
    e.g. 'AR' -> 5, 'EB' -> 9, 'ER' -> 10). The selected columns depend on the BelegTyp (see
    `LINE_ITEM_COLUMNS`); filters are described in `build_line_items_query`. Column
    `transform`s (e.g. RTF -> plain text) are applied before a batch is built.
    """
    import asyncio
    from asyncio import Queue

    queue: Queue[Optional[pa.RecordBatch]] = asyncio.Queue()
    query = build_line_items_query(
        beleg_typ_id,
        artikel_nummer=artikel_nummer,
        adress_nummer=adress_nummer,
        date_from=date_from,
        date_to=date_to,
        include_archived=include_archived,
        open_only=open_only,
        include_text_lines=include_text_lines,
    )
    descriptions = {col.name: col.description_for(beleg_typ_id) for col in query.columns}
    transforms = [(col.name, col.transform) for col in query.columns if col.transform]

    def _fetch():
        conn = None
        cursor = None
        try:
            conn = build_connection()
            cursor = conn.cursor()
            cursor.execute(query.sql, query.params)

            columns = [col[0] for col in cursor.description]
            schema = _arrow_schema(cursor.description, descriptions)
            fetched_any = False
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    if not fetched_any and schema is not None:
                        # no matches: an empty batch keeps the schema, so the stored empty
                        # result set still has its columns for follow-up queries
                        queue.put_nowait(pa.RecordBatch.from_pylist([], schema=schema))
                    break
                fetched_any = True
                dicts = [dict(zip(columns, row)) for row in rows]
                for name, transform in transforms:
                    for d in dicts:
                        d[name] = transform(d[name])
                if schema is not None:
                    batch = pa.RecordBatch.from_pylist(dicts, schema=schema)
                else:
                    batch = pa.RecordBatch.from_pylist(dicts)
                    new_fields = []
                    for field in batch.schema:
                        desc = descriptions.get(field.name)
                        new_fields.append(field.with_metadata({b"description": desc.encode("utf-8")}) if desc else field)
                    batch = pa.RecordBatch.from_arrays(batch.columns, schema=pa.schema(new_fields))
                queue.put_nowait(batch)
        except Exception:
            logger.exception("Error fetching line items")
            queue.put_nowait(None)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task


async def fetch_part_stats_stream(
    mpn: Optional[str] = None,
    year: int = None,
    sales_purchasing: Optional[str] = None,
    limit: Optional[int] = None,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield part statistics rows as Arrow record batches.
    """
    import asyncio
    from asyncio import Queue

    queue: Queue[Optional[pa.RecordBatch]] = asyncio.Queue()

    def _fetch():
        conn = None
        cursor = None
        try:
            conn = build_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT
                    A.ArtikelNummer,
                    EinkaufVerkauf,
                    Jahr,
                    KW,
                    Netto,
                    Roh AS Rohertrag,
                    BEEanzahl AS Menge
                FROM [ALPHAPLAN_EchtDaten].[dbo].[StatistikenArtikel]
                JOIN dbo.Artikel A ON A.ArtikelID = StatistikenArtikel.ArtikelID
                WHERE A.ArtikelID > 0
            """
            params = []
            
            if year is not None:
                query += " AND Jahr = ?"
                params.append(year)
            
            if sales_purchasing == 'E':
                query += " AND EinkaufVerkauf = 'E'"
            elif sales_purchasing == 'V':
                query += " AND EinkaufVerkauf = 'V'"
            else:
                query += " AND (EinkaufVerkauf = 'E' OR EinkaufVerkauf = 'V')"
                
            if mpn:
                query += " AND A.ArtikelNummer LIKE ?"
                params.append(mpn)
                
            query += " ORDER BY ArtikelNummer, KW"
            
            cursor.execute(query, params)

            columns = [col[0] for col in cursor.description]
            rows_fetched = 0
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    break
                dicts = [dict(zip(columns, row)) for row in rows]
                batch = pa.RecordBatch.from_pylist(dicts)
                # Attach column descriptions to the schema metadata
                new_fields = []
                for field in batch.schema:
                    desc = _PART_STATS_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                cols = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(cols, schema=new_schema)
                queue.put_nowait(batch)
                
                if limit is not None:
                    rows_fetched += len(rows)
                    if rows_fetched >= limit:
                        break
        except Exception as e:
            logger.exception("Error fetching part stats")
            queue.put_nowait(None)
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task


async def fetch_parts_stream(
    positive_filter_term: Optional[str] = None,
    negative_filter_term: Optional[str] = "TCC%",
    match_code: Optional[str] = None,
    sample_n: Optional[int] = None,
    include_archived: Optional[bool] = False,
) -> AsyncGenerator[pa.RecordBatch, None]:
    """
    Asynchronously yield part rows as Arrow record batches.
    
    Supports optional positive/negative filtering on ArtikelNummer and match code search.
    When `sample_n` is set, the sample is taken *after* all other filters are applied, returning
    up to `sample_n` random rows from the filtered population.

    By default, archived articles (`A.IstArchiviert = 1`) are excluded. Pass
    `include_archived=True` to include them as well.
    """
    import asyncio
    from asyncio import Queue

    queue: Queue[Optional[pa.RecordBatch]] = asyncio.Queue()

    def _fetch():
        conn = None
        cursor = None
        try:
            conn = build_connection()
            cursor = conn.cursor()
            
            columns_sql = """
                SELECT 
                    A.ArtikelNummer, 
                    A.Groesse AS RollengroesseZoll,
                    A.Bezeichnung + ' ' + A.LangText AS Beschreibung,
                    A.MatchCode AS MatchCode,
                    A.Feld02 AS [Status],
                    A.K_VPE AS VPE,
                    L.SuchName AS HauptLieferant,
                    A.Feld03 AS NachfolgeArtikel,
                    A.Wiederbeschaffungszeit AS Wiederbeschaffungszeit_Tage,
                    WG.Bezeichnung AS WarenGruppe,
                    WG.WarenGruppenNummer AS WarenGruppenNummer
                FROM ALPHAPLAN_EchtDaten.dbo.Artikel A WITH (NOLOCK)
                JOIN ALPHAPLAN_EchtDaten.dbo.Adressen L WITH (NOLOCK) ON L.AdressenID = A.HauptLieferantenAdressenID
                JOIN ALPHAPLAN_EchtDaten.dbo.WarenGruppen WG WITH (NOLOCK) ON WG.WarenGruppenID = A.WarenGruppenID
            """

            # Build the WHERE clauses (equivalent to `inner_where` below). We do not use
            # TABLESAMPLE: it samples the physical table *before* filters, i.e. from the wrong
            # population. Instead we filter first (inner subquery), then take up to sample_n random
            # rows from the filtered result via `ORDER BY NEWID()`.
            params = []
            archived_predicate = "" if include_archived else "A.IstArchiviert = 0 AND "
            where = (
                f"WHERE {archived_predicate}A.ArtikelID > 0"
                " AND WG.Bezeichnung IS NOT NULL AND LTRIM(RTRIM(WG.Bezeichnung)) <> ''"
            )

            if positive_filter_term:
                where += " AND A.ArtikelNummer LIKE ?"
                params.append(positive_filter_term)

            if negative_filter_term:
                where += " AND A.ArtikelNummer NOT LIKE ?"
                params.append(negative_filter_term)

            if match_code:
                where += " AND A.MatchCode LIKE ?"
                params.append(f"%{match_code}%")

            if sample_n is not None:
                inner = f"{columns_sql}\n{where}"
                query = f"SELECT TOP (?) * FROM (\n{inner}\n) t ORDER BY NEWID()"
                params.insert(0, sample_n)
            else:
                query = f"{columns_sql}\n{where}\nORDER BY A.ArtikelNummer ASC"

            cursor.execute(query, params)

            columns = [col[0] for col in cursor.description]
            while True:
                rows = cursor.fetchmany(settings.RESULTSET_CHUNK_ROWS)
                if not rows:
                    break
                dicts = [dict(zip(columns, row)) for row in rows]
                batch = pa.RecordBatch.from_pylist(dicts)
                # Attach column descriptions to the schema metadata
                new_fields = []
                for field in batch.schema:
                    desc = _PART_COLUMN_DESCRIPTIONS.get(field.name)
                    if desc:
                        metadata = {**field.metadata, b"description": desc.encode("utf-8")} if field.metadata else {b"description": desc.encode("utf-8")}
                        new_fields.append(field.with_metadata(metadata))
                    else:
                        new_fields.append(field)
                new_schema = pa.schema(new_fields)
                cols = [batch.column(i) for i in range(batch.num_columns)]
                batch = pa.RecordBatch.from_arrays(cols, schema=new_schema)
                queue.put_nowait(batch)
        except Exception as e:
            logger.exception("Error fetching parts")
            queue.put_nowait(None)  # signal error
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        queue.put_nowait(None)  # signal end

    loop = asyncio.get_running_loop()
    task = loop.run_in_executor(None, _fetch)

    while True:
        batch = await queue.get()
        if batch is None:
            break
        yield batch
    await task  # propagate any exception
