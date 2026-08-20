# AlphaPlan – Preise, Statistik & Dispo

> Teil der AlphaPlan-SQL-Referenz. Einstieg, Konventionen und Fallstricke: `alphaplan://start`.
> Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind markiert.

---

## Preisfindung

Vier Ebenen, aus denen sich ein Preis ergeben kann. **Die Auswahllogik nicht selbst
nachbauen** – sie steckt in `VerkaufspreisPrioritaeten` (35 Regelzeilen aus 25 Bit-Flags
mit `Sortierung`) und ist praktisch nicht reproduzierbar. Der tatsächlich verwendete
Preis steht immer in der Belegposition; die Preistabellen erklären nur, woher er kam.

### `EinkaufsPreise` (rund 450.000)

Lieferanten-Preisliste mit Mengenstaffeln.

| Spalte | Bedeutung |
|---|---|
| `ArtikelID` + `AdressenID` | Artikel und Lieferant |
| `Anzahl` | **Staffelmenge** – ab dieser Menge gilt der Preis |
| `EKNetto` / `VKNetto` | Einkaufs- und daraus abgeleiteter Verkaufspreis |
| `EffektivNetto` | EK inkl. `Nebenkosten_BE` und `Zoll_Proz` – der ehrliche Preis |
| `WaehrungenID`, `EinheitenID`, `PEEinheitenID` | Bezugsgrößen – vor jedem Vergleich prüfen |
| `GueltigVon` / `GueltigBis` | Zeitraum |
| `Lieferzeit` | Tage |
| `LosGroesse`, `AnzahlBEs`, `Teilbarkeit`, `BEProVPE_EK` | Verpackungslogik |
| `LieferKondition`, `VPE_EK_cbm` | Incoterm, Volumen |
| `AlsEPVerwenden`, `IstEmpfohlenerVerkaufspreis` | Steuerflags |

### `VerkaufsPreise` (rund 330.000)

| Spalte | Bedeutung |
|---|---|
| `ArtikelID` + `AdressenID` / `AdressgruppenID` / `AdressPreisGruppenID` / `ProjekteID` | Geltungsbereich – vier konkurrierende Zuordnungen |
| `Anzahl` | Staffelmenge |
| `PreisStufe` | `01` / `02` (siehe `Texte`) |
| `Netto`, `Brutto` | Preis |
| `HandelsSpanne`, `Aufschlag`, `Rohertrag`, `RohertragW` | Kalkulation |
| `GueltigVon` / `GueltigBis`, `Prioritaet`, `IgnoriereRest` | Auswahlsteuerung |
| `WaehrungenID`, `KursFaktor`, `KursDivisor` | eigene Währungsangabe je Preiszeile |

### `AdressSpezArtikelDaten` (rund 330.000)

Kundenspezifische Sicht auf einen Artikel – **die Brücke zwischen Kundenteilenummer und
eigener MPN**, und damit der Gegenpart zu `BelegePositionen.KundenArtikelNummer`.

`ArtikelID` + `AdressenID`/`AdressGruppenID`, dazu `ArtikelNummer` (die des Kunden),
`Bezeichnung`, `LangText`, `EANNummer`, `Preis`, `Suchbegriff`, `Kategorie`,
`K_Lieferplannummer`, `K_Marker`, `K_AngelegtAm`.

### Verknüpfung zum Beleg

`EKPreiseEinheitenAnzahlen` verbindet `BelegePositionenID` mit `EinkaufsPreiseID`
(und `SammelBestellungenID`) – darüber lässt sich nachvollziehen, welche Preiszeile eine
Bestellposition gezogen hat. `Handelsspannen` (2 Zeilen, schemastabil) hält
Grenze/Minimum/Maximum.

---

---

## Statistik & Kennzahlen

Alle Werte sind **vorberechnete Aggregate**, keine Sichten auf `Belege`. Sie können von
einer Live-Auswertung abweichen; welcher Job sie fortschreibt, ist unbekannt.

### `StatistikenArtikel` (rund 180.000) – Backend von `list_part_stats`

Granularität **Artikel × Einkauf/Verkauf × Jahr × Kalenderwoche**:

| Spalte | Bedeutung |
|---|---|
| `ArtikelID` | |
| `EinkaufVerkauf` | `E` = Einkauf, `V` = Verkauf (Parameter `sales_purchasing`) |
| `Jahr`, `KW` | **KW-Granularität, nicht Monat** |
| `Netto`, `Roh` | Umsatz und Rohertrag |
| `BEEanzahl` | Stückzahl (Schreibfehler im Spaltennamen ist echt) |
| `VorJahresStatistikenID`, `FolgeJahresStatistikenID` | Verkettung über Jahresgrenzen |

### `StatistikenMonat` (rund 1,3 Mio)

Monatsaggregat über mehrere Dimensionen gleichzeitig: `AdressenID`, `AdressGruppenID`,
`ArtikelID`, `WarenGruppenID`, `VertreterAdressenID`. Welche davon gefüllt sind, steuert
`StatistikTyp` *(Werte unbekannt)*. Kennzahlen: `VKNetto`, `VKRoh`, `VKBEAnzahl`,
`EKNetto`, `EKBEAnzahl` sowie Bestandswerte (`AnfangsBestand`, `EndBestand`,
`BestandsEP`, `EndBestandsEP`). Neben `Monat`/`Jahr` gibt es `GeschaeftsMonat`/
`GeschaeftsJahr` – bei abweichendem Geschäftsjahr die relevantere Achse.

### `ArtikelStatistiken` (rund 300.000, eine Zeile je Artikel)

Snapshot-Kennzahlen: `VKNetto`/`VKRoh`/`VKBEAnzahl` und `EKNetto` jeweils als
Ist, Vorjahr, `…BisAktTag`, `…BisAktMonat` und `…Gesamt`, dazu Schnitt-/Delta-Werte.
`Letzte*ID`-Spalten zeigen auf den jeweils letzten Beleg je Art (Angebot, Auftrag,
Rechnung, Warenzugang, RMA …) – bequemer Einstieg für „wann zuletzt“-Fragen.

**Bemerkenswert:** `Verfuegbar2_00` bis `Verfuegbar2_51` – eine 52-Wochen-Vorschau der
Verfügbarkeit je Artikel *(Semantik und Startwoche unverifiziert)*.

### Weitere

* `StatistikenArtikelBest` (rund 300.000) – Verbrauchsprognose je Artikel mit gewichteten
  Betrachtungszeiträumen und `PrognostizierterTagesBedarf`.
* `AdressenStatistiken` (rund 4.300) – dieselbe Snapshot-Logik je Adresse, inkl. `Letzte*ID`.
* `AdressenBelegSummen` (rund 4.300) – offene Summen je Belegart (`SummeAuftraege`,
  `SummeLieferscheine`, `SummeRechnungen`, `SummeWarenZugaenge`, `SummePositiv`,
  `SummeNegativ` …).
* `WarenGruppenStatistiken` (rund 300, schemastabil).

---

---

## Dispo & Reichweiten

Die Verfügbarkeitsvorschau: aus Bestand, offenen Bestellungen und offenen Aufträgen
wird je Artikel ein Verlauf über kommende Perioden berechnet. Zusammen im zweistelligen
Millionenbereich.

`ReichweitenTypen` steuert die Läufe – aktiv sind nur zwei:

| ID | Beschreibung | aktiv |
|---|---|---|
| 1 | Gruppiert nach Artikeln | ja |
| 2 | Gruppiert nach Artikeln, ohne Vorschläge | nein |
| 49 | Reichweiten Export | nein |
| 53 | ArtikelReichweiten | ja |

| Tabelle | Zeilen (~) | Inhalt |
|---|---|---|
| `Reichweiten` | ~2.000 | Kopf je `ReichweitenTypenID` + `ArtikelID` mit Ausgangs-`Bestand` |
| `ReichweitenArtikel` | ~300.000 | eine Zeile je Artikel mit `Verfuegbar0` … `Verfuegbar53` plus `VerfuegbarN` – **54 Perioden Vorschau** (Periodenlänge unbestimmt, vermutlich Wochen) |
| `ReichweitenBuchungen` | ~5 Mio | Einzelbuchungen: `ArtikelID`, `BelegeID`, `BelegePositionenID`, `LagerID`, `Anzahl`, `MengenfeldTyp`, `BuchungsDatum`, `SourceTyp`/`SourceID` |
| `ReichweitenGruppiert` | ~180.000 | strukturgleich, verdichtet |
| `ReichweitenVerfuegbar` | ~175.000 | Verfügbarkeit je Buchung |
| `ReichweitenAusgewertet` | ~3 Mio | Verarbeitungsstatus je Buchung und Typ |
| `ArtikelReichweitenTage` | ~3 Mio | Zeitraster (`Datum`, `Modus`, `Zeitraum`) |
| `ArtikelReichweitenBelege` | ~16 | Bestandsgrößen je Beleg zu einem Stichtag |

`ReichweitenBuchungen` ist die einzige Stelle, an der Bedarfe und Zugänge mit Datum und
Belegbezug nebeneinander stehen – die Grundlage für „wann reicht der Bestand nicht mehr".
`MengenfeldTyp` und `SourceTyp` sind nicht kartiert *(unverifiziert)*.

Verwandt: `StatistikenArtikelBest.PrognostizierterTagesBedarf` (Statistik) und
`ArtikelStatistiken.Verfuegbar2_00..51` – letzteres ist offenbar dieselbe Vorschau,
redundant gespeichert.

---
