# AlphaPlan – Belegmodell

> Teil der AlphaPlan-SQL-Referenz. Einstieg, Konventionen und Fallstricke: `alphaplan://start`.
> Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind markiert.

---

## Das Belegmodell

Alle Belegarten teilen sich **einen** Kopf- und **einen** Positionsstamm:

* `Belege` (~1,04 Mio Zeilen) – Kopf aller Belegtypen, 255 Spalten
* `BelegePositionen` (~10,8 Mio Zeilen) – Positionen aller Belegtypen, 192 Spalten

Daneben trägt **fast jeder Belegtyp eine gleichnamige Satellitentabelle** mit
typspezifischen Zusatzfeldern. **Es sind 1:1-Erweiterungen, keine Alternativen:** Kopf-
Satelliten hängen an `BelegeID`, Positions-Satelliten an `BelegePositionenID`. Positionen
liegen **immer** in `BelegePositionen`.

| Typ | Kopf-Satellit | wofür er sich lohnt |
|---|---|---|
| 2 AN | `Angebote` | Vertriebs-Funnel: `Chance`, `GueltigBis`, `KontaktGehabtAm`, `VerhandlungDurchgefuehrtAm`, `EntscheidungGetroffenAm`, `HatFolgeAngebot`, `K_IstFreigegeben`, `K_AbgabeDatumZiel` |
| 3 AB | `Auftraege` | Daueraufträge (`DA*`: Intervall, `DANaechsteBerechnung`, `DAAutoVerlaengerung`), `SVStatus`/`SVPrioritaet`, `K_EDI_*` |
| 9 EB | `Bestellungen` | `IstAutoBestellung`, `MussEKMahnungenPruefen`, `SBFreigabe` |
| 10 ER | `WarenZugaenge` | **Kreditorenteil**: `LieferantenRechnungsNummer`, `IstBezahlt`, `SchonBezahlt`, `SkontoDatum1/2`, `FIBU*`, dazu Fracht / Verpackung / Versicherung / Zoll je mit Nettopreis und MwSt |

Weitere Kopf-Satelliten ohne nennenswerte Felder: `Anfragen`, `Lieferscheine`, `Rechnungen`,
`Gutschriften`, `LieferantenAnfragen`, `Packzettel`, `BestellVorschlaege`,
`AuftragsVorschlaege`. Positions-Satelliten existieren für AA, AN, AB, AR, EB, ER, EV, AV;
inhaltlich relevant sind nur `AuftraegePositionen` (`FRVEAnzahl`, `IstStreckenPosition`)
und `BestellungenPositionen` (`IstStreckenPosition`, `K_SondermengeFuerKunde`).

> Typ 14 heißt in `BelegTypen` „Auftragsvorerfassungen", die Satellitentabelle aber
> `AuftragsVorschlaege`.

> **Konsequenz für Auswertungen:** `AuftragsVorschlaegePos` stellt mit 8,5 Mio Zeilen 79 %
> von `BelegePositionen`. Wer ohne Join auf `Belege` und ohne `BelegTyp`-Filter aggregiert,
> misst überwiegend Auftragsvorerfassungen.

### Belege ohne Positionen

Verwaiste Positionen gibt es keine, aber **Köpfe ohne jede Position sind häufig**:
ER 10.093 (**57 %**), AB 5.615 (4 %), AL 699, AR 479, AS 339, EB 268.

Bei Warenzugängen ist das kein Suchfehler: `WarenZugaenge` ist faktisch der
**Eingangsrechnungs-/Kreditorenbeleg** und kommt auch ohne Warenbewegung vor (Fracht,
Zoll, Dienstleistung, Vorabrechnung). Ein INNER JOIN Kopf↔Position verliert diese Belege
stillschweigend – für Belegzählungen daher auf `Belege` zählen, nicht auf Positionen.

---

## Belegtypen

`Belege.BelegTyp` ist `tinyint`. Die Belegnummer trägt das Kürzel als Präfix
(`AB2618895`), was der zuverlässigste Weg ist, einen Typ zu erkennen.
Nummernkreis-Schema: `<Kürzel><JJ><laufend>`.

| Typ | Kürzel | Bezeichnung | Belege | archiviert | Anmerkung |
|---|---|---|---|---|---|
| 1 | AA | Anfragen | 161 | 42 % | Verkaufsanfragen, selten genutzt |
| 2 | AN | Angebote | 70.471 | 94 % | |
| 3 | AB | Aufträge | 143.893 | **96 %** | Auftragsbestätigungen / Abrufe |
| 4 | AL | Lieferscheine | 219.788 | **99,8 %** | Ausgangslieferscheine |
| 5 | AR | Rechnungen | 192.326 | **0,3 %** | Ausgangsrechnungen |
| 6 | AG | Gutschriften | 927 | 15 % | |
| 8 | EA | Lieferantenanfragen | 12.831 | 0,8 % | Preisanfragen an Lieferanten, überwiegend TY |
| 9 | EB | Bestellungen | 14.054 | 95 % | Bestellungen bei Lieferanten |
| 10 | ER | Warenzugänge | 17.730 | 57 % | Kürzel steht für Eingangsrechnung |
| 12 | AP | Packzettel | 1 | – | praktisch ungenutzt |
| 13 | EV | Bestellvorschläge | 89 | 80 % | selten |
| 14 | AV | Auftragsvorerfassungen | **338.574** | **99,9 %** | größter Typ; Ziel des Forecast-Imports |
| 16 | RK | RMA Kunde | 116 | 5 % | |
| 17 | RL | RMA Lieferant | 19 | 16 % | |
| 18 | AS | Service Verkauf | 18.419 | 0,8 % | u. a. Musterlieferungen |
| 19 | ES | Service Einkauf | 5.897 | 13 % | |
| 21 | EG | Lieferantengutschriften | 117 | 9 % | |
| 23 | AU | Umbuchungen | 79 | 4 % | abweichendes Nummernschema `AU2610079` |
| 25 | EL | Eingangslieferscheine | 1 | – | Einzelfall 2018 |
| 27 | EF | Lieferavise | 1 | – | Einzelfall 2020 |

Ohne Bestand und damit ungenutzt: 7 Wertgutschriften, 11 Lieferantenmahnungen,
15 Vorabrechnungen, 20 Vermietrechnungen, 22 Lieferantenwertgutschriften,
24 Qualitätssicherungen, 26 EK-Vorabrechnungen. Dazu eine Dummy-Zeile mit `BelegTyp = 0`.

**Achtung – Fehler im MCP-Tooling:** `list_line_items` dokumentiert `AA` als Typ **8**.
Real ist `AA` = **1** (Verkaufsanfragen) und **8** = `EA` (Lieferantenanfragen). Wer über
das Tool `document_type='AA'` anfragt, bekommt Lieferantenanfragen.

**Archivierung ist typabhängig und kein Löschkennzeichen.** Bei AB/AL/AV/EB ist Archivierung
der Normalzustand nach Abwicklung – `IstArchiviert = 0` blendet dort 95–99,9 % der Daten aus.
Bei AR/EA/AS ist es umgekehrt. Historische Auswertungen daher **mit** archivierten Belegen.

### `BelegTypen` – Join-Falle

Die Klarnamen stehen in `BelegTypen` (28 Zeilen). **`BelegTypenID` ist nicht `BelegTyp`:**
ab ID 7 laufen die Nummern auseinander, weil Wertgutschriften nachträglich einsortiert
wurden (ID 7 → Typ 8, ID 13 → Typ 14, ID 14 → Typ 7). Ein Join über die ID liefert
falsche Bezeichnungen.

```sql
JOIN ALPHAPLAN_EchtDaten.dbo.BelegTypen BT ON BT.BelegTyp = Belege.BelegTyp   -- richtig
JOIN ALPHAPLAN_EchtDaten.dbo.BelegTypen BT ON BT.BelegTypenID = Belege.BelegTyp -- falsch
```

`BelegTypen` enthält außerdem die E-Mail-Vorlagen für den Belegversand
(`EmailBetreff` als SQL-Ausdruck, `EmailInhalt` als RTF) – bei Typ 5 belegt.

### Belegkette

`BelegTypenQuellBelegTypen` definiert die erlaubten Übernahmen. **Beide Spalten
referenzieren `BelegTypenID`**, unterliegen also derselben Verschiebung. Der konkrete
Vorgängerbeleg steht in `Belege.SrcBelegeID` bzw. `BelegePositionen.SrcBelegePositionenID`.

```
Verkauf:  AA ─> AN ─> AB ─> AL ─> AR ─> AG        AV ─> AB / AL / AR
                       └─> AP                     AA / AN ─> AV

Einkauf:  EA ─> EB ─> ER                          EA ─> EV ─> EB
               └─> Lieferavis (27) / QS (24) / Eingangslieferschein (25) ─> ER
          ER ─> EG (21)
```

`IstAlsQuelleZugelassen = 1` steht nur bei vier Übergängen (AN→AB, AV→AB, AN→AL, AB→AL),
obwohl die übrigen fachlich stattfinden – das Flag meint vermutlich eine engere
Berechtigung wie die Automatikübernahme, nicht „diese Kette existiert" *(unverifiziert)*.

---

## `Belege` – relevante Spalten

Von 255 Spalten sind die meisten Summen-, Rabatt-, Steuer- und FIBU-Felder. Fachlich relevant:

| Spalte | Typ | Bedeutung |
|---|---|---|
| `BelegeID` | int | PK, Join-Key zu `BelegePositionen` und `XDateien` |
| `Belegnummer` | nvarchar(50) | fachliche Nummer inkl. Typpräfix |
| `BelegTyp` | tinyint | Belegtypen |
| `Datum` | datetime | Belegdatum, kann in der Zukunft liegen |
| `Sachbearbeiter` | nvarchar(30) | Klarname als **Freitext** – unzuverlässig, siehe Sachbearbeiter |
| `BelegTitel` | nvarchar(50) | bei EK-Belegen ein Bestellungstyp aus `Texte`, bei AN der Endkunde |
| `AuftragsAdressenID` | int | → `Adressen.AdressenID` – **der Kunde bzw. Lieferant des Belegs** |
| `LieferAdressenID`, `RechnungsAdressenID`, `VersenderAdressenID`, `VertreterAdressenID`, `VerbandsAdressenID` | int | weitere Adressrollen |
| `LieferAnschriftenID` | int | → `Anschriften.AnschriftenID` → `Laender` |
| `WaehrungenID` | int | → `Waehrungen`. **Das Währungskürzel steht nicht im Beleg** (Waehrungen → `alphaplan://stammdaten`) |
| `KursFaktor`, `KursDivisor` | float | Kurs-Snapshot zum Belegdatum. **`KursFaktor` ist praktisch immer 1, der Kurs steckt im `KursDivisor`** |
| `ReferenzWaehrungenID`, `ReferenzKursFaktor/-Divisor` | | zweite Währungsebene |
| `AbsicherungsWaehrungenID`, `AbsicherungsKurs*Soll/Ist` | | Kurssicherung |
| `MwstPflicht` | bit | muss zum Land der Lieferanschrift passen |
| `IstArchiviert`, `IstStorniert`, `IstGesperrt`, `IstUebernommen` | bit | Status-Flags, Belegtypen beachten |
| `LieferDatum` | datetime | Kopf-Liefertermin |
| `AnlieferungsDatumWunsch`, `AnlieferungsDatumBestaetigt` (+ `…KW`) | datetime | **existieren auf Kopf- UND Positionsebene** – Positionsebene ist die maßgebliche |
| `IhrAuftrag` | nvarchar(30) | Kundenauftragsnummer; im AN-Kontext stattdessen die Abgabeart (Werte in `Texte.FieldName='IhrAuftrag'`: Portal, SAP Ariba, Luminovo, PDF, Tabelle, Intern) |
| `IhreAnfrage` | nvarchar(30) | Kundenanfrage-Referenz; bei Zollner **zweckentfremdet** als VK-Netto-Freitext |
| `InterneBelegNummer` | nvarchar(50) | bei Zollner-Abrufen **zweckentfremdet**: Zielmenge mit Tausenderpunkten |
| `BestellNummer` | nvarchar(30) | Kundenbestellnummer; im AN-Kontext als SOP interpretiert |
| `Kommission` | nvarchar(30) | im AN-Kontext als EOP interpretiert |
| `AuftragsTyp` | nvarchar(30) | Rahmen / Lieferplan / Konsi / ForeCast / Einzelbestellung |
| `AnsprechPartner` | nvarchar(30) | Freitext (im AN-Kontext das Projekt); daneben `AuftragsAnsprechpartnerID` → `Ansprechpartner` |
| `Abteilung` | nvarchar(30) | Werte aus `Texte` |
| `Vorsatz`, `Nachsatz`, `Hinweistext1/2` | nvarchar(max) | **RTF-Kandidaten**, Textbausteine aus `Texte` |
| `SrcBelegeID` | int | Vorgängerbeleg – Belegkette (AA→AN→AB→AL→AR) |
| `EDI_BelegeID`, `MitEDI`, `IstEDIUebertragen` | | EDI-Anbindung |

### `Sachbearbeiter` ist ein Freitextfeld

Fast jede gewachsene Auswertung filtert `Sachbearbeiter LIKE '%…%'`. Die realen Werte
ab 2024 (50 verschiedene) machen das riskant:

* **71.552 Belege haben gar keinen Sachbearbeiter** – die mit Abstand größte Gruppe.
  Jeder `LIKE`-Filter verliert sie stillschweigend.
* `CVS` (50.013) ist kein Mensch, sondern ein technischer Benutzer.
* Tippfehler und Dubletten sind real: `Andreas Scholz` (3.723) vs. `Andreas Sholz` (1),
  `Jannick Machowetz` (1.240) vs. `Jannick Machowetz 1` (1). Dazu Platzhalter `Frei 1`,
  `Frei 2`, `Kunde`, `Ü`, `Walsin Benchmark`.
* `Texte` kennt unter `FieldName = 'Sachbearbeiter'` nur 14 Namen – real sind 50 im Umlauf.

Die saubere Alternative ist `Belege.AnlageBenutzerID` → `Benutzer` (95 Zeilen:
`BenutzerID`, `LoginName`, `Name`, `EMailAdresse`). Das ist allerdings der **Anleger**,
nicht zwingend der zuständige Sachbearbeiter – die beiden Felder sind nicht verknüpft
*(unverifiziert)*.

Es gibt **kein** `AdressNummer`, `KundenNummer`, `SuchName` oder `WaehrungsKuerzel`
auf `Belege` – diese Felder existieren nur in der ETL-Staging-DB. Kundenname und
-nummer kommen ausschließlich über den Join auf `Adressen`.

---

## `BelegePositionen` – relevante Spalten

| Spalte | Typ | Bedeutung |
|---|---|---|
| `BelegePositionenID` | int | PK |
| `BelegeID` | int | → `Belege.BelegeID` |
| `ZeilenPosition` | int | Sortierung innerhalb des Belegs |
| `PositionsTyp` | tinyint | `0` = Artikelposition (99,9 % aller Zeilen, mit `ArtikelID`), `2` = Textzeile (nie mit `ArtikelID`, ~0,3 %), `8` nur bei RMA-Belegen (16/17), `10` Einzelfall. Siehe unten |
| `ArtikelID` | int | → `Artikel.ArtikelID`; 0/NULL bei Freitextpositionen |
| `ArtikelNummer` | nvarchar(30) | MPN |
| `ArtikelBezeichnung` | nvarchar(max) | Freitext; trägt bei Importen die MPN, wenn kein Artikel existiert |
| `KundenArtikelNummer` | nvarchar(30) | Teilenummer beim Kunden |
| `BestellNummer` | nvarchar(30) | positionsbezogene Bestellnummer |
| `PositionsText` | **nvarchar(30)** | kurzes Kennzeichen, **kein Langtext und kein RTF**. Marker wie `RM`, `SL`, `Abruf` |
| `ZusatzText1..3` | nvarchar(max) | die eigentlichen Langtexte, **RTF-Kandidaten**; Bedeutung je Belegtyp (bei AN: Lieferzeit / Ergebnis / Kommentar) |
| `LieferDatum` | datetime | Liefertermin |
| `AnlieferungsDatumWunsch`, `AnlieferungsDatumBestaetigt` (+ `…KW`) | datetime | Wunsch- / bestätigter Termin |
| `K_bestaetigtesLieferdatum`, `K_Lief_KW` | | kundenspezifische Terminfelder – **faktisch ungenutzt** (1 gefüllte Zeile) |
| `MontageDatum` | datetime | |
| `SrcBelegeID`, `SrcBelegePositionenID`, `SrcVKBelegeID`, `OrgBelegePositionenID` | int | Herkunft in der Belegkette |

### `PositionsTyp`

| Wert | Bedeutung | Vorkommen |
|---|---|---|
| 0 | Artikelposition | alle Belegtypen; ~99,97 % davon mit `ArtikelID` |
| 2 | Text-/Kommentarzeile | AA, AN, AB, EA, EB, AV, AS, ES – nie mit `ArtikelID`. Bei AL, AR, ER, AG gar nicht vorhanden |
| 8 | RMA-spezifisch | nur BelegTyp 16/17, 50 Zeilen gesamt |
| 10 | unbekannt | eine einzige Zeile (BelegTyp 19) |

Für Mengen- und Wertsummen also `PositionsTyp = 0` filtern, sonst laufen Textzeilen mit.

### Mengen und Preiseinheit

Formal gibt es drei Einheitenebenen mit je eigener Menge, Faktor und Divisor
(`VEAnzahl`/`VEText`, `BEAnzahl`/`BEFaktor`/`BEDivisor`/`BEsProVE`/`BEText`,
`PEAnzahl`/`PEFaktor`/`PEDivisor`/`PEsProBE`/`PEText`).

**In der Praxis ist das unkritisch:** `BEsProVE = 1` bei 10.735.872 von 10.735.882
Positionen (10 Ausreißer mit 100 bzw. 1000). `VEText` und `BEText` sind fast immer
`Stück`. **`VEAnzahl` ist also die Stückzahl** und direkt mit `Artikel.K_VPE` vergleichbar.

Die Preiseinheit weicht dagegen systematisch ab: `PEText` ist bei 10,50 Mio Positionen
`/1000` und bei 238.838 `/100`. Die Branche kalkuliert je 1000 Stück.

| Spalte | Bezug |
|---|---|
| `NettoPreisEinzel` | **je Stück** – verifiziert: `NettoPreisGesamt = NettoPreisEinzel * VEAnzahl` |
| `PENettoPreis` | je Preiseinheit, also i. d. R. je 1000 Stück |

Daher die `* 1000`-Multiplikationen in gewachsenen Reports: sie rechnen den Stückpreis
auf die gewohnte Anzeigeeinheit hoch. Wer Preise gegen eine Lieferantenliste vergleicht,
muss wissen, welche der beiden Einheiten dort gemeint ist.

### Welches Datumsfeld gilt wann?

Die vier Terminfelder sind **nicht parallel gepflegt, sondern belegtyp-spezifisch**
(Positionen ab 2024):

| Typ | Positionen | `LieferDatum` | `AnlieferungsDatumWunsch` | `AnlieferungsDatumBestaetigt` |
|---|---|---|---|---|
| 1 AA | 17.912 | 97 % | – | – |
| 2 AN | 63.388 | 28 % | – | – |
| 3 AB | 161.526 | 99,9 % | **79 %** | – |
| 4 AL | 67.545 | 99,9 % | 95 % | – |
| 5 AR | 71.960 | 99,9 % | 95 % | – |
| 8 EA | 25.879 | 43 % | – | – |
| 9 EB | 38.795 | 99,8 % | – | **88 %** |
| 10 ER | 36.213 | 100 % | – | **97 %** |
| 14 AV | 2.895.428 | 99,97 % | – | – |

**Faustregel: `AnlieferungsDatumWunsch` ist die Verkaufsseite, `AnlieferungsDatumBestaetigt`
die Einkaufsseite.** Ein Wunschtermin auf einer Bestellung existiert praktisch nicht
(1 Zeile), ein bestätigter Termin auf einem Auftrag ebenso wenig (11 Zeilen). `LieferDatum`
ist das einzige durchgängig gefüllte Feld – außer bei Angeboten (28 %) und
Lieferantenanfragen (43 %), wo naturgemäß noch kein Termin feststeht.

> Das MCP-Tool `list_line_items` filtert über alle vier Felder gleichzeitig (ODER-verknüpft).
> Der Filter verhält sich damit je Belegtyp unterschiedlich: bei EB greift er praktisch über
> `LieferDatum` und `AnlieferungsDatumBestaetigt`, bei AB über `LieferDatum` und
> `AnlieferungsDatumWunsch`.

Der `31.12.`-Platzhalter im Wunschtermin betrifft 1.750 AB-Positionen (1 %).

### Übernahme-Mechanik (offene Mengen)

`VEAnzahl` = `SchonUebernommen` + `NochZuUebernehmen`. Die Identität hält über alle
Belegtypen hinweg (Abweichung bei 9 von 3,39 Mio Positionen ab 2024). Die AlphaPlan-Oberfläche
zeigt nur `VEAnzahl` (Gesamtmenge) und `NochZuUebernehmen` (offen); `SchonUebernommen` ist
das intern geführte Gegenstück. Ergänzend: `MindestensUebernehmen`, `JetztUebernehmen`,
`JetztUebernehmenMin/Max`, `JetztUebernehmenInBEs`, `IgnoriereSchonUebernommen`.

**`NochZuUebernehmen > 0` bedeutet nur bei Belegtypen mit Folgebeleg wirklich offen.**
Bei Endbelegen wird nie übernommen, dort bleibt der Wert dauerhaft auf der Gesamtmenge
stehen (Positionen ab 2024):

| Typ | Positionen | komplett offen | komplett erledigt | Interpretation |
|---|---|---|---|---|
| 3 AB | 161.523 | 30 % | **70 %** | aussagekräftig |
| 9 EB | 38.795 | 16 % | **84 %** | aussagekräftig |
| 1 AA | 17.912 | 31 % | 70 % | aussagekräftig |
| 2 AN | 63.384 | 97 % | 3 % | nur bei Auftragserteilung übernommen |
| 14 AV | 2.895.428 | **98 %** | 2 % | dominiert jede ungefilterte Auswertung |
| 4 AL | 67.544 | 99 % | 1 % | Endbeleg – Wert ohne Aussage |
| 5 AR | 71.960 | 99,9 % | 0,03 % | Endbeleg – Wert ohne Aussage |
| 8 EA | 25.879 | 100 % | 0,7 % | Endbeleg – Wert ohne Aussage |
| 10 ER | 36.213 | 99,99 % | 0,01 % | Endbeleg – Wert ohne Aussage |

### Preise, Währung und die `X`-Spalten

**Die `X`-Spalten sind keine Währungsumrechnung.** Geprüft über 10,5 Mio Positionen:
`NettoPreisEinzelX / NettoPreisEinzel` ist für EUR, USD und JPY exakt 1,0 – die
X-Variante ist in der Praxis wertgleich mit dem Original (vermutlich ein
Rundungs-/Ausgabewert). Wer über `…X` in EUR zu rechnen glaubt, rechnet in Belegwährung.

Betroffen: `NettoPreisEinzel(X)`, `NettoPreisGesamt(X)`, `BruttoPreisEinzel(X)`,
`BruttoPreisGesamt(X)`, `EinstandsPreisGesamt(X)`, `MehrwertSteuer(X)`, `AustauschSteuer(X)`.

**Verkaufswerte stehen in Belegwährung, Einstandspreise offenbar in EUR** *(unverifiziert,
aus Plausibilitätsrechnung)*. Beispiel aus einem JPY-Lieferschein:
`NettoPreisGesamt` = 17.820 (JPY), `EinstandsPreisGesamt` = 92,90 – bei einem Divisor
von 186,58 ergibt 17.820 JPY ≈ 95,50 EUR, was zum Einstand passt. In JPY wäre der
Einstand absurd niedrig. **Konsequenz: Rohertrag und Marge lassen sich bei
Fremdwährungsbelegen nicht direkt aus den beiden Spalten bilden**, ohne vorher
umzurechnen.

Umrechnung in EUR:

```sql
BP.NettoPreisGesamt * B.KursFaktor / NULLIF(B.KursDivisor, 0)
```

Weitere Preisspalten: `PENettoPreis`, `BEEinstandsPreis`, `RabattProzente1..3` /
`RabattBetrag1..3` / `RabattBetragProVE1..3`, `ZuschlagProzente1..3`, `NebenKosten1/2`,
`RohertragGesamt`, `HandelsSpanne`.

`NettoGesamt` gibt es nicht – die Spalte heißt `NettoPreisGesamt`.

### `K_*`-Custom-Felder (kundenspezifisch)

`K_GeschaeftsArt`, `K_Quotierungsphase`, `K_ChanceOfSucces` (sic, ein „s"), `K_AuftragsArt`,
`K_Kommentar`, `K_MOQ`, `K_StornoGrund`, `K_FreiesFeld`, `K_IstInKlaerung`, `K_IstZugeteilt`,
`K_IstBandstillstand`, `K_AngebotePositionenBenchmark1/2`, `K_ProjekteID`.
Wertelisten in `Texte` (Texte → `alphaplan://stammdaten`).

---
