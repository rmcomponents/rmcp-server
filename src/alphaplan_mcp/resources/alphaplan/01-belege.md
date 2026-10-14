# AlphaPlan – Belegmodell

> Teil der AlphaPlan-SQL-Referenz. Einstieg, Konventionen und Fallstricke: `alphaplan://start`.
> Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind markiert.

---

## Das Belegmodell

Alle Belegarten teilen sich **einen** Kopf- und **einen** Positionsstamm:

* `Belege` (im einstelligen Millionenbereich) – Kopf aller Belegtypen, 255 Spalten
* `BelegePositionen` (rund 11 Mio Zeilen) – Positionen aller Belegtypen, 192 Spalten

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

> **Konsequenz für Auswertungen:** `AuftragsVorschlaegePos` stellt rund 79 %
> von `BelegePositionen`. Wer ohne Join auf `Belege` und ohne `BelegTyp`-Filter aggregiert,
> misst überwiegend Auftragsvorerfassungen.

### Belege ohne Positionen

Verwaiste Positionen gibt es keine, aber **Köpfe ohne jede Position sind häufig**:
am stärksten bei ER (**57 %**) und AB (4 %); AL, AR, AS und EB folgen mit jeweils nur
einigen hundert Belegen.

Bei Warenzugängen ist das kein Suchfehler: `WarenZugaenge` ist faktisch der
**Eingangsrechnungs-/Kreditorenbeleg** und kommt auch ohne Warenbewegung vor (Fracht,
Zoll, Dienstleistung, Vorabrechnung). Ein INNER JOIN Kopf↔Position verliert diese Belege
stillschweigend – für Belegzählungen daher auf `Belege` zählen, nicht auf Positionen.

---

## Belegtypen

`Belege.BelegTyp` ist `tinyint`. Die Belegnummer trägt das Kürzel als Präfix
(`AB2618895`), was der zuverlässigste Weg ist, einen Typ zu erkennen.
Nummernkreis-Schema: `<Kürzel><JJ><laufend>`. Bestandsangaben sind gerundete
Größenordnungen.

| Typ | Kürzel | Bezeichnung | Bestand (~) | archiviert | Anmerkung |
|---|---|---|---|---|---|
| 1 | AA | Anfragen | 0,2k | 42 % | Verkaufsanfragen, selten genutzt |
| 2 | AN | Angebote | 70k | 94 % | |
| 3 | AB | Aufträge | 144k | **96 %** | Auftragsbestätigungen / Abrufe |
| 4 | AL | Lieferscheine | 220k | **99,8 %** | Ausgangslieferscheine |
| 5 | AR | Rechnungen | 192k | **0,3 %** | Ausgangsrechnungen |
| 6 | AG | Gutschriften | 0,9k | 15 % | |
| 8 | EA | Lieferantenanfragen | 13k | 0,8 % | Preisanfragen an Lieferanten, überwiegend TY |
| 9 | EB | Bestellungen | 14k | 95 % | Bestellungen bei Lieferanten |
| 10 | ER | Warenzugänge | 18k | 57 % | Kürzel steht für Eingangsrechnung |
| 12 | AP | Packzettel | 1 | – | praktisch ungenutzt |
| 13 | EV | Bestellvorschläge | 90 | 80 % | selten |
| 14 | AV | Auftragsvorerfassungen | 339k | **99,9 %** | größter Typ; Ziel des Forecast-Imports |
| 16 | RK | RMA Kunde | 0,1k | 5 % | |
| 17 | RL | RMA Lieferant | 20 | 16 % | |
| 18 | AS | Service Verkauf | 18k | 0,8 % | u. a. Musterlieferungen |
| 19 | ES | Service Einkauf | 6k | 13 % | |
| 21 | EG | Lieferantengutschriften | 0,1k | 9 % | |
| 23 | AU | Umbuchungen | 80 | 4 % | abweichendes Nummernschema `AU2610079` |
| 25 | EL | Eingangslieferscheine | 1 | – | Einzelfall 2018 |
| 27 | EF | Lieferavise | 1 | – | Einzelfall 2020 |

Ohne Bestand und damit ungenutzt: 7 Wertgutschriften, 11 Lieferantenmahnungen,
15 Vorabrechnungen, 20 Vermietrechnungen, 22 Lieferantenwertgutschriften,
24 Qualitätssicherungen, 26 EK-Vorabrechnungen. Dazu eine Dummy-Zeile mit `BelegTyp = 0`.

Das MCP-Tool `list_beleg_positionen` bildet die Kürzel korrekt ab: `beleg_typ='AA'` liefert
Verkaufsanfragen (Typ 1), `'EA'` Lieferantenanfragen (Typ 8).

**Archivierung ist typabhängig und kein Löschkennzeichen.** Bei AB/AL/AV/EB ist Archivierung
der Normalzustand nach Abwicklung – `IstArchiviert = 0` blendet dort 95–99,9 % der Daten aus.
`list_beleg_positionen` schließt archivierte Belege standardmäßig aus (auch bei AB); für
Historienfragen `include_archived=true` setzen.
Bei AR/EA/AS ist es umgekehrt. Historische Auswertungen daher **mit** archivierten Belegen.

### `BelegTypen` – Join-Falle

Die Klarnamen stehen in `BelegTypen` (28 Zeilen, schemastabil – kann sich bei
Schema-Änderung verschieben). **`BelegTypenID` ist nicht `BelegTyp`:**
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

`IstAlsQuelleZugelassen = 1` steht nur bei vier von rund 30 Übergängen (AN→AB, AV→AB,
AN→AL, AB→AL),
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
ab 2024 (um die 50 verschiedene) machen das riskant:

* **gut 70.000 Belege haben gar keinen Sachbearbeiter** – die mit Abstand größte Gruppe.
  Jeder `LIKE`-Filter verliert sie stillschweigend.
* `CVS` (rund 50.000) ist kein Mensch, sondern ein technischer Benutzer.
* Tippfehler und Dubletten sind real: `Andreas Scholz` (der mit Abstand häufigste Name)
  vs. `Andreas Sholz`, `Jannick Machowetz` vs. `Jannick Machowetz 1`. Dazu Platzhalter
  `Frei 1`, `Frei 2`, `Kunde`, `Ü`, `Walsin Benchmark`.
* `Texte` kennt unter `FieldName = 'Sachbearbeiter'` nur 14 Namen – real sind um die 50
  im Umlauf.

Die saubere Alternative ist `Belege.AnlageBenutzerID` → `Benutzer` (95 Zeilen,
schemastabil, kann sich bei Account-Pflege verschieben:
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
| `K_bestaetigtesLieferdatum`, `K_Lief_KW` | | kundenspezifische Terminfelder – **faktisch ungenutzt** (nur vereinzelte gefüllte Zeilen) |
| `MontageDatum` | datetime | |
| `SrcBelegeID`, `SrcBelegePositionenID`, `SrcVKBelegeID`, `OrgBelegePositionenID` | int | Herkunft in der Belegkette |

### `PositionsTyp`

| Wert | Bedeutung | Vorkommen |
|---|---|---|
| 0 | Artikelposition | alle Belegtypen; ~99,97 % davon mit `ArtikelID` |
| 2 | Text-/Kommentarzeile | AA, AN, AB, EA, EB, AV, AS, ES – nie mit `ArtikelID`. Bei AL, AR, ER, AG gar nicht vorhanden |
| 8 | RMA-spezifisch | nur BelegTyp 16/17, einige Dutzend Zeilen gesamt |
| 10 | unbekannt | eine einzige Zeile (BelegTyp 19) |

Für Mengen- und Wertsummen also `PositionsTyp = 0` filtern, sonst laufen Textzeilen mit.
`list_beleg_positionen` liefert Textzeilen standardmäßig mit (Spalten `PositionsTyp`,
`ArtikelBezeichnung`); `include_text_lines=false` blendet sie aus.

### Mengen und Preiseinheit

Formal gibt es drei Einheitenebenen mit je eigener Menge, Faktor und Divisor
(`VEAnzahl`/`VEText`, `BEAnzahl`/`BEFaktor`/`BEDivisor`/`BEsProVE`/`BEText`,
`PEAnzahl`/`PEFaktor`/`PEDivisor`/`PEsProBE`/`PEText`).

**In der Praxis ist das unkritisch:** `BEsProVE = 1` bei praktisch allen Positionen
(nur eine Handvoll Ausreißer mit 100 bzw. 1000). `VEText` und `BEText` sind fast immer
`Stück`. **`VEAnzahl` ist also die Stückzahl** und direkt mit `Artikel.K_VPE` vergleichbar.

Die Preiseinheit weicht dagegen systematisch ab: `PEText` ist bei der überwiegenden
Mehrheit der Positionen `/1000` und nur bei einer kleinen Minderheit `/100`. Die Branche
kalkuliert je 1000 Stück.

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

| Typ | Positionen (~) | `LieferDatum` | `AnlieferungsDatumWunsch` | `AnlieferungsDatumBestaetigt` |
|---|---|---|---|---|
| 1 AA | 18k | 97 % | – | – |
| 2 AN | 63k | 28 % | – | – |
| 3 AB | 162k | 99,9 % | **79 %** | – |
| 4 AL | 68k | 99,9 % | 95 % | – |
| 5 AR | 72k | 99,9 % | 95 % | – |
| 8 EA | 26k | 43 % | – | – |
| 9 EB | 39k | 99,8 % | – | **88 %** |
| 10 ER | 36k | 100 % | – | **97 %** |
| 14 AV | 2,9 Mio | 99,97 % | – | – |

**Faustregel: `AnlieferungsDatumWunsch` ist die Verkaufsseite, `AnlieferungsDatumBestaetigt`
die Einkaufsseite.** Ein Wunschtermin auf einer Bestellung existiert praktisch nicht,
ein bestätigter Termin auf einem Auftrag ebenso wenig. `LieferDatum`
ist das einzige durchgängig gefüllte Feld – außer bei Angeboten (28 %) und
Lieferantenanfragen (43 %), wo naturgemäß noch kein Termin feststeht.

> Das MCP-Tool `list_beleg_positionen` filtert über alle vier Felder gleichzeitig (ODER-verknüpft).
> Der Filter verhält sich damit je Belegtyp unterschiedlich: bei EB greift er praktisch über
> `LieferDatum` und `AnlieferungsDatumBestaetigt`, bei AB über `LieferDatum` und
> `AnlieferungsDatumWunsch`. Mit `open_only=true` (nur AA/AB/EB) ist der Datumsfilter
> optional – so liefert z. B. `beleg_typ='AB'`, `adress_nummer`, `open_only=true` den
> kompletten offenen Auftragsbestand eines Kunden. In den Ergebnissen heißen die Terminfelder
> je Belegtyp unterschiedlich (EB: `RequestedDate`/`BestaetigtZum`, AB: `KDWunsch`), und der
> 1900-01-01-Platzhalter wird als null geliefert. Alle Termine kommen als reines Datum ohne
> Uhrzeit; der Datumsfilter umfasst ganze Tage (`date_to` schließt den ganzen Tag ein, auch
> Belege mit Uhrzeit wie `B.Datum` 21:45).

Der `31.12.`-Platzhalter im Wunschtermin betrifft rund 2.000 AB-Positionen (1 %).

**Weitere bewusste Platzhalter:** `7777-01-01` (rund 2.600 Positionen, v. a. `LieferDatum`) und
`9999-xx-xx` (rund 180, v. a. bestätigte Termine) werden vom Team gezielt gesetzt und sind keine
echten Termine. `list_beleg_positionen` gibt sie unverändert aus; nur `1900-01-01` wird zu null.

### Übernahme-Mechanik (offene Mengen)

`VEAnzahl` = `SchonUebernommen` + `NochZuUebernehmen`. Die Identität hält über alle
Belegtypen hinweg (nur vereinzelte Abweichungen unter Positionen ab 2024). Die
AlphaPlan-Oberfläche
zeigt nur `VEAnzahl` (Gesamtmenge) und `NochZuUebernehmen` (offen); `SchonUebernommen` ist
das intern geführte Gegenstück. Ergänzend: `MindestensUebernehmen`, `JetztUebernehmen`,
`JetztUebernehmenMin/Max`, `JetztUebernehmenInBEs`, `IgnoriereSchonUebernommen`.

**`NochZuUebernehmen > 0` bedeutet nur bei Belegtypen mit Folgebeleg wirklich offen.**
Bei Endbelegen wird nie übernommen, dort bleibt der Wert dauerhaft auf der Gesamtmenge
stehen (Positionen ab 2024):

| Typ | Positionen (~) | komplett offen | komplett erledigt | Interpretation |
|---|---|---|---|---|
| 3 AB | 162k | 30 % | **70 %** | aussagekräftig |
| 9 EB | 39k | 16 % | **84 %** | aussagekräftig |
| 1 AA | 18k | 31 % | 70 % | aussagekräftig |
| 2 AN | 63k | 97 % | 3 % | nur bei Auftragserteilung übernommen |
| 14 AV | 2,9 Mio | **98 %** | 2 % | dominiert jede ungefilterte Auswertung |
| 4 AL | 68k | 99 % | 1 % | Endbeleg – Wert ohne Aussage |
| 5 AR | 72k | 99,9 % | 0,03 % | Endbeleg – Wert ohne Aussage |
| 8 EA | 26k | 100 % | 0,7 % | Endbeleg – Wert ohne Aussage |
| 10 ER | 36k | 99,99 % | 0,01 % | Endbeleg – Wert ohne Aussage |

### Preise, Währung und die `X`-Spalten

**Die `X`-Spalten sind keine Währungsumrechnung.** Geprüft über Millionen von Positionen:
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
