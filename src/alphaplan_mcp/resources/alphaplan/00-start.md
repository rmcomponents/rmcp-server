# AlphaPlan – SQL-Referenz

Referenz für Agenten, die gegen die AlphaPlan-SQL-Server-Datenbank `ALPHAPLAN_EchtDaten`
lesen. Diese Seite enthält alles, was bei **jeder** Query gilt; die Detailseiten werden
nach Bedarf dazugeladen.

Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind im Text als
*(unverifiziert)* markiert.

> **Stand der Zahlen:** Alle in diesen Dokumenten genannten Zeilenzahlen,
> Tabellengrößen und Verteilungsangaben sind illustrative Momentaufnahmen vom Stand der
> Analyse und können von der Live-Datenbank abweichen. Als einziger absoluter Bezug gilt
> das Datum oben (Stand der Analyse); für verbindliche Größen immer live zählen,
> nicht auf die hier genannten Werte bauen.

---

## Wegweiser

| Resource | Inhalt | wann laden |
|---|---|---|
| `alphaplan://belege` | Belegköpfe und Positionen: Kundenaufträge, Angebote, Lieferantenbestellungen, Lieferscheine, Rechnungen. Mengen, offene Restmengen, Liefertermine, Preise, Belegkette | bei allem, was Aufträge, Angebote, Bestellungen, Lieferungen oder Rechnungen betrifft |
| `alphaplan://stammdaten` | Artikel (MPN, VPE, EOL, Lieferfähigkeit), Adressen und ihre Klassifikation, Lagerorte und Bestände, Länder, Währungen, Auflösung von Codes über `Texte` | Artikel-, Kunden-, Lieferanten- oder Bestandsfragen; wenn ein Code oder Kürzel aufzulösen ist |
| `alphaplan://preise-und-zahlen` | Einkaufs- und Verkaufspreise mit Mengenstaffeln, kundenspezifische Teilenummern, vorberechnete Umsatz- und Absatzstatistik, Reichweiten- und Bedarfsvorschau | Preisvergleiche, Umsatz- und Absatzzahlen, Verfügbarkeit in der Zukunft |
| `alphaplan://historie` | Woher ein Beleg kam (EDI, Webshop) und wer wann welches Feld geändert hat; Lagerbewegungsprotokoll | Herkunft eines Belegs, Änderungshistorie, Bestandsbewegungen |
| `alphaplan://sql-muster` | Erprobte Queries und die Indexlage | vor jeder größeren Query |
| `alphaplan://wissensluecken` | ungeklärte Felder und nicht erschlossene Bereiche | wenn eine Frage ins Leere läuft – statt zu raten |

---|---|---|
| `alphaplan://belege` | `Belege`, `BelegePositionen`, Belegtypen, Belegkette, Satellitentabellen, Mengen, Preise, Termine, offene Mengen | bei allem, was mit Aufträgen, Angeboten, Bestellungen, Lieferscheinen oder Rechnungen zu tun hat |
| `alphaplan://stammdaten` | `Artikel`, `Adressen`, `Anschriften`, `Lager`, `Waehrungen`, `WarenGruppen`, `Texte` | Artikel-, Kunden-, Lieferanten- oder Bestandsfragen; Auflösung von Codes und Wertelisten |
| `alphaplan://preise-und-zahlen` | Preisfindung (EK/VK/Staffeln), Statistiktabellen, Reichweiten/Dispo | Preisvergleiche, Umsatz- und Absatzzahlen, Verfügbarkeitsvorschau |
| `alphaplan://historie` | EDI-Eingang, `AenderungsProtokolle`, `Lagerprotokoll` | Herkunft eines Belegs, Änderungshistorie, Bestandsbewegungen |
| `alphaplan://sql-muster` | erprobte Query-Muster und Performance-/Indexlage | vor dem Schreiben jeder größeren Query |
| `alphaplan://wissensluecken` | ungeklärte Felder und nicht erschlossene Subsysteme | wenn eine Frage ins Leere läuft |

---

## Konventionen

* **Immer** `?`-Platzhalter (pyodbc), niemals String-Interpolation von User-Input.
* Lese-/Reportabfragen dürfen `WITH (NOLOCK)` verwenden.
* `IstArchiviert = 0` filtert – aber je Belegtyp völlig unterschiedlich hart (Belegtypen → `alphaplan://belege`).
* Join-Key ist **`BelegeID`**, nicht `Belegnummer`.
* Die DB-Collation ist case-insensitive; die reale Schreibweise weicht an mehreren
  Stellen von der intuitiven ab (`Belege.Belegnummer` klein-n, `Belege.AnsprechPartner`
  groß-P). Queries funktionieren trotzdem, Spaltenvergleiche im Code nicht.
* Es gibt eine Dummy-Zeile mit `BelegTyp = 0`, leerer Belegnummer und Datum
  `1900-01-01`. Auch die meisten Satellitentabellen haben genau eine solche Zeile.
  Datumsfilter auf `> '1900-01-01'` schließen sie aus.
* `Datum` kann in der Zukunft liegen (Belege werden vordatiert angelegt).

---

---

## Belegtypen – Kurzreferenz

`Belege.BelegTyp` ist `tinyint`. Die Belegnummer trägt das Kürzel als Präfix
(`AB2618895`), Schema `<Kürzel><JJ><laufend>`. Vollständige Tabelle mit Beständen,
Archivierungsquoten und der Belegkette: `alphaplan://belege`.

| Typ | Kürzel | Name | Typ | Kürzel | Name |
|---|---|---|---|---|---|
| 1 | AA | Anfragen | 14 | AV | Auftragsvorerfassungen |
| 2 | AN | Angebote | 16 | RK | RMA Kunde |
| 3 | AB | Aufträge | 17 | RL | RMA Lieferant |
| 4 | AL | Lieferscheine | 18 | AS | Service Verkauf |
| 5 | AR | Rechnungen | 19 | ES | Service Einkauf |
| 6 | AG | Gutschriften | 21 | EG | Lieferantengutschriften |
| 8 | EA | Lieferantenanfragen | 23 | AU | Umbuchungen |
| 9 | EB | Bestellungen | 25 | EL | Eingangslieferscheine |
| 10 | ER | Warenzugänge | 27 | EF | Lieferavise |
| 13 | EV | Bestellvorschläge | 7, 11, 12, 15, 20, 22, 24, 26 | | ungenutzt oder Einzelfälle |

Verkaufsseite `A*`, Einkaufsseite `E*`. AV ist mit deutlichem Abstand der größte
Belegtyp, davor AL und AR; AB ist der fachlich wichtigste.

Das MCP-Tool `list_beleg_positionen` verwendet dieselben Kürzel: `AA` = 1 (Verkaufsanfragen),
`EA` = 8 (Lieferantenanfragen). Archivierte Belege schließt es standardmäßig bei **allen**
Belegtypen aus, auch bei AB – für die volle Historie `include_archived=true` setzen.

---

## Join-Pfade

```
Belege ──BelegeID──> BelegePositionen ──ArtikelID──> Artikel ──> Lager
   │                                                   (LagerOrteID = AbbuchungsLagerOrteID
   │                                                    AND ArtikelID = ArtikelID)
   ├──AuftragsAdressenID──> Adressen (SuchName, AdressNummer, IstLieferant)
   ├──LieferAnschriftenID──> Anschriften ──LaenderID──> Laender
   ├──WaehrungenID──────────> Waehrungen        (Kürzel nur hier)
   ├──SrcBelegeID───────────> Belege            (Vorgängerbeleg)
   └──BelegeID = XDateien.RecordID              (nur mit TableName = 'Belege')
```

---

---

## Fallstricke

* **Archivierung ist typabhängig** (Belegtypen → `alphaplan://belege`). `IstArchiviert = 0` bei AB/AL/AV/EB entfernt
  fast den gesamten Bestand; bei AR fast nichts.
* **`BelegePositionen` ohne `BelegTyp`-Filter** misst zu 79 % Auftragsvorschläge.
* **`NochZuUebernehmen > 0`** trifft auf 91 % der Positionen zu und ist allein kein
  Offen-Filter.
* **RTF steckt nicht in `PositionsText`** (nvarchar 30, 0 RTF-Treffer), sondern in
  `ZusatzText1..3`, `ArtikelBezeichnung`, `Vorsatz`, `Nachsatz`, `Hinweistext*`,
  `Artikel.LangText`.
* **Kein Währungskürzel im Beleg** – Join auf `Waehrungen` nötig.
* **`KursFaktor` ist immer 1**, der Kurs steht im `KursDivisor`. `WHERE KursFaktor <> 1`
  liefert garantiert nichts.
* **Die `X`-Spalten rechnen nicht um** – sie sind wertgleich mit dem Original.
* **VK-Werte in Belegwährung, Einstand in EUR** – Margenrechnung nur nach Umrechnung.
* **`NochZuUebernehmen` ist bei Endbelegen (AL, AR, ER, EA) bedeutungslos** und steht
  dort dauerhaft auf der Gesamtmenge.
* **`IstLieferant` verfehlt 43 % der Lieferanten** – lieber über `BelegTyp` trennen.
* **`Texte` bildet die realen Werte nicht zuverlässig ab** (Texte → `alphaplan://stammdaten`).
* **`list_artikel` blendet per Default 92 % des Artikelstamms aus** (`TCC%`).
* **`Feld02` = `grün` mit Umlaut**, nicht `gruen`.
* **Ein Artikel kann in mehreren Lagerorten liegen** – `TOP (1)` über das Abbuchungslager
  ist nicht der Gesamtbestand, und Sperr-/Webshop-/Strategielager zählen nicht in die
  Lagersummen (Stammdaten → `alphaplan://stammdaten`).
* **`Sachbearbeiter` ist Freitext**, gut 70.000 Belege ab 2024 haben gar keinen (Sachbearbeiter → `alphaplan://belege`).
* **`AdressGruppenID` statt `IstLieferant`** zur Kunde/Lieferant-Trennung (Stammdaten → `alphaplan://stammdaten`).
* **`Anschriften` sind Belegkopien**, keine Stammdaten – über 250.000 Zeilen für nur
  wenige tausend Adressen (Stammdaten → `alphaplan://stammdaten`).
* **57 % der Warenzugangsbelege haben keine Positionen** – ER ist auch der reine
  Eingangsrechnungsbeleg (Belegmodell → `alphaplan://belege`).
* **MPN-Suche auf Positionen ist ein Full Scan** – erst `ArtikelID` auflösen (Performance → `alphaplan://sql-muster`).
* **Wunschtermin = Verkauf, bestätigter Termin = Einkauf** – nie andersherum filtern (BelegePositionen → `alphaplan://belege`).
* **`NettoPreisEinzel` ist der Stückpreis**, die übliche Anzeige aber je 1000 Stück.
* **Statistiktabellen sind vorberechnet** und können von Live-Auswertungen abweichen (Statistik → `alphaplan://preise-und-zahlen`).
* **Preisfindung nicht nachbauen** – `VerkaufspreisPrioritaeten` ist ein 25-Flag-Regelwerk (Preisfindung → `alphaplan://preise-und-zahlen`).
* **`BelegTypen` niemals über `BelegTypenID` joinen**, sondern über `BelegTyp` (Belegtypen → `alphaplan://belege`).
* **Textzeilen mitzählen:** ohne `PositionsTyp = 0` laufen Kommentarzeilen in Summen mit.
* **Kein Kundenname im Beleg** – Join auf `Adressen` über `AuftragsAdressenID`.
* **Mengen sind VE, nicht Stück** – Umrechnung über `BEsProVE`.
* **`K_VPE` kann 0 oder NULL sein** (0,07 % der Positionen ab 2024) → `NULLIF`.
* **Freitextpositionen ohne `ArtikelID`** (0,06 % ab 2024) fallen bei INNER JOIN auf
  `Artikel` raus.
* **Preis 0** bei 1,6 % der Positionen ab 2024 – bei Preisstatistiken ausschließen.
* **Dummy-Zeile** `BelegTyp = 0`, Datum `1900-01-01`.
* **Zweckentfremdete Felder** bei Zollner-Abrufen: `IhreAnfrage` = VK-Netto,
  `InterneBelegNummer` = Zielmenge, `BelegTitel` enthält die MPN.
* **`AnlieferungsDatumWunsch = 31.12.`** markiert Abruf-Sammelpositionen, kein Termin.
* **Reale Spaltenschreibweise** beachten: `Belegnummer`, `AnsprechPartner`,
  `K_ChanceOfSucces`.

---

Ausführliche Begründung zu jedem Punkt in der jeweils genannten Datei.
