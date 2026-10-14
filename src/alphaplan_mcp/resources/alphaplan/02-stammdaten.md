# AlphaPlan – Stammdaten & Texte

> Teil der AlphaPlan-SQL-Referenz. Einstieg, Konventionen und Fallstricke: `alphaplan://start`.
> Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind markiert.

---

## Stammdaten

### `Artikel` (rund 300.000 Zeilen, 211 Spalten)

| Spalte | Bedeutung |
|---|---|
| `ArtikelID` / `ArtikelNummer` (nvarchar 30) | PK / MPN |
| `Bezeichnung` (70) / `LangText` (max) | Kurz-/Langtext, LangText ist RTF-Kandidat |
| `MatchCode` (60) | Suchfeld, vom MCP-Tool `list_artikel` per `LIKE '%…%'` gefiltert |
| `K_VPE` (int) | Verpackungseinheit; kann 0/NULL sein → `NULLIF(K_VPE, 0)` |
| `Feld01..Feld04` | Freifelder. **`Feld02` = Lieferfähigkeits-Status**, siehe unten |
| `HauptLieferantenAdressenID`, `HerstellerAdressenID`, `HerstellerNummer` | Bezugsquellen |
| `AbbuchungsLagerOrteID`, `ZubuchungsLagerOrteID` | Lagerzuordnung, Join-Key zu `Lager` |
| `EinstandsPreis`, `LetzterEK`, `LetzterEKDatum`, `LetzterEKAnzahl`, `LetzterEKAdressenID`, `LetzterEKBelegeID` | Einkaufshistorie am Stamm |
| `EmpfohlenerVerkaufspreis` | |
| `Wiederbeschaffungszeit`, `K_Wiederbeschaffungszeit`, `LieferDauer`, `SicherheitsTage`, `SollLagerReichweite` | Dispo |
| `IstAuslaufArtikel`, `DatumProduktabkuendigung`, `DatumProduktabkuendigungsmitteilung`, `DatumLetzterMoeglicherKauf`, `DatumLetzterMoeglicherVersand` | **EOL-Daten** |
| `IstGesperrt`, `IstGesperrtFuerNeueVKBelege`, `…EKBelege`, `IstGesperrtVKGrund`, `…EKGrund` | Sperren |
| `K_ArtikelnummerAlt`, `K_ArtikelnummerNeu` | MPN-Umstellungen |
| `K_BeschaffungsTyp` | z. B. `strategisch` |
| `RoHS`, `RoHSTyp`, `REACH`, `ZollTarifNummer`, `HerkunftsLaenderID` | Compliance |
| `WarenGruppenID` | → `WarenGruppen` (314, schemastabil) – **die faktische Hersteller- und Produktklassifikation**, siehe unten |
| `NichtStornierbarKeineRueckgabe` | NCNR |
| `IstArchiviert` | |

**Bestandszahlen:** rund 300.000 Artikel, `ArtikelNummer` ist eindeutig (so viele distinct
wie Artikel). Ein paar tausend archiviert, ~1.000 ohne `K_VPE`, ~130 ohne Hauptlieferant,
20 MPNs mit führendem/abschließendem Leerzeichen. Einige tausend Auslaufartikel, aber nur
**ein einziger** mit gepflegtem `DatumProduktabkuendigung` – die EOL-Datumsfelder sind
faktisch leer, `IstAuslaufArtikel` ist der einzige brauchbare EOL-Indikator.

> **`list_artikel` filtert per Default `ArtikelNummer NOT LIKE 'TCC%'` und entfernt damit
> rund 92 % des Stamms.** `TCC`-Artikel sind neu angelegte
> Teile, die noch nicht vertrieben werden; der Default ist für Vertriebs- und
> Beschaffungsfragen also richtig. Für Fragen zum Katalogumfang, zu Neuanlagen oder zur
> Stammdatenqualität muss man `negative_filter_term=None` setzen – sonst betrachtet man
> ~8 % des Bestands und hält sie für das Ganze.

> **Die `TCC`-Zahlen oben sind Vorfilter-Zahlen.** `list_artikel` wendet zusätzlich zum
> `TCC%`-Ausschluss immer auch `IstArchiviert = 0` an: Archivierte Artikel (ein paar
> tausend von rund 300.000, also knapp 2 %) sind per Default **nicht** im Ergebnis.
> Die tatsächliche Ausgabe des Tools liegt daher unter der oben genannten Zahl.
> Beide Schnitte sind unabhängig – auch mit `negative_filter_term=None` bleiben
> Archivierte draußen, bis man `include_archived=true` setzt.

**`WarenGruppen` (314 Zeilen, schemastabil – kann sich bei Klassifikationspflege
verschieben)** ist trotz `WarenGruppenParentID` und `Ebene` flach
(alle auf Ebene 1). Struktur:

* `KostenTraeger` trägt den **Hersteller** – Taiyo Yuden, HolyStone, TaiTech, Mercury,
  CapXon, Elna, Walsin, CCTC, Kaga Fei, Wittig, Varta, Hitachi, Vishay …
* `Bezeichnung` die Produktfamilie (`hochkapazitive MLCC`, `Ferritperlen`, `Schottky Diode`)
* `WarenGruppenNummer` den Code (`TY_CE standard`, `HS-Aktive_SD_AECQ`, `TaiTech_TMPC AECQ`)

Automotive-Ware liegt in eigenen Gruppen mit Suffix `AECQ`, Industrie-/Medizinqualität als
`8 High Grade`. Daneben nichttechnische Gruppen: `RMC_Kosten Transport` (Fracht,
Verpackung), `RMC_GiveAways`, `RMC_Partner` (Dienstleistungen), `Muster_Rohmaterial`,
`Lager` (Verbrauchsmaterial). **Wer Umsatz nach Hersteller auswerten will, geht über
`WarenGruppen.KostenTraeger`** – ein Herstellerfeld am Artikel gibt es nur als
`HerstellerAdressenID`/`HerstellerNummer`, deren Pflegegrad ungeprüft ist.

`EinheitenSets` (15, schemastabil) hängen ebenfalls an der Warengruppe und am Artikel: `VPE 250`,
`VPE 500`, … `VPE 15000`, `Standard ohne VPE`, `Fracht`. Die zugehörigen `Einheiten`
tragen `Teilbarkeit` = Losgröße und `Preiseinheit`; das ist die zweite, ältere
Repräsentation der Verpackungseinheit neben `Artikel.K_VPE`.

**`Feld02` (Lieferfähigkeits-Status)** ist ein Freitextfeld, kein Enum. Reale Verteilung
(Größenordnungen):

| Wert | Anzahl (~) |
|---|---|
| `grün` (mit Umlaut!) | 295k |
| `Active` | 4k |
| leer | 4k |
| `NRND` | 1k |
| `rot` | 0,9k |
| `läuft nicht` | 0,8k |
| `0` | ~40 |
| `gelb` | ~7 |
| Sonstiges | ~14 |

Die Tool-Dokumentation von `list_artikel` nennt `rot`/`gelb`/`gruen` – real steht dort
**`grün` mit Umlaut**, ein Filter auf `gruen` trifft nichts. `gelb` existiert praktisch
nicht; die englischen Werte `Active`/`NRND` bilden eine zweite, parallele
Vokabelwelt. Unter Sonstiges stehen versehentlich eingetragene Preise
(`0,168 USD`, `17,10 USD (Sonderpreis)`) und ein Tippfehler `NDNR`.

### `Adressen` (einige tausend Zeilen, 248 Spalten)

Kunden **und** Lieferanten in einer Tabelle.

| Spalte | Bedeutung |
|---|---|
| `AdressenID` / `AdressNummer` (nvarchar 10) | PK / fachliche Nummer |
| `SuchName` (nvarchar 33) | **der Anzeigename** – keine getrennte Namensspalte hier |
| `IstLieferant` (bit) | Kunde vs. Lieferant – **unzuverlässig**, siehe unten |
| `KundenTyp` (255) | Finale Klassifizierung als Kurzwert: `KeyAccount`, `A`–`D`, `kein Potential`, `? (unbekannt)`, `Distributor`, `TAIYO` (die Langtexte „Finale Klassifizierung – 1 – KeyAccount" stehen nur in der Werteliste `Texte`) |
| `K_KundenTypNeu`, `K_KundentypAlt` | Distributor / EMS-Dienstleister / Entwicklung / Fertigung / Handel / Universität … |
| `Branche` (60) | Automotive, Medical, Industrial, Telecom, Luft- & Raumfahrt … |
| `Kategorie` (1) + `Beruf` | Potential- und Umsatz-Level-Codes |
| `K_Vertriebskanal` | EDI, Webshop, API, Kundenportale, Luminovo, SAP Arriba … |
| `K_IstZielkunde`, `K_IstJPA`, `K_IstBonusVereinbarung` | Vertriebsflags |
| `AnschriftenID` | → `Anschriften` (über 250.000) → `Laender` (272, schemastabil: kann sich bei Schema-Änderung verschieben) |
| `WaehrungenID`, `SprachenID`, `ZahlungsBedingungenID`, `VersandArtenID` | Vorbelegungen |
| `KreditLimit`, `ZahlungsMoral`, `Mahnbar` | Bonität |
| `LieferzeitVorgabeEK`, `LieferDauer` | |
| `IstArchiviert` | |

> **`IstLieferant` ist kein verlässlicher Filter.** Nur eine Handvoll der Adressen trägt das
> Flag, aber deutlich mehr Adressen haben EK-Belege – 43 % davon sind nicht als Lieferant
> markiert (Stand der Analyse). Umgekehrt haben einzelne der markierten Lieferanten auch
> VK-Belege.

**Besser: `AdressGruppenID`.** Die 9 Gruppen sind die eigentliche Klassifikation:

| ID | `AdressGruppe` | Name | Bemerkung |
|---|---|---|---|
| 2 | 100 | Kunden | Standardwährung EUR |
| 3 | 700 | **Lieferanten** | `IstLieferant = 1`, Standardwährung **JPY** |
| 4 | 200 | Interessenten | |
| 5 | 300 | eigene Adressen | |
| 6 | 400 | Diverse Adressen | |
| 7 | 800 | Taiyo-Distributoren | |
| 8 | 900 | Hersteller | |
| 9 | 600 | Webshop Kunden | eigenes Abbuchungslager (LagerOrteID 10) |

Die Gruppe liefert auch Vorbelegungen (Zahlungs-/Versandbedingungen, Währung, Preisstufe,
Mahnstufen), die auf der Adresse überschrieben werden können. Für eine belastbare
Kunde/Lieferant-Trennung bleibt der `BelegTyp` des Belegs der sicherste Weg
(1–6, 14, 18 = Verkauf; 8–10, 13, 19, 21 = Einkauf).

### Anschriften – Snapshot-Modell

`Anschriften` (über 250.000 Zeilen) enthält die reinen Adressdaten (`Name1..3`, `Strasse`, `HausNummer`,
`Postleitzahl`, `Ort`, `LaenderID`, `Telefon`, `Telefax`, `Email`, `Internet`,
`LieferHinweis`). **Eine Viertelmillion Anschriften bei nur einigen tausend Adressen**:
Belege bekommen offenbar eine eingefrorene Kopie der Anschrift, damit spätere
Stammdatenänderungen alte Belege nicht verändern *(erschlossen aus dem Mengenverhältnis
und `ReferenzZaehler`, unverifiziert)*.

Die vier schmalen Zuordnungstabellen sind dagegen die am Stamm hinterlegte Auswahlliste:

| Tabelle | Zeilen (~) | Verknüpfung |
|---|---|---|
| `LieferAnschriften` | ~1.000 | `AdressenID` → `AnschriftenID`, mit `IstStandard` |
| `RechnungsAnschriften` | ~330 | dito |
| `AuftragsAnschriften` | ~13 | dito |
| `LieferAdressen` / `RechnungsAdressen` | 7 / 3 | `AdressenID` → `AdressenID2`, also abweichender Empfänger als eigene Adresse |

`Ansprechpartner` (rund 20.000) hängt über `AdressenID` an der Adresse und über `AnschriftenID`
an einer eigenen Anschrift; interessant sind `Abteilung`, `Position`, `SuchName`,
`AusgeschiedenAm`, `IstZeichnungsBerechtigt` und die Einwilligungsflags
(`AkzeptiertNewsletter`, `AkzeptiertWerbung` …).

`Laender` (272, schemastabil: kann sich bei Schema-Änderung verschieben) trägt neben
`LaenderKuerzel` auch **`IstEU`**, `ISOCode` und
`IstLieferschwelleErreicht` – für USt-Prüfungen brauchbarer als ein Vergleich auf `'D'`.

### `Lager` (rund 300.000 Zeilen) und `LagerOrte` (10, schemastabil)

| ID | LagerOrtName | Zeilen (~) | mit Bestand (~) | zählt in Lagersummen |
|---|---|---|---|---|
| 2 | **RM-Lager** (`IstStandard = 1`) | 307k | 0,9k | ja |
| 10 | RM-Webshoplager | 0,8k | 0,8k | **nein** |
| 9 | RM-Strategisches Bestandslager | 0,5k | ~100 | **nein** |
| 4 | RM-Musterlager RMC | 0,5k | ~130 | ja |
| 3 | RM-Sperrlager | ~130 | ~15 | **nein** |
| 5–8 | RM-Musterlager AUTO SC RM 50/60/80/90 | je ~60 | je ~16 | ja |

> **`SollFuerLagersummenBeruecksichtigtWerden = 0`** bei Sperr-, Webshop- und
> strategischem Lager: deren Bestände fließen nicht in `LagerortSummeBestand` ein.
> Webshopbestände sind mit einigen hundert bestandsführenden Zeilen kein Randfall.

Das Hauptlager führt eine Zeile je Artikel; die Nebenlager nur für tatsächlich bevorratete
Teile. **Ein Artikel kann also in mehreren Lagerorten liegen** – ein `TOP (1)` über
`Artikel.AbbuchungsLagerOrteID` liefert nur das Abbuchungslager, nicht den Gesamtbestand.

Chargenführung ist praktisch ungenutzt (nur vereinzelte Zeilen mit `Charge`),
`EisernerBestand` ist durchgängig 0. `Verfuegbar` und `Verfuegbar2` weichen bei einer
kleinen Anzahl Zeilen voneinander ab – der Unterschied ist nicht dokumentiert
*(unverifiziert)*.

Mengenspalten: `Bestand`, `Verfuegbar`, `Verfuegbar2`, `Reserviert`, `Reserviervorschlag`,
`Bestellt`, `Bestellvorschlag`, `Angeboten`, `QSBestand`, `RMABestand`, `BestandBeimKunden`,
`MindestBestand`, `SollBestand`, `EKLieferavisBestand`, `LagerortSummeBestand`,
`LagerortSummeBestandOhneEB`.

### `Waehrungen` (4 Zeilen, schemastabil)

| ID | Kürzel | Text | KursFaktor | KursDivisor | Standardwaehrung |
|---|---|---|---|---|---|
| 0 | | *(Dummy)* | 0 | 0 | 0 |
| 1 | USD | US Dollar | 1 | 1,1605 | 0 |
| 112 | EUR | Euro/€ | 1 | 1 | **1** |
| 113 | JPY | Yen | 1 | 184,62 | 0 |

Hauswährung ist **EUR** (`WaehrungenID = 112`, `Standardwaehrung = 1`). Umrechnung:

```
Betrag_EUR = Betrag_Belegwährung * KursFaktor / KursDivisor
```

`KursFaktor` ist in allen vier Zeilen 1 – der Kurs steht ausschließlich im `KursDivisor`
(USD 1,1605 → 100 USD = 86,17 EUR). Filter auf Fremdwährungsbelege daher über
`KursDivisor <> 1`, **nicht** über `KursFaktor <> 1` (das liefert keine Treffer).
`BoersenKursFaktor`/`-Divisor`/`-Datum` halten den tagesaktuellen Kurs am Stamm,
`Belege.KursFaktor`/`-Divisor` den zum Belegdatum eingefrorenen. Reale Belegkurse
streuen entsprechend (JPY-Divisor zwischen ca. 155 und 187). Auch die Divisor-Werte
oben sind Momentaufnahmen und verschieben sich mit den Wechselkursen.

Verteilung der Positionen ab 2024: EUR und JPY je grob 4 Mio, USD grob 2,4 Mio –
**Fremdwährung ist der Normalfall, nicht die Ausnahme.** Nur eine kleine Anzahl
Positionen hängt an Belegen mit `WaehrungenID = 0` (Dummy-Zeile, kein Kürzel).

---

---

## `Texte` – der Lookup-Mechanismus (rund 760 Zeilen, schemastabil: wächst mit der Klassifikationspflege)

Zentrale Werteliste für Enum-Felder **und** Textbaustein-Bibliothek. Spalten:
`TableName`, `FieldName`, `TextText`, `LangText`, `Beschreibung`, `ShortCut`,
`Sortierung`, `ParentFieldName`, `ParentTextText`, `IstArchiviert`.

Zwei grundverschiedene Nutzungsmuster – das zu verwechseln ist die häufigste Fehlerquelle:

**a) Enum-Feld:** `TextText` **ist** der Wert, der in der Fachspalte steht.

```sql
SELECT TextText, Sortierung
FROM ALPHAPLAN_EchtDaten.dbo.Texte WITH (NOLOCK)
WHERE FieldName = 'K_Quotierungsphase' AND IstArchiviert = 0
ORDER BY Sortierung;
-- 1 - AN-Erstellung / 2 - AN bei Kunde / 3 - nachgefragt /
-- 4 - Nachverhandlung / 5 - abgeschlossen
```

**b) Textbaustein:** `TextText` ist der **Name** der Vorlage, `LangText` der Inhalt
(oft RTF). Betrifft `Vorsatz`, `Nachsatz`, `ZusatzText1..3`, `ArtikelBezeichnung`,
`Hinweistext*`. Hier gibt es mehrere Zeilen pro `TextText` – die Auswahl braucht
zusätzlich den Vorlagennamen:

```sql
SELECT LangText
FROM ALPHAPLAN_EchtDaten.dbo.Texte WITH (NOLOCK)
WHERE FieldName = 'ZusatzText2' AND TextText = 'AN_Ergebnis Quote';   -- rund ein Dutzend Zeilen
```

Vorlagennamen folgen der Konvention `<Belegkürzel>_<Sprache> - <Zweck>`
(`AB_DE - Hinweis ALLOKATION`, `AN_LZ - EN 20-25`, `EA_Taiyo`).

Belegte `FieldName`-Werte u. a.: `K_Quotierungsphase`, `K_ChanceOfSucces`, `K_GeschaeftsArt`,
`K_AuftragsArt`, `K_StornoGrund`, `K_KundenTypNeu`, `K_Vertriebskanal`, `K_BeschaffungsTyp`,
`K_PacklistenStatus`, `AuftragsTyp`, `Branche`, `KundenTyp`, `Kategorie`, `Beruf`, `Abteilung`,
`Sachbearbeiter`, `Position`, `Anrede`, `Masseinheit`, `Medium`, `PreisStufe`, `Groesse`,
`IhrAuftrag`, `BelegTitel`, `Ergebnis`, `Thema`, `Verursacher`, `ProjektPhase`.

> **`Texte` ist nicht autoritativ für die tatsächlich gespeicherten Werte.** Bei
> `K_GeschaeftsArt` enthält `Texte` die Werte Bestand / neu / Projekt / Spotorder – in
> den Positionen stehen aber Neugeschäft (größte Gruppe, Zehntausende), Bestandsgeschäft
> (ebenfalls Zehntausende, deutlich kleiner) und Projektgeschäft (nur einige tausend),
> die in `Texte` komplett fehlen; von der gepflegten Liste kommt nur Spotorder real vor,
> und zwar nur ein paar Mal. Die Liste wurde offenbar umbenannt,
> ohne die Bestandsdaten zu migrieren. **Für echte Wertelisten immer
> `SELECT DISTINCT <spalte>` auf den Nutzdaten fahren, `Texte` nur ergänzend.**
>
> Auch innerhalb gepflegter Felder gibt es Ausreißer: `K_Quotierungsphase` enthält neben
> den fünf definierten Phasen vereinzelte Rohwerte (2, 1) sowie eine
> versehentlich eingetragene MPN.

> Datenqualität: Dubletten und Tippfehler sind real vorhanden – `Sachbearbeiter` mit
> Leerzeichen am Ende, `Artikelbezeichnung` vs. `ArtikelBezeichnung`, in
> `K_ChanceOfSucces` sowohl `02 - 10%` als auch `02 - 100%`. Vergleiche daher
> tolerant (TRIM, case-insensitiv).

---
