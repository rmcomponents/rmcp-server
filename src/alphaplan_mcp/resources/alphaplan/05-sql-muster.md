# AlphaPlan – Query-Muster & Performance

> Teil der AlphaPlan-SQL-Referenz. Einstieg, Konventionen und Fallstricke: `alphaplan://start`.
> Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind markiert.

---

## Query-Muster

### Angebotsauswertung (DQR)

```sql
SELECT
    Belege.Datum,
    BelegePositionenID,
    Belege.BelegeID,
    Belege.Belegnummer,
    Adressen.SuchName                 AS Kunde,
    BP.ArtikelNummer,
    CONVERT(INT, BP.VEAnzahl)         AS Menge,
    K_GeschaeftsArt                   AS GeschaeftsArt,
    K_Quotierungsphase                AS Quotierungsphase,
    K_ChanceOfSucces                  AS ChanceOfSuccess,
    K_AuftragsArt                     AS AuftragsArt,
    COALESCE(ZusatzText1, '')         AS Lieferzeit,
    COALESCE(ZusatzText2, '')         AS Ergebnis,
    COALESCE(ZusatzText3, '')         AS KommentarZuErgebnis,
    COALESCE(K_Kommentar, '')         AS Kommentar,
    Belege.BestellNummer              AS SOP,
    Belege.Kommission                 AS EOP,
    K_AngebotePositionenBenchmark1    AS BenchDistri,
    K_AngebotePositionenBenchmark2    AS BenchZP,
    COALESCE(D.Anzahl, 0)             AS DateienAnzahl,
    Belege.Sachbearbeiter,
    Belege.IhrAuftrag                 AS Abgabeart,
    Belege.IhreAnfrage,
    Belege.BelegTitel                 AS Endkunde,
    BP.PositionsText                  AS Position,
    Belege.AnsprechPartner            AS Projekt
FROM ALPHAPLAN_EchtDaten.dbo.BelegePositionen AS BP WITH (NOLOCK)
JOIN ALPHAPLAN_EchtDaten.dbo.Belege WITH (NOLOCK)
     ON Belege.BelegeID = BP.BelegeID
    AND Belege.BelegTyp = 2                       -- AN
JOIN ALPHAPLAN_EchtDaten.dbo.Adressen WITH (NOLOCK)
     ON Belege.AuftragsAdressenID = Adressen.AdressenID
LEFT JOIN (
    -- Dateianhänge vorab aggregieren, sonst vervielfacht der Join die Positionen
    SELECT COUNT(*) AS Anzahl, RecordID AS BelegeID
    FROM ALPHAPLAN_EchtDaten.dbo.XDateien WITH (NOLOCK)
    WHERE TableName = 'Belege' AND IstArchiviert = 0
    GROUP BY RecordID
) AS D ON D.BelegeID = Belege.BelegeID
WHERE Belege.IstArchiviert = 0                    -- blendet 94 % der Angebote aus!
  AND Belege.Sachbearbeiter LIKE ?
  AND Belege.Datum >= DATEFROMPARTS(?, ?, 1)
ORDER BY Belege.Datum DESC, Belege.Belegnummer DESC;
```

Parameter: Sachbearbeiter-Pattern (inkl. `%`), Jahr, Monat.

### Offene EB-Menge im Wareneingangsfenster

```sql
-- nächster Mittwoch: an Mo–Mi der laufenden, ab Do der Folgewoche
DECLARE @nextWednesday DATE = (
    SELECT DATEADD(day,
        (DATEDIFF(day, 0, GETDATE()) / 7) * 7 + 3
        - DATEDIFF(day, '19000101', GETDATE()) % 7
        + CASE WHEN DATEPART(dw, GETDATE()) < 4 THEN 0 ELSE 7 END,
        '19000101')
);
DECLARE @today DATE = GETDATE();

SELECT COALESCE(SUM(NochZuUebernehmen), 0)
FROM ALPHAPLAN_EchtDaten.dbo.BelegePositionen BP WITH (NOLOCK)
JOIN ALPHAPLAN_EchtDaten.dbo.Belege WITH (NOLOCK)
     ON BP.BelegeID = Belege.BelegeID
    AND Belege.BelegTyp = 9                       -- EB
WHERE BP.ArtikelID = ?
  AND BP.AnlieferungsDatumBestaetigt BETWEEN @today AND @nextWednesday
  AND Belege.IstArchiviert = 0;
```

> `DATEPART(dw, …)` hängt von `SET DATEFIRST` ab. Bei EB sind 95 % der Belege
> archiviert – für offene Wareneingänge ist der Filter dennoch richtig.

### Lagerbestand

**Abbuchungslager** (das, aus dem verkauft wird):

```sql
SELECT TOP (1) L.Bestand, L.Verfuegbar, L.Reserviert, L.Bestellt
FROM ALPHAPLAN_EchtDaten.dbo.Artikel A WITH (NOLOCK)
JOIN ALPHAPLAN_EchtDaten.dbo.Lager L WITH (NOLOCK)
     ON L.LagerOrteID = A.AbbuchungsLagerOrteID
    AND L.ArtikelID   = A.ArtikelID
WHERE A.ArtikelID = ?;
```

**Gesamtbestand über alle Lagerorte** – nötig, sobald Webshop-, Muster- oder
strategisches Lager relevant sind (Stammdaten → `alphaplan://stammdaten`):

```sql
SELECT LO.LagerOrtName,
       L.Bestand, L.Verfuegbar, L.Reserviert, L.Bestellt,
       LO.SollFuerLagersummenBeruecksichtigtWerden AS ZaehltInSummen
FROM ALPHAPLAN_EchtDaten.dbo.Lager L WITH (NOLOCK)
JOIN ALPHAPLAN_EchtDaten.dbo.LagerOrte LO WITH (NOLOCK)
     ON LO.LagerOrteID = L.LagerOrteID
WHERE L.ArtikelID = ? AND L.Bestand <> 0
ORDER BY L.Bestand DESC;
```

### Belegwerte in EUR

```sql
SELECT
    B.Belegnummer,
    B.Datum,
    W.WaehrungsKuerzel,
    BP.ArtikelNummer,
    BP.VEAnzahl,
    BP.NettoPreisGesamt                                          AS WertBelegwaehrung,
    BP.NettoPreisGesamt * B.KursFaktor / NULLIF(B.KursDivisor, 0) AS WertEUR,
    BP.EinstandsPreisGesamt                                      AS EinstandEUR
FROM ALPHAPLAN_EchtDaten.dbo.Belege B WITH (NOLOCK)
JOIN ALPHAPLAN_EchtDaten.dbo.Waehrungen W WITH (NOLOCK)
     ON W.WaehrungenID = B.WaehrungenID
JOIN ALPHAPLAN_EchtDaten.dbo.BelegePositionen BP WITH (NOLOCK)
     ON BP.BelegeID = B.BelegeID
    AND BP.PositionsTyp = 0
WHERE B.BelegeID = ?;
```

> `NettoPreisGesamt` steht in Belegwährung, `EinstandsPreisGesamt` offenbar in EUR.
> Die `…X`-Spalten helfen dabei nicht – sie sind wertgleich mit dem Original (BelegePositionen → `alphaplan://belege`).

---

---

## Performance & Indexe

Die Indexlage entscheidet bei über 10 Mio Positionen über Sekunden oder Minuten.

**`Belege` ist geclustert auf `(BelegTyp, Datum, BelegeID)`** – nicht auf dem
Primärschlüssel, der nur nonclustered vorliegt. Ein Filter `BelegTyp = x AND Datum >= y`
ist damit die günstigste überhaupt mögliche Zugriffsform und sollte in jeder Query stehen.

**`BelegePositionen` ist geclustert auf `(BelegeID, ZeilenPosition, BelegePositionenID)`** –
der Join von `Belege` auf `BelegePositionen` über `BelegeID` ist also optimal.

> **`BelegePositionen.ArtikelNummer` hat keinen Index.** Eine Suche per MPN-String auf den
> Positionen ist ein Full Scan über 10 Mio Zeilen. `Artikel.ArtikelNummer` ist dagegen
> eindeutig indiziert. **Immer erst die `ArtikelID` auflösen und dann über `BP.ArtikelID`
> filtern** – dafür existiert ein passender Index.

```sql
-- langsam: Full Scan
WHERE BP.ArtikelNummer = ?
-- schnell: Indexzugriff
WHERE BP.ArtikelID = (SELECT ArtikelID FROM Artikel WHERE ArtikelNummer = ?)
```

Weitere nutzbare Indizes:

| Tabelle | indizierte Einstiegsspalten |
|---|---|
| `Belege` | `Datum`, `Sachbearbeiter`, `BelegTitel`, `Belegnummer`+`BelegTyp`, `AuftragsAdressenID`, `LieferAdressenID`, `RechnungsAdressenID`, `SrcBelegeID`, `ProjekteID` |
| `BelegePositionen` | `ArtikelID`, `ZeilenPosition`, `SrcBelegeID`, `SrcBelegePositionenID`, `OrgBelegePositionenID`, `AbbuchungsLagerID` |
| `Artikel` | `ArtikelNummer`, `MatchCode`, `Bezeichnung`, `EANNummer`, `WarenGruppenID` |
| `Adressen` | `AdressNummer`, `SuchName`, `AnschriftenID`, `AdressGruppenID` |
| `Lager` | `ArtikelID`, `(ArtikelID, LagerOrteID, Charge)`, `LagerOrteID` |

Nicht indiziert und daher teuer: alle Datumsfelder der Positionen (`LieferDatum` nur als
dritte Spalte im Composite `StapelVerarbeitung`), sämtliche `K_*`-Felder,
`NochZuUebernehmen` als alleinstehendes Prädikat. Ein führendes `LIKE '%…'` verhindert
Indexnutzung generell – `MatchCode`-Suchen mit beidseitiger Wildcard scannen den
Artikelstamm.

---
