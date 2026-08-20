# AlphaPlan – EDI & Protokolle

> Teil der AlphaPlan-SQL-Referenz. Einstieg, Konventionen und Fallstricke: `alphaplan://start`.
> Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind markiert.

---

## EDI & Änderungsprotokoll

### EDI – wie Bestellungen hereinkommen

| Tabelle | Zeilen (~) | Inhalt |
|---|---|---|
| `EDI_Belege` | ~420.000 | Staging der eingehenden Nachricht, 99 Spalten |
| `EDI_BelegePositionen` | ~10 Mio | deren Positionen |
| `EDI_QuellDaten` | ~22 Mio | Rohnachricht als Text (`QuellDaten` nvarchar(max)) je `EDI_BelegeID` |
| `EDI_BelegeAdressen` | 3 | Adressen je Nachricht (`AdressTyp`) |
| `EDI_Sendungen` | ~100.000 | ausgehende Nachrichten |
| `EDI_Einstellungen` | 3 | Provider, Import-/Exportpfade |

`EDI_Belege` hält die Nachricht vollständig denormalisiert: Auftrags-, Liefer-, Rechnungs-
und Warenempfängeradresse jeweils als Klartextblock (`A_*`, `L_*`, `R_*`, `UD_*`) samt ILN,
dazu `BestellNummer`, `KundenAuftragsNummer`, `RahmenVertragsNummer`, `Provider`,
`IstWebShop`, `IstTestNachricht`, `DateiName`.

Statusfelder: `IstVerarbeitet`, `IstFreigegeben`, `IstManuellVeraendert`, `Status`,
`FehlerText`. **`EDI_Belege.BelegeID` verweist auf den erzeugten Beleg** – damit lässt sich
für jeden Auftrag feststellen, ob er per EDI oder Webshop kam und wie die Rohbestellung aussah.
Umgekehrt trägt `Belege.EDI_BelegeID` den Rückverweis.

### `AenderungsProtokolle` (~32 Mio) – der Audit-Trail

| Spalte | Inhalt |
|---|---|
| `DateAndTime`, `UserID`, `SessionUUID` | wer, wann, in welcher Sitzung |
| `TableName` + `RecordID` | der geänderte Datensatz |
| `MasterTableName` + `MasterRecordID` | der zugehörige Kopf – bei Positionsänderungen `Belege` + `BelegeID` |
| `FieldsAndValues` | die Änderungen als **Freitext** im Format `Feld:'alt'->'neu'`, komma-separiert |
| `Version` | Transaktionsnummer; alle Zeilen einer Speicherung teilen sie |

`MasterRecordID` ist der praktische Einstieg: alle Änderungen an einem Beleg **inklusive
seiner Positionen** hängen an derselben `BelegeID`.

```sql
SELECT DateAndTime, UserID, TableName, RecordID, FieldsAndValues
FROM ALPHAPLAN_EchtDaten.dbo.AenderungsProtokolle WITH (NOLOCK)
WHERE MasterTableName = 'Belege' AND MasterRecordID = ?
ORDER BY DateAndTime DESC;
```

> `FieldsAndValues` ist nicht strukturiert und enthält bei Textfeldern komplette
> RTF-Blöcke – eine Auswertung einzelner Felder erfordert Parsing und ist fehleranfällig.
> Für Fragen wie „wer hat den Preis geändert" ist die Tabelle brauchbar, für Statistiken
> darüber nicht.

Verwandte Protokolle: `Lagerprotokoll` (~15 Mio) mit `BestandAlt`/`BestandDiff`/`BestandNeu`,
`BuchungsTyp`, `BuchungsText`, `LagerEP`, `BewertungsEP` je Bewegung – **strukturiert und
damit weit besser auswertbar als `AenderungsProtokolle`**. Dazu `ArtikelVorgaenge` (~11 Mio,
reine Verknüpfungstabelle Artikel ↔ Beleg/Termin/Support) und `AdressenVorgaenge`.

---
