# AlphaPlan – Offene Punkte

> Teil der AlphaPlan-SQL-Referenz. Einstieg, Konventionen und Fallstricke: `alphaplan://start`.
> Stand der Analyse: 20.08.2026. Nicht verifizierte Aussagen sind markiert.

---

## Offene Punkte

**Inhaltlich ungeklärt**

* `StatistikTyp` in `StatistikenMonat` – steuert, welche Dimension gefüllt ist.
* `Verfuegbar2_00..51` in `ArtikelStatistiken` – 52-Wochen-Vorschau, aber Startwoche und
  Semantik unbekannt.
* Wofür die `X`-Spalten da sind (wertgleich mit dem Original, aber eigene Spalten).
* Bestätigung, dass `EinstandsPreisGesamt` in EUR geführt wird.
* `PositionsTyp` 8 und 10 (RMA / Einzelfall).
* `IstAlsQuelleZugelassen` – nur 4 von 34 Belegkettenschritten gesetzt.
* Aktualisierungsrhythmus der Statistik- und Reichweitentabellen.
* Periodenlänge und Startpunkt von `ReichweitenArtikel.Verfuegbar0..53` bzw.
  `ArtikelStatistiken.Verfuegbar2_00..51` (Wochen? ab wann?).
* `MengenfeldTyp` und `SourceTyp` in `ReichweitenBuchungen`.
* Unterschied `Lager.Verfuegbar` vs. `Verfuegbar2` (1.337 abweichende Zeilen).
* Ob `Belege.AnlageBenutzerID` fachlich dem `Sachbearbeiter` entspricht.
* Bestätigung des Anschriften-Snapshot-Modells über `ReferenzZaehler`.

**Noch nicht angefasste Subsysteme**

* **Dispo/Reichweiten** – `ReichweitenBuchungen` (4,7 Mio), `ArtikelReichweitenTage`
  (3,2 Mio), `ReichweitenAusgewertet` (2,8 Mio), `ReichweitenArtikel`,
  `ReichweitenGruppiert`, `ReichweitenVerfuegbar`, `ReichweitenTypen`, `Reichweiten`.
* **EDI** – `EDI_QuellDaten` (22,4 Mio), `EDI_BelegePositionen` (9,7 Mio), `EDI_Belege`,
  `EDI_Sendungen`: der Weg, auf dem Bestellungen hereinkommen.
* **Audit** – `AenderungsProtokolle` (31,9 Mio), `ArtikelVorgaenge` (10,7 Mio),
  `AdressenVorgaenge`, `Lagerprotokoll` (14,9 Mio): wer hat wann was geändert.
* **Chargenverwaltung** – `K_LotNummern` (1,29 Mio) bei nur 2 chargengeführten Lagerzeilen:
  wofür wird die Tabelle gefüllt?
* **Produktion** – `Produktionen*`, `Stuecklisten`, `ArtikelSets`.
* **FIBU** – `FibuBelege` (454 k), `Zahlungen` (249 k), `FibuAdressen`, offene Posten.
* **Termine/CRM** – `Termine` (102 k), `SupportInfos`, `Projekte`, `K_EmailProtokoll` (212 k).
