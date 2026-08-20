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
* `IstAlsQuelleZugelassen` – nur 4 von rund 30 Belegkettenschritten gesetzt.
* Aktualisierungsrhythmus der Statistik- und Reichweitentabellen.
* Periodenlänge und Startpunkt von `ReichweitenArtikel.Verfuegbar0..53` bzw.
  `ArtikelStatistiken.Verfuegbar2_00..51` (Wochen? ab wann?).
* `MengenfeldTyp` und `SourceTyp` in `ReichweitenBuchungen`.
* Unterschied `Lager.Verfuegbar` vs. `Verfuegbar2` (bei einer kleinen Anzahl abweichender Zeilen).
* Ob `Belege.AnlageBenutzerID` fachlich dem `Sachbearbeiter` entspricht.
* Bestätigung des Anschriften-Snapshot-Modells über `ReferenzZaehler`.

**Noch nicht angefasste Subsysteme**

* **Dispo/Reichweiten** – `ReichweitenBuchungen` (im einstelligen Millionenbereich),
  `ArtikelReichweitenTage` (rund 3 Mio), `ReichweitenAusgewertet` (rund 3 Mio), `ReichweitenArtikel`,
  `ReichweitenGruppiert`, `ReichweitenVerfuegbar`, `ReichweitenTypen`, `Reichweiten`.
* **EDI** – `EDI_QuellDaten` (rund 22 Mio), `EDI_BelegePositionen` (rund 10 Mio), `EDI_Belege`,
  `EDI_Sendungen`: der Weg, auf dem Bestellungen hereinkommen.
* **Audit** – `AenderungsProtokolle` (rund 32 Mio), `ArtikelVorgaenge` (rund 11 Mio),
  `AdressenVorgaenge`, `Lagerprotokoll` (rund 15 Mio): wer hat wann was geändert.
* **Chargenverwaltung** – `K_LotNummern` (rund 1,3 Mio) bei nur wenigen chargengeführten Lagerzeilen:
  wofür wird die Tabelle gefüllt?
* **Produktion** – `Produktionen*`, `Stuecklisten`, `ArtikelSets`.
* **FIBU** – `FibuBelege` (rund 450 k), `Zahlungen` (rund 250 k), `FibuAdressen`, offene Posten.
* **Termine/CRM** – `Termine` (rund 100 k), `SupportInfos`, `Projekte`, `K_EmailProtokoll` (rund 200 k).
