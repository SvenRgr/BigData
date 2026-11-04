**Doch, damit lassen sich interessante Zusatz-KPIs bilden.** Die Stationsdatei ergänzt vor allem **Gemeinde und Anzahl der Stellplätze** – Informationen, die in den Fahrtdaten fehlen.

| KPI | Berechnung / Aussage |
|---|---|
| **Nutzung je Stellplatz** | Starts pro Station und Monat ÷ Stellplätze. Macht kleine und große Stationen besser vergleichbar. |
| **Stationsbilanz** | Ankünfte − Abfahrten je Station und Zeitraum. Zeigt, wo durch Fahrten netto Fahrräder hinzukommen oder fehlen. |
| **Nutzung nach Gemeinde** | Starts und Ziele nach `Municipality` zusammenfassen: Welche Gemeinden werden besonders häufig genutzt? |
| **Verbindungen zwischen Gemeinden** | Fahrten innerhalb einer Gemeinde gegenüber Fahrten zwischen Gemeinden. |
| **Tagesprofil je Station** | Starts und Ankünfte nach Stunde: beispielsweise morgens überwiegend Abfahrten, abends überwiegend Ankünfte. |

**Mein Favorit wäre „Starts je Stellplatz und Monat“:** Das nutzt die zusätzliche Stationsinformation direkt und ergänzt deine bisherigen Top-10-Listen sinnvoll. Eine Station mit vielen Fahrten ist nicht automatisch die relativ am stärksten genutzte Station.

### Zwei wichtige Einschränkungen

1. **Zuordnung klären:** Die Stations-IDs der beiden Dateien verwenden unterschiedliche Formate. Du brauchst eine geprüfte Zuordnung, beispielsweise über Stationsnamen mit Koordinatenabgleich. Nicht einfach per ähnlich klingendem Namen verbinden. Den Anteil erfolgreich zugeordneter Fahrten solltest du ausweisen.
2. **Historischer Stand:** Die Stellplatzzahl stammt aus Juli 2017. Für Fahrten von 2015 wäre die Kennzahl nur eine **Näherung auf Basis der Kapazität von 2017**.

**Nicht ableitbar ist die tatsächliche Stationsauslastung:** Aus Fahrten und Stellplatzzahl allein weißt du nicht, wie viele Fahrräder jeweils verfügbar waren oder wann eine Station leer beziehungsweise voll war. Auch die Stationsbilanz berücksichtigt keine manuellen Fahrradtransporte.

Für die Prüfung würde ich **höchstens ein bis zwei Zusatz-KPIs** ergänzen, sofern die Zuordnung zuverlässig gelingt. Und ja: Spalten nur zum „Reduzieren“ zu löschen, ist bei deinem Umfang kein Selbstzweck – Parquet, Partitionierung und vorberechnete Aggregate sind bereits sinnvolle Optimierungen.