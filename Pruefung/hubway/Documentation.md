# Hubway ETL Workflow



## Setup

1. Hadoop / Spark / Airflow Container aus der Vorlesung 
2. dags und skripte in container kopieren
3. OSRM starten
5. DAG `Hubway` in Airflow anschalten / triggern.

## Workflow (DAG)


| Task | Was passiert |
| --- | --- |
| `create_import_dir`, `clear_import_dir` | lokales Verzeichnis `/home/airflow/hubway` anlegen und leeren |
| `download_hubway_data` | Kaggle Datensatz als zip laden |
| `unzip_hubway_data` | zip entpacken |
| `create_hubway_stations_hdfs_raw_dir`, `create_hubway_stations_hdfs_clean_dir` | HDFS Ordner für Stationen anlegen |
| `put_hubway_stations_2017` | Stationsdatei 2017 nach HDFS raw |
| `create_hubway_tripdata_hdfs_raw_directories` | pro Monat einen Ordner `raw/YYYYMM` anlegen |
| `create_hubway_tripdata_hdfs_clean_directories` | clean Ordner anlegen |
| `import_hubway_tripdata_to_hdfs` | jede `YYYYMM-hubway-tripdata.csv` in ihren Monatsordner legen |
| `clean_hubway_tripdata` | Spark: Fahrtdaten bereinigen, als Parquet nach `clean` |
| `clean_station_tripdata` | Spark: Stationsdaten bereinigen, als Parquet nach `stations/clean` |
| `calculate_trip_length` | Spark: Routenlänge über OSRM ergänzen, nach `final` |
| `calculate_kpis` | Spark: KPIs berechnen und als Excel exportieren |

Ablauf:

```
create_import_dir -> clear_import_dir -> download -> unzip
unzip -> stations raw/clean dirs -> put stations -> clean stations


unzip -> tripdata raw dirs -> import to hdfs -> clean tripdata -> trip length -> calculate_kpis
unzip -> tripdata clean dirs
```

## HDFS Ordner

| Layer | Pfad | Format |
| --- | --- | --- |
| Raw | `/user/hadoop/hubway/tripdata/raw/YYYYMM/` | CSV |
| Clean | `/user/hadoop/hubway/tripdata/clean/month=YYYYMM/` | Parquet |
| Final | `/user/hadoop/hubway/tripdata/final/month=YYYYMM/` | Parquet + OSRM Distanz |
| Stationen | `/user/hadoop/hubway/stations/raw`, `/clean` | CSV / Parquet |
| KPIs | `/user/hadoop/hubway/kpis/kpis.xlsx` | Excel |

Es werden nur die monatlichen Dateien (`YYYYMM-hubway-tripdata.csv`) verarbeitet, also 2015-01 bis 2017-12.

Es werden keine Hive Tabellen angelegt, die Scripts lesen und schreiben direkt Parquet. Deshalb gibt es keine DDLs.

## Datenbereinigung (`clean_tripdata.py`)

- Platzhalter beim Geburtsjahr (`\N`, leer, `null`, ...) werden zu `null`
- Zeiten werden zu Timestamps, IDs / Dauer / Gender zu Zahlen, Koordinaten zu double
- Leerzeichen in Spaltennamen werden zu `_`
- Monat kommt aus dem Dateinamen, danach wird nach `month` partitioniert

Bewusst werden keine kompletten Zeilen gelöscht. Ungültige Werte werden nur in der jeweiligen Berechnung ignoriert, z.B. ohne Geburtsjahr landet die Fahrt beim Alter unter "unbekannt", zählt aber trotzdem bei Dauer, Stationen usw. mit.

## KPIs (`calc_kpis.py`)

Werden einmal für den ganzen Zeitraum ("Gesamt") und für jeden Monat berechnet.

| KPI | Regel |
| --- | --- |
| Durchschnittliche Fahrtdauer | `tripduration` / 60, in Minuten  |
| Durchschnittliche Entfernung | Luftlinie (Haversine) zwischen Start und Endstation in km, Fahrten ohne Koordinaten werden ignoriert |
| Routenentfernung (Zusatz) | Fahrradroute laut OSRM in km, Fahrten ohne Route (z.B. Roundtrips (gleiche Anfang und Endstation)) werden ignoriert |
| Geschlecht | 0 = unbekannt, 1 = männlich, 2 = weiblich (siehe unten) |
| Alter | Startjahr - Geburtsjahr: unter 20, 20-39, 40-60, über 60, unbekannt |
| Top 10 Fahrräder | nach Anzahl Fahrten pro `bikeid` |
| Top 10 Start / Endstationen | nach Anzahl Fahrten pro Station-ID, Name aus den Fahrtdaten |
| Zeitslots | Startstunde in 0-6, 6-12, 12-18, 18-24 |

Rundfahrten werden bei beiden Entfernungsdurchschnitten ausgeschlossen, bei allen anderen KPIs berücksichtigt.

Gender-Codes sind in den Daten nirgends beschrieben, Bedeutung von hier übernommen: https://www.erikellis.com/2018/12/hubway-capstone-project-user-type.html

### Excel

Eine Zeile pro Zeitraum (erste Zeile Gesamt, dann die Monate), eine Spalte pro Wert. Bei den Top 10 gibt es immer zwei Spalten (ID/Name und Anzahl Fahrten). Export mit dem Spark Excel Paket `com.crealytics:spark-excel_2.11:0.11.1`.

## Zusatz: Routenlänge mit OSRM

Die Luftlinie ist kürzer als die echte Strecke. Deshalb wird zusätzlich über einen eigenen OSRM-Server (Fahrradprofil, Karte Massachusetts von Geofabrik) die Route zwischen Start und Endstation berechnet. Pro Stationspaar gibt es nur eine Anfrage, danach wird das Ergebnis an die Fahrten gejoint. Wenn keine Route gefunden wird, bleibt die Distanz leer.

Ist auch nur eine Schätzung, nicht die wirklich gefahrene Strecke. Rundfahrten (gleiche Start- und Endstation) haben Luftlinie 0.

## Bekannte Punkte

- Stationsdaten werden geladen und bereinigt, aber für die KPIs nicht gebraucht. Habe ich drin gelassen, vlt. für weitere KPIs.
- `usertype` ist auch nicht für die KPIs nötig, drin gelassen für mögliche weitere Statistiken.
- Koordinaten werden nur auf `null` geprüft. 

## Analyse Notebook

`analyze.ipynb` habe ich vorher genutzt, um mir die Daten anzuschauen (Januar 2015 + Stationsdateien).

## Hinweis

Zur formatierten Ausgabe in Excel, zum Debugging sowie zum Erstellen dieser Dokumentation auf Basis des Workflows wurde AI genutzt.
