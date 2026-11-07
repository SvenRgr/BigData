# Hubway-Pruefung: Schritt-fuer-Schritt-Plan

## 1. Ziel und festgelegte Architektur

**Entschieden am 03.11.2026: Airflow fuer die Workflow-Steuerung und PySpark fuer die Datenverarbeitung. Pentaho wird nicht eingesetzt.**

Du baust eine automatisch ausfuehrbare Datenpipeline:

**Kaggle -> Download -> HDFS Raw -> PySpark-Aufbereitung -> HDFS Final -> monatliche KPIs -> Excel**

Festgelegte Basis und ergaenzende Empfehlungen:

- `marcelmittelstaedt/spark_base`: HDFS, YARN und Spark/PySpark.
- `marcelmittelstaedt/airflow`: Steuerung der gesamten Pipeline.
- Parquet als Format fuer die aufbereiteten Daten.
- Eine zum Container passende Excel-Bibliothek fuer den Export.
- Optional: Hive fuer zusaetzlichen SQL-Zugriff. Fuer diese Aufgabe nicht zwingend erforderlich.

Airflow steuert die Arbeitsschritte. PySpark verarbeitet die Fahrtdaten. Nur die kleinen, bereits aggregierten Ergebnisse werden fuer den Excel-Export lokal zusammengefuehrt.

Dieser Leitfaden legt Airflow + PySpark zugrunde. Ein Frontend, Machine Learning, Streaming und ein produktiver Mehrknotencluster sind nicht erforderlich.

### Quellen und Termine

- [Aufgabenstellung](../exercises/winter_semester_2026-2027/exam_calculate_hubway_bike_sharing_usage_kpis/Use_Kaggle_Hubway_Data_To_Calclulate_Bike_Sharing_Usage_KPIs.pdf)
- [Allgemeine Pruefungsvorgaben](../slides/winter_semester_2026-2027/5_Practical%20Exam.pdf)
- [Spark-Image](../docker/spark_base/README.md)
- [Airflow-Image](../docker/airflow/README.md)
- [Oeffentliche Docker-Images](https://hub.docker.com/u/marcelmittelstaedt)
- [Kaggle-Datensatz](https://www.kaggle.com/acmeyer/hubway-data)

**Abgabe laut Pruefungsfolien: 20.11.2026 um 23:59 Uhr.**

Praesentationen laut Folien: 23.11. oder 30.11.2026. Persoenliche Terminzuordnung und spaetere Ankuendigungen des Dozenten beachten.

## 2. Pflichtumfang der Aufgabe

- [ ] Daten von Kaggle automatisiert beziehen.
- [ ] Original-CSV-Dateien in HDFS nach `YYYYMM` partitioniert speichern.
- [ ] Daten bereinigen, reduzieren und fuer Abfragen optimieren.
- [ ] Aufbereitete Daten getrennt im Final-Bereich von HDFS speichern.
- [ ] Alle unten aufgefuehrten KPIs pro Monat berechnen.
- [ ] KPIs in eine echte Excel-Datei, beispielsweise `.xlsx`, exportieren.
- [ ] Den gesamten Datenworkflow in Airflow abbilden und automatisch ausfuehren.
- [ ] Dokumentation, DAGs, Skripte und berechnete KPIs abgeben.
- [ ] Falls Tabellen angelegt werden: pro Tabelle eine eigene DDL-Datei abgeben.

Pflicht-KPIs pro Monat:

1. Durchschnittliche Fahrtdauer in Minuten.
2. Durchschnittliche Fahrtentfernung in Kilometern.
3. Nutzungsanteile nach Geschlecht in Prozent.
4. Nutzungsanteile nach Alter in Prozent.
5. Top 10 der meistgenutzten Fahrraeder.
6. Top 10 der haeufigsten Startstationen.
7. Top 10 der haeufigsten Endstationen.
8. Nutzungsanteile der Startzeitfenster 00-06, 06-12, 12-18 und 18-24 Uhr.

## 3. Deine Entscheidungen vor der Umsetzung

Trage deine Festlegungen ein. Die Empfehlungen sind Vorschlaege, keine zusaetzlichen Vorgaben des Dozenten.

| Entscheidung | Empfehlung | Deine Festlegung |
|---|---|---|
| Verarbeitungsumfang | Mit einem Monat entwickeln, danach alle vereinbarten Monate verarbeiten | Offen |
| Zuordnung zum Monat | Monat des Fahrtbeginns | Offen |
| Bedeutung von Nutzung | Anzahl der Fahrten, nicht Personen oder Nutzungsminuten | Offen |
| Entfernung | Haversine-Luftlinie zwischen Start und Ziel; mit Dozenten abstimmen | Offen |
| Alter | Jahr des Fahrtbeginns minus Geburtsjahr | Offen |
| Altersdarstellung | Einzelalter; Gruppen optional zusaetzlich | Offen |
| Fehlende Demografie | Eigene Kategorie `Unbekannt` | Offen |
| Dauerquelle | `tripduration`, nach Pruefung der Einheit; Zeitstempeldifferenz zur Kontrolle | Offen |
| Ausreisser | Erst untersuchen, dann fachlich begruendete Grenzen festlegen | Offen |
| Duplikate | Definition dokumentieren; nicht nur nach Fahrrad oder Station deduplizieren | Offen |
| Wiederholung | Unveraenderte Eingaben erzeugen keine zusaetzlichen Fahrten | Offen |
| Hive | Erst bei erkennbarem Zusatznutzen ergaenzen | Offen |

### Frueh mit dem Dozenten klaeren

- [ ] Ist die Luftlinienentfernung als Naeherung fuer die Fahrtentfernung akzeptiert?
- [ ] Welcher Datenumfang wird erwartet, falls du nicht den gesamten Datensatz verarbeitest?
- [ ] Sind Altersgruppen anstelle von Einzelaltern akzeptiert, falls du diese bevorzugst?

**Wichtig:** Start- und Zielkoordinaten enthalten nicht die tatsaechlich gefahrene Route. Eine berechnete Luftlinie darf nicht als gemessene Fahrstrecke dargestellt werden.

## 4. Phase A: Projekt und Infrastruktur vorbereiten

### Schritt 1: Projektstruktur anlegen

- [ ] Eigenes Arbeitsverzeichnis innerhalb dieses Pruefungsordners anlegen.
- [ ] Unterordner fuer Docker-Konfiguration, DAGs, Verarbeitungsskripte, Tests, Exporte und Dokumentation vorsehen.
- [ ] Optional einen Ordner fuer Hive-DDLs ergaenzen.
- [ ] Zugangsdaten, heruntergeladene Massendaten und temporaere Dateien aus der Versionsverwaltung ausschliessen.
- [ ] Eine zentrale Konfiguration fuer Monatsumfang, HDFS-Ziele und Exportziel vorsehen.

Vorgeschlagene Struktur:

```text
hubway/
  docker/
  dags/
  scripts/
  tests/
  exports/
  docs/
  ddl/          (nur bei verwendeten Tabellen)
```

**Fertig, wenn:** Code, Konfiguration und Ergebnisse getrennt abgelegt werden koennen.

### Schritt 2: Docker-Basis pruefen

- [ ] Docker Desktop mit Linux-Containern bereitstellen.
- [ ] Images `marcelmittelstaedt/spark_base` und `marcelmittelstaedt/airflow` herunterladen.
- [ ] Verwendete Image-IDs beziehungsweise Digests dokumentieren, damit die Umgebung nachvollziehbar bleibt.
- [ ] Tatsaechliche Versionen von Python, Spark und Airflow in den Images feststellen.
- [ ] Verfuegbaren Arbeitsspeicher und Speicherplatz pruefen; Docker- und YARN-Ressourcen aufeinander abstimmen.
- [ ] Gemeinsames Docker-Netzwerk `bigdatanet` einrichten.
- [ ] Container mit den Namen `hadoop` und `airflow` konfigurieren.
- [ ] Weboberflaechen nur lokal veroeffentlichen, nicht ungeschuetzt ins Netzwerk stellen.

Die mitgelieferten Konfigurationen verwenden diese Container-Namen zur gegenseitigen Aufloesung. Abweichende Namen erfordern entsprechende Konfigurationsaenderungen.

**Achtung:** Die Lehrimages enthalten alte Software. Aktuelle Airflow-Anleitungen oder aktuelle Python-Pakete nicht ungeprueft uebernehmen. Die Images wurden fuer diesen Plan nicht gestartet; die Laufzeitkompatibilitaet ist in diesem Schritt zu pruefen.

### Schritt 3: Persistenz und Startverhalten einrichten

- [ ] Projektcode und Exporte ueber geeignete Bind-Mounts verfuegbar machen.
- [ ] HDFS-NameNode- und DataNode-Daten persistent speichern.
- [ ] Airflow-DAGs, Logs und die PostgreSQL-Metadaten persistent halten.
- [ ] Falls Hive genutzt wird, auch die Metadatenhaltung beruecksichtigen.
- [ ] Startablauf des Hadoop-Containers pruefen und HDFS/YARN starten.
- [ ] Sicherstellen, dass vorhandenes HDFS beim Neuerstellen des Containers nicht erneut formatiert wird.
- [ ] Einen reproduzierbaren Startablauf, vorzugsweise ueber Docker Compose, festhalten.

Besonders wichtig: Das mitgelieferte Spark-Startskript initialisiert beim ersten Start HDFS. Der dauerhafte automatische Hadoop-Start ist dort auskommentiert. Containerstatus allein ist deshalb kein Bereitschaftstest.

### Schritt 4: Kleinen Infrastrukturtest durchfuehren

- [ ] HDFS-Weboberflaeche und YARN-Weboberflaeche erreichen.
- [ ] Eine kleine Testdatei in HDFS schreiben und wieder lesen.
- [ ] Einen kleinen PySpark-Job mit `--master yarn` ausfuehren.
- [ ] Aus dem Airflow-Container HDFS und YARN erreichen.
- [ ] Festlegen, wo der Spark-Treiber laeuft und wie Skripte sowie Abhaengigkeiten dort verfuegbar werden.
- [ ] Einen Testjob aus Airflow starten; Rueckweg zwischen Spark-Treiber und Executors pruefen.
- [ ] Container neu starten und Persistenz pruefen.

Empfehlung: Airflow startet `spark-submit` mit dem vorhandenen Spark-Client gegen YARN. Pfade und Umgebung explizit setzen; nicht darauf verlassen, dass ein nicht interaktiver Task die gleiche Shell-Konfiguration wie ein manuelles Terminal laedt.

**Fertig, wenn:** Ein Airflow-Testtask erfolgreich eine kleine Berechnung auf YARN ausfuehrt und ein persistentes Ergebnis erzeugt.

## 5. Phase B: Daten verstehen und importieren

### Schritt 5: Beispieldaten untersuchen

- [ ] Eine Monatsdatei als Entwicklungsbeispiel auswaehlen.
- [ ] Dateinamen, Monatsumfang und Datenquelle dokumentieren.
- [ ] CSV-Trennzeichen, Anfuehrungszeichen, Header und Zeichenkodierung pruefen.
- [ ] Datumsformate, Dauer-Einheit und Geschlechtskodierungen anhand der Quelle pruefen.
- [ ] Fehlende Werte und auffaellige Werte zaehlen.
- [ ] Pruefen, ob sich Spaltennamen oder Formate zwischen Monaten unterscheiden.
- [ ] Zeitzoneninterpretation festlegen; lokale Stationszeiten nicht versehentlich als UTC umdeuten.

**Fertig, wenn:** Du die Bedeutung jeder fuer die KPIs benoetigten Spalte erklaeren kannst.

### Schritt 6: Explizites Schema definieren

- [ ] Start- und Endzeit als Zeitstempel vorsehen.
- [ ] Dauer und Koordinaten numerisch modellieren.
- [ ] Fahrrad- und Stations-IDs konsistent modellieren.
- [ ] Geburtsjahr und Geschlechtsangabe mit dokumentierter Behandlung fehlender Werte modellieren.
- [ ] Hilfsspalten fuer Fahrtmonat, Alter, Zeitfenster und Entfernung definieren.
- [ ] Fuer jede abgeleitete Spalte Bedeutung und Einheit dokumentieren.

**Fertig, wenn:** Das Einlesen nicht dauerhaft von automatisch geratenen Datentypen abhaengt.

### Schritt 7: Download automatisieren

- [x] Zum alten Laufzeitumfeld passenden Downloadweg fuer Kaggle festlegen.
- [ ] Erforderliche Zugangsdaten extern bereitstellen, nicht im Code speichern.
- [ ] Downloadskript mit klarer Fehlerausgabe und Exitcode implementieren.
- [ ] Download auf Vollstaendigkeit und erwartete Dateien pruefen.
- [ ] Archive kontrolliert entpacken und Dateiliste protokollieren.
- [ ] Bei erneuter Ausfuehrung bereits vorhandene unveraenderte Downloads erkennen.

Abhaengigkeiten erst nach Pruefung der vorhandenen Pakete gezielt ergaenzen. Benoetigte Versionen reproduzierbar in einer Projektkonfiguration oder einem abgeleiteten Image festhalten; nicht nur manuell in einem laufenden Container installieren.

Der [Hubway-DAG](hubway/dags/hubway.py) verwendet wie das IMDb-Beispiel den
vorhandenen `HttpDownloadOperator`: Die oeffentliche URL
`https://www.kaggle.com/api/v1/datasets/download/acmeyer/hubway-data` wird als
`/home/airflow/hubway/hubway.zip` gespeichert. Anschliessend entpackt die
Python-Task `unzip_hubway_data` das ZIP mit `zipfile.ZipFile` nach
`/home/airflow/hubway/raw`. Der Kurs-`UnzipFileOperator` verarbeitet nur GZIP,
nicht ZIP. Weder `kagglehub` noch ein Python-Update ist erforderlich.
Download und Entpacken wurden am 04.11.2026 ohne Zugangsdaten geprueft.
Bestehende fehlgeschlagene Runs werden durch eine Codeaenderung nicht
automatisch wiederholt: In Airflow die Task `download_hubway_data` und ihre
nachgelagerten Tasks gezielt per **Clear** zuruecksetzen, ohne die bereits
erfolgreichen Verzeichnis-Tasks erneut auszufuehren.

### Schritt 8: Raw-Import nach HDFS implementieren

- [ ] Original-CSV-Dateien unveraendert aufbewahren.
- [ ] Dateien dem jeweiligen Quellmonat zuordnen und gegen die enthaltenen Startdaten pruefen.
- [ ] Nach `/user/hadoop/hubway/raw/YYYYMM/` schreiben.
- [ ] Quelldatei, Monat, Pruefsumme, Importstatus und Zeilenanzahl nachvollziehbar festhalten.
- [ ] Erneuten Import derselben Datei ohne Doppelzaehlung behandeln.
- [ ] Teilweise fehlgeschlagene Importe von vollstaendigen Importen unterscheiden.

Die Pfade mit `/` sind HDFS-/Linux-Pfade innerhalb der Container, keine Windows-Hostpfade.

**Fertig, wenn:** Download und Raw-Import ohne manuelles Kopieren funktionieren und Raw-Dateien nicht durch die Bereinigung veraendert werden.

## 6. Phase C: Bereinigen und Final-Bereich aufbauen

### Schritt 9: Bereinigungsregeln implementieren

- [ ] Nicht lesbare CSV-Zeilen und Konvertierungsfehler erkennen und zaehlen.
- [ ] Fehlende oder ungueltige Startzeitpunkte getrennt ausweisen.
- [ ] Dauer auf fehlende, negative, null und auffaellig grosse Werte pruefen.
- [ ] Dauer mit der Differenz aus Start- und Endzeit vergleichen; Abweichungen quantifizieren.
- [ ] Koordinaten auf fehlende Werte und gueltige Wertebereiche pruefen.
- [ ] Geburtsjahre beziehungsweise Alter auf Plausibilitaet pruefen.
- [ ] Geschlechtswerte anhand der verifizierten Kodierung vereinheitlichen.
- [ ] Duplikate nach der dokumentierten Definition erkennen.
- [ ] Verworfene Datensaetze beziehungsweise Gruende nachvollziehbar speichern.

Nicht jede ungueltige Spalte macht die gesamte Fahrt unbrauchbar:

| Problem | Empfohlener Umgang |
|---|---|
| Geburtsjahr fehlt | Fahrt behalten, Alter `Unbekannt` |
| Geschlecht fehlt | Fahrt behalten, Geschlecht `Unbekannt` |
| Koordinaten ungueltig | Fahrt behalten, Entfernung nicht berechnen |
| Dauer ungueltig | Fahrt fuer andere geeignete KPIs behalten, aus Dauermittel ausschliessen |
| Startzeit ungueltig | Keine sichere Monats-/Zeitfensterzuordnung; separat ausweisen |

**Deine Entscheidung:** Konkrete Ausreissergrenzen erst nach Untersuchung setzen. Jede Grenze und ihre Auswirkung in der Dokumentation nennen.

### Schritt 10: Final-Daten schreiben

- [ ] Nur benoetigte Originalspalten und dokumentierte Hilfsspalten uebernehmen.
- [ ] Parquet mit einer unterstuetzten Kompression verwenden.
- [ ] Nach Monat unter `/user/hadoop/hubway/final/` partitionieren.
- [ ] Konkretes Verzeichnislayout festlegen, beispielsweise `final/201606/`.
- [ ] Bei Spark-Verzeichnissen wie `month=201606/` deren Verwendung konsistent dokumentieren.
- [ ] Bei erneutem Lauf nur die betroffenen Monate kontrolliert ersetzen, nicht versehentlich den gesamten Bestand.
- [ ] Ergebnisse erst nach erfolgreicher Verarbeitung fuer Folgeschritte freigeben.
- [ ] Anzahl und Groesse der Dateien kontrollieren; viele winzige Dateien vermeiden.
- [ ] Final-Daten erneut lesen und Schema sowie Zeilenanzahlen pruefen.

**Fertig, wenn:** Ein Monat korrekt aufbereitet und ohne Doppelzaehlung erneut erzeugt werden kann.

## 7. Phase D: Monatliche KPIs berechnen

### Schritt 11: Gemeinsame Definitionen umsetzen

- [ ] Fuer alle Auswertungen dieselbe dokumentierte Monatszuordnung verwenden.
- [ ] Anzahl aller auswertbaren Fahrten pro Monat als Referenz speichern.
- [ ] Fuer jeden KPI die Anzahl seiner gueltigen Eingabewerte ausgeben.
- [ ] Unbekannte Kategorien in Anteilsberechnungen sichtbar beruecksichtigen.
- [ ] Bei Monaten ohne gueltige Werte keinen erfundenen Mittelwert `0` ausgeben; fehlendes Ergebnis kennzeichnen.
- [ ] Rundung nur fuer die Ausgabe vornehmen, nicht fuer Zwischenergebnisse.

### Schritt 12: Dauer und Entfernung

- [ ] Mittlere Fahrtdauer aus gueltigen Dauerwerten berechnen: `avg(tripduration) / 60`, sofern die Quelle Sekunden bestaetigt.
- [ ] Entfernung mit Haversine aus Start- und Zielkoordinaten berechnen, wenn diese Naeherung vereinbart wurde.
- [ ] Durchschnitt der gueltigen Entfernungen pro Monat berechnen.
- [ ] Entfernung im Bericht ausdruecklich als Luftlinien-Naeherung kennzeichnen.
- [ ] Fahrten mit gleicher Start- und Zielstation nicht pauschal als ungueltig loeschen.

Eine Luftlinienentfernung von null kann trotz einer realen Rundfahrt korrekt aus den vorhandenen Koordinaten folgen.

### Schritt 13: Geschlecht, Alter und Zeitfenster

- [ ] Fahrten je Geschlechtskategorie und Monat zaehlen.
- [ ] Alter naeherungsweise als `Fahrtjahr - Geburtsjahr` berechnen.
- [ ] Fahrten je Alter und Monat zaehlen; unbekanntes Alter separat ausgeben.
- [ ] Je Kategorie den Anteil `Kategorie-Fahrten / Gesamtfahrten * 100` berechnen.
- [ ] Startzeiten in die vier geforderten Zeitfenster einordnen.
- [ ] Fehlende Zeitfenster mit null Fahrten und null Prozent darstellen, sofern der Monat Fahrten enthaelt.

Zeitfenster sind links eingeschlossen und rechts ausgeschlossen:

| Zeitfenster | Bedingung fuer Startstunde |
|---|---|
| 00-06 Uhr | `0 <= Stunde < 6` |
| 06-12 Uhr | `6 <= Stunde < 12` |
| 12-18 Uhr | `12 <= Stunde < 18` |
| 18-24 Uhr | `18 <= Stunde < 24` |

### Schritt 14: Top-10-Listen

- [ ] Fahrten je Monat und Fahrrad-ID zaehlen.
- [ ] Fahrten je Monat und Startstations-ID zaehlen.
- [ ] Fahrten je Monat und Endstations-ID zaehlen.
- [ ] Stationsnamen als Beschriftung zuordnen; wechselnde Namen nicht unbeabsichtigt als verschiedene Stationen zaehlen.
- [ ] Je Monat nach Fahrtenzahl absteigend und bei Gleichstand nach ID stabil sortieren.
- [ ] Je Liste hoechstens zehn Eintraege pro Monat ausgeben.

**Fertig, wenn:** Alle acht Pflichtauswertungen fuer den Entwicklungsmonat nachvollziehbar vorliegen.

## 8. Phase E: Excel und Airflow fertigstellen

### Schritt 15: Excel-Export implementieren

Der aktuelle Spark-Excel-Export verwendet `com.crealytics:spark-excel_2.11:0.11.1`
fuer Spark 2.3.4 mit Scala 2.11 aus dem Airflow-Dockerfile. Der Task
`calculate_kpis` laedt das Paket ueber `SparkSubmitOperator(packages=...)`.
Beim ersten Lauf benoetigt der Airflow-Container Zugriff auf Maven Central;
danach koennen die Abhaengigkeiten aus dem lokalen Ivy-Cache geladen werden.
Diese Connector-Version verwendet das Format `com.crealytics.spark.excel`
und die Pflichtoption `useHeader`, nicht `header`. Die Ausgabe liegt unter
`/user/hadoop/hubway/kpis/kpis.xlsx` in HDFS, noch nicht im Windows-Exportverzeichnis.

- [ ] Vorhandene Bibliotheken im Export-Laufzeitumfeld pruefen.
- [ ] Falls erforderlich, kompatible Excel-Abhaengigkeit reproduzierbar ergaenzen.
- [ ] Nur aggregierte Ergebnisse zum Exportprozess uebertragen.
- [ ] Eine Arbeitsmappe mit Monatsspalte in allen Ergebnistabellen erzeugen.
- [ ] Einheiten und Prozentdarstellung eindeutig beschriften.
- [ ] Exportdatei erst nach erfolgreichem Schreiben als fertiges Ergebnis bereitstellen.
- [ ] Ausgabe in ein vom Windows-Host erreichbares, persistentes Exportverzeichnis schreiben.
- [ ] Excel-Datei tatsaechlich oeffnen und Inhalte kontrollieren.

Empfohlene Tabellenblaetter:

1. `Uebersicht`: Monat, Fahrten, mittlere Dauer und mittlere Entfernung.
2. `Geschlecht`.
3. `Alter`.
4. `Top10_Fahrraeder`.
5. `Top10_Startstationen`.
6. `Top10_Endstationen`.
7. `Zeitfenster`.
8. `Datenqualitaet`: Eingabemengen, Duplikate und ungueltige Werte je Monat.
9. `Definitionen`: Datenumfang, Regeln, Einheiten und Einschraenkungen.

### Schritt 16: Vollstaendigen Airflow-DAG erstellen

- [ ] DAG passend zur installierten Airflow-1.10.x-Version implementieren.
- [ ] Download, Entpacken, Raw-Import, Aufbereitung, Pruefung, KPI-Berechnung und Export als nachvollziehbare Tasks verbinden.
- [ ] Verarbeitungsskripte aus den Tasks aufrufen; umfangreiche Verarbeitung nicht beim Einlesen der DAG-Datei ausfuehren.
- [ ] Quell- und Zielverzeichnis als Parameter vorsehen. Der Bereinigungsjob verarbeitet alle Monatsdateien unter `tripdata/raw/YYYYMM/YYYYMM-hubway-tripdata.csv` ohne Monatsparameter und schreibt jeden Monat separat nach `tripdata/final/YYYYMM/`.
- [ ] Rueckgabecodes und Fehler korrekt an Airflow weitergeben.
- [ ] Wiederholungsversuche fuer voruebergehende Fehler konfigurieren.
- [ ] Ueberlappende Laeufe auf denselben Zielmonaten verhindern.
- [ ] Zeitplanung und Nachholverhalten bewusst konfigurieren, keine unbeabsichtigten historischen Laeufe starten.
- [ ] Sicherstellen, dass nachgelagerte Schritte bei fehlgeschlagenen Voraussetzungen nicht erfolgreich weiterlaufen.
- [ ] DAG aus der Weboberflaeche starten und ohne manuelle Zwischenschritte durchlaufen lassen.

Zielablauf:

```text
download
  -> validate_and_extract
  -> import_raw
  -> transform_final
  -> validate_final
  -> calculate_kpis
  -> export_excel
  -> validate_export
```

Ein manueller Start zur Praesentation ist moeglich. Danach muss der gesamte Datenworkflow automatisch laufen.

**Fertig, wenn:** Ein DAG-Start vom Datenbezug bis zur geprueften Excel-Datei fuehrt.

## 9. Phase F: Richtigkeit und Wiederholbarkeit belegen

### Schritt 17: Kleinen kontrollierten Testdatensatz erstellen

- [ ] Mehrere Monate und mindestens einen Jahreswechsel abdecken.
- [ ] Unterschiedliche Fahrraeder und Stationen einbauen.
- [ ] Fehlendes Alter, fehlendes Geschlecht und ungueltige Koordinaten abdecken.
- [ ] Ein bekanntes Duplikat und eine ungueltige Dauer vorsehen.
- [ ] Startzeiten direkt vor und auf den Grenzen 06:00, 12:00 und 18:00 Uhr testen.
- [ ] Erwartete Resultate unabhaengig von Hand berechnen.

### Schritt 18: Konkrete Ergebnispruefungen ausfuehren

- [ ] 120 Sekunden ergeben 2 Minuten.
- [ ] Gleiche Koordinaten ergeben 0 Kilometer Luftlinie.
- [ ] Eine bekannte Koordinatenpaarung liefert eine plausible, unabhaengig gepruefte Entfernung.
- [ ] 06:00 Uhr gehoert zum Zeitfenster 06-12 Uhr.
- [ ] Alter wird mit dem Fahrtjahr statt dem heutigen Jahr berechnet.
- [ ] Geschlechts-, Alters- und Zeitfensteranteile ergeben je Monat vor Rundung etwa 100 Prozent.
- [ ] Summen der Kategorie-Fahrten stimmen mit ihrem dokumentierten Nenner ueberein.
- [ ] Top-10-Listen haben hoechstens zehn Zeilen je Monat und stabile Reihenfolge.
- [ ] Rohmengen, Duplikate, Ausschluesse und Final-Mengen lassen sich abstimmen.
- [ ] Ein Monat ohne gueltige Dauer- oder Entfernungswerte wird korrekt gekennzeichnet.
- [ ] Ein zweiter Lauf erzeugt dieselben KPIs und keine doppelten Daten.
- [ ] Ein absichtlich fehlgeschlagener Download fuehrt zu einem sichtbaren Fehler und keinem falschen Erfolgsexport.
- [ ] Ein Wiederanlauf nach Fehler funktioniert.
- [ ] Container-Neustart erhaelt die benoetigten Daten.

### Schritt 19: Auf den vereinbarten Gesamtumfang erweitern

- [ ] Alle vorgesehenen Monate verarbeiten.
- [ ] Schemaabweichungen zwischen Dateien behandeln.
- [ ] Laufzeit, Datenmenge und Ergebnisgroesse dokumentieren.
- [ ] Pruefen, dass fuer jeden enthaltenen Monat alle Pflichtauswertungen existieren.
- [ ] Excel-Bericht mit realen Daten auf Plausibilitaet pruefen.
- [ ] Auffaellige Ergebnisse kurz fachlich interpretieren, nicht nur Tabellen liefern.

**Fertig, wenn:** Korrekte Ergebnisse fuer den gesamten vereinbarten Umfang vorliegen und die Wiederholbarkeit nachgewiesen ist.

## 10. Vorlesungskonzepte in der Dokumentation sichtbar machen

| Material | Umsetzung und Erklaerung |
|---|---|
| Skript 1: Datenmodelle und Zugriff | Explizites Schema, Datentypen, Aggregationen, spaltenorientierte Speicherung; optional Hive |
| Skript 2: Partitionierung | Monatsaufteilung und gezieltes Lesen eines Monats; Dateipartitionen von Spark-Ausfuehrungspartitionen unterscheiden |
| Skript 3: Replikation und Spark | PySpark auf YARN; Replikation, Ausfallsicherheit und Grenzen der lokalen Umgebung erlaeutern |
| Skript 4: ETL, Batch und Stream | Airflow-DAG, Abhaengigkeiten, Fehlerbehandlung; Batch-Verarbeitung wegen begrenzter Monatsdateien begruenden |
| Skript 5: Praktische Pruefung | Vollstaendige Artefakte, Dokumentation und startbarer Demonstrationsworkflow |

Der mitgelieferte Hadoop-Aufbau ist pseudo-verteilt auf einem Knoten; der konfigurierte HDFS-Replikationsfaktor ist 1. Das bietet keine echte Hochverfuegbarkeit. Ein Docker-Volume ist ebenfalls kein Backup. Diese Grenzen ehrlich benennen.

Nicht jedes Vorlesungskonzept muss kuenstlich eingebaut werden. Fuer diese Aufgabe sind weder Streaming noch zusaetzliche NoSQL-Datenbanken notwendig.

## 11. Dokumentation und Abgabe

### Schritt 20: Kurze, vollstaendige Dokumentation schreiben

- [ ] Ziel, Datenquelle und verarbeitete Monate nennen.
- [ ] Architektur mit Containern, HDFS, YARN, Spark und Airflow darstellen.
- [ ] Startanleitung einschliesslich benoetigter Zugangsdaten-Konfiguration bereitstellen, ohne Geheimnisse einzufuegen.
- [ ] Jeden Airflow-Task und seine Aufgabe kurz erklaeren.
- [ ] Raw-/Final-Datenmodell, Verzeichnislayout und Partitionierung beschreiben.
- [ ] Alle Bereinigungsregeln, KPI-Formeln und Nenner dokumentieren.
- [ ] Entfernung als Naeherung und Alter als jahrbasierte Schaetzung kennzeichnen.
- [ ] Fehlerbehandlung, Wiederholbarkeit und Tests beschreiben.
- [ ] Ergebnisse interpretieren und Grenzen nennen.
- [ ] Verwendete Versionen und Abhaengigkeiten festhalten.

### Schritt 21: Abgabepaket pruefen

- [ ] Dokumentation enthalten.
- [ ] Alle Airflow-DAGs enthalten.
- [ ] Alle Download-, Aufbereitungs-, KPI- und Exportskripte enthalten.
- [ ] Docker-/Startkonfiguration und Abhaengigkeitsdefinitionen enthalten.
- [ ] Tests und kleine erlaubte Testdaten enthalten.
- [ ] Falls Tabellen verwendet werden: je Tabelle eine DDL-Datei mit passendem Dateinamen enthalten.
- [ ] Berechnete Excel-KPIs enthalten.
- [ ] Keine Zugangsdaten, privaten Token oder unnoetigen temporaeren Dateien enthalten.
- [ ] Datenquelle und Wiederbeschaffung erklaert; Weitergabe grosser Quelldaten nur gemaess Lizenz und Abgabevorgaben.
- [ ] Startanleitung aus einem sauberen Zustand nachvollziehen.
- [ ] Abgabedownload beziehungsweise Freigabelink pruefen.
- [ ] Abgabe gemaess Folien an `contact@marcel-mittelstaedt.com` uebermitteln; aktuelle Ankuendigungen beachten.

## 12. Praesentation vorbereiten

### Schritt 22: Demonstration proben

- [ ] Container und Dienste vor Beginn auf Bereitschaft pruefen.
- [ ] Workflow starten.
- [ ] Waehrend der Ausfuehrung Quelle, Datenstruktur und Beschaffung erklaeren.
- [ ] DAG und Zweck der Tasks zeigen.
- [ ] Raw-/Final-Trennung, Monatsaufteilung und Parquet begruenden.
- [ ] Bereinigungsregeln und fachliche Entscheidungen erklaeren.
- [ ] Nach Ende Excel oeffnen und die geforderten KPIs zeigen.
- [ ] Mindestens zwei interessante Beobachtungen aus den Ergebnissen erklaeren.
- [ ] Einschraenkungen von Distanz, Alter und Einzelknotenaufbau nennen.
- [ ] Laufzeit vorher messen und Demonstrationsumfang passend planen.
- [ ] Bereits berechnete Ergebnisse als klar gekennzeichnete Reserve bereithalten; keinen erfolgreichen Live-Lauf vortaeuschen.

## 13. Zeitplan bis zur Abgabe

| Zeitraum | Ziel |
|---|---|
| 03.-05.11.2026 | Regeln festlegen, Infrastruktur testen, Quelldaten verstehen |
| 06.-09.11.2026 | Download, Raw-Import und Aufbereitung eines Monats |
| 10.-12.11.2026 | Final-Speicherung und alle Pflicht-KPIs |
| 13.-15.11.2026 | Excel, vollstaendiger DAG und vereinbarter Gesamtumfang |
| 16.-18.11.2026 | Tests, Wiederanlauf und Dokumentation |
| 19.11.2026 | Praesentationsprobe und Abgabepaket |
| 20.11.2026 | Puffer, letzte Pruefung und Abgabe vor 23:59 Uhr |

## 14. Abschlusskontrolle

Das Projekt ist fertig, wenn du diese Frage mit Ja beantworten kannst:

> Kann ich mit dokumentierter Konfiguration einen Airflow-Workflow starten, der die vereinbarten Kaggle-Daten automatisch bezieht, nach Monaten in HDFS ablegt, nachvollziehbar bereinigt, alle Pflicht-KPIs berechnet und einen geprueften Excel-Bericht erzeugt, ohne bei Wiederholung doppelte Daten zu produzieren?

- [ ] Ja: Funktionsumfang und Tests nachgewiesen.
- [ ] Ja: Ergebnisse und Dokumentation dauerhaft gespeichert.
- [ ] Ja: Abgabepaket vollstaendig und ohne Geheimnisse.
