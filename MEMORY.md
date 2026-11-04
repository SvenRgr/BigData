# Setup-Notizen (Big Data, DHBW)

Abweichungen vom Setup in den Folien, die für die nächsten Stunden gelten.

## VM (ersetzt am 04.11.2026)
- Projekt `dhbw-511410` (`191824237382`), Instanz `big-data`, Zone `europe-west3-a`.
- Typ `n2-highmem-4`: 4 vCPU, 32 GB RAM; 100 GB `pd-standard`.
- Grundlage: Folien Wintersemester 2026/2027. Mit Zustimmung Ubuntu 24.04 LTS statt des alten Ubuntu 20.04 aus den Folien; die Kurssoftware verwendet weiterhin die Kurs-Docker-Images.
- Statische externe IP `34.40.105.245`, reservierte Adresse `big-data-ip` in `europe-west3`.
- Die alte VM in `us-central1-a` konnte wegen `ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS` am 03./04.11. nicht starten. Alte VM und Disk `instance-20261012-124119` wurden nach ausdruecklicher Freigabe geloescht. Alte Container, IMDb-/HDFS-Daten und Notebooks wurden nicht migriert.
- Zugriff wie bisher: `ssh big-data`. SSH-Key unveraendert; eigener `HostKeyAlias big-data-frankfurt-2026` fuer die neue VM. Andere SSH-Hosts unveraendert.
- VM-Neustart und erneuter SSH-Zugriff wurden getestet.
- Am Ende der Stunde stoppen:
  `gcloud compute instances stop big-data --project=dhbw-511410 --zone=europe-west3-a`
- Wieder starten:
  `gcloud compute instances start big-data --project=dhbw-511410 --zone=europe-west3-a`
- Disk und reservierte externe IPv4-Adresse verursachen auch bei gestoppter VM Kosten.

## SSH-Portweiterleitungen
- SSH-Sitzung offen lassen; alternativ nur Tunnel: `ssh -N big-data`.
- Alle Weiterleitungen binden ausschliesslich an `127.0.0.1`. Docker-Ports sind auf dem VM-Host ebenfalls nur an `127.0.0.1` gebunden; keine oeffentliche Freigabe der Kursdienste.
- Konfigurierte lokale Ports: `80`, `4040`, `8030`, `8031`, `8032`, `8042`, `8080`, `8088`, `8888`, `9000`, `9090`, `9091`, `9092`, `9864`, `9870`, `10000`.
- Alle 16 lokalen SSH-Listener und HTTP-Zugriff auf Airflow wurden getestet.
- Wichtige Ziele nach Start des jeweiligen Dienstes:
  - Airflow: http://127.0.0.1:8080
  - YARN: http://127.0.0.1:8088
  - HDFS NameNode: http://127.0.0.1:9870
  - HDFS DataNode: http://127.0.0.1:9864
  - Jupyter: http://127.0.0.1:8888 (Token ueber `jupyter notebook list` im Container)
  - Spark: http://127.0.0.1:4040 (nur waehrend einer Spark-Anwendung)
  - Hive/DBeaver: `jdbc:hive2://localhost:10000/default`, Benutzer `hadoop`, kein Passwort.
- `80` und `9090`-`9092` sind vorsorglich laut Kurs-Firewallliste weitergeleitet; aktuell kein Dienst dahinter. Eine Weiterleitung startet keinen Dienst.

## Container
- Installierte Kurs-Images: `marcelmittelstaedt/spark_base:latest`, `marcelmittelstaedt/airflow:latest`, `marcelmittelstaedt/pentaho:latest`.
- Container `hadoop` und `airflow`, Netzwerk `bigdatanet`, jeweils `--ulimit nofile=65536:65536`.
- Container `pentaho` am 04.11.2026 auf Wunsch entfernt: Der Hubway-Plan verwendet Airflow und PySpark, kein Pentaho. Das heruntergeladene Pentaho-Image bleibt erhalten.
- `hadoop` publiziert `8088`, `9870`, `9864`, `10000`, `8032`, `8030`, `8031`, `9000`, `8888`, `4040`, `8042`; `airflow` publiziert `8080`.
- Containerstart nach VM-Neustart wurde getestet. `airflow` verwendet `--restart unless-stopped`; `hadoop` wird von `course-hadoop.service` gestartet.
- Auf Wunsch keine weitere Einrichtung innerhalb der Container. Der Hostdienst fuehrt nur `docker start hadoop` aus, keine Hadoop-/Hive-/Jupyter-Startbefehle.
- Bereits vor dieser Klarstellung: HDFS initialisiert, Kursdatei `/home/hadoop/Faust_1.txt` nach `/user/hadoop/Faust_1.txt` geladen (194879 Bytes, fsck HEALTHY); Airflow-Verzeichnisse auf die lokalen Kursdateien 2026/2027 unter `/opt/course-airflow` verlinkt.
- Hadoop/Hive/Jupyter waren kurz gestartet, wurden durch Container-/VM-Neustart beendet und nicht automatisch erneut gestartet. Airflow startet seine eigenen Dienste gemaess Image-Entrypoint.
- Daten liegen in den Container-Dateisystemen auf der VM-Disk. Stoppen/Starten erhaelt sie; Containerloeschung nicht.
- Weiterarbeit spaeter: `sudo docker exec -it --user hadoop --workdir /home/hadoop hadoop bash`, dann je nach Kurs `start-all.sh`, `hiveserver2`, `jupyter notebook` oder `spark-shell --master yarn`.

## Windows / SSH-Fallen
- Mehrzeilige Befehle über `ssh ... "bash -s"` brauchen LF statt CRLF.
- Verschachtelte Anführungszeichen in PowerShell sind fehleranfällig. Besser Skripte per `scp` auf die VM kopieren und mit `docker cp` in den Container.
