# Setup-Notizen (Big Data, DHBW)

Abweichungen vom Setup in den Folien, die für die nächsten Stunden gelten.

## VM
- Instanz `big-data`, Zone `us-central1-a`, Typ `n4-standard-8` (nicht europe-west3-a / n2-highmem-4).
- Die externe IP ändert sich eventuell nach einem Neustart. Dann `HostName` in `~/.ssh/config` (Host `big-data`) anpassen.
- Zugriff: `ssh big-data`. In der SSH-Config sind `LocalForward` für 8088, 9870, 9864 und 10000 gesetzt. Die SSH-Sitzung muss offen sein, damit die Web-UIs und DBeaver funktionieren.
- VM am Ende der Stunde stoppen: `gcloud compute instances stop big-data --zone us-central1-a`.

## Container
### Spark setup (26.10.2026, replication slides)
- Current external VM IP: `34.9.101.52`. SSH alias `big-data` is updated; `HostKeyAlias` retains the previously trusted VM key.
- Active container: `hadoop`, image `marcelmittelstaedt/spark_base:latest`, network `bigdatanet`, `--ulimit nofile=65536:65536`.
- Published ports: 8088, 9870, 9864, 10000, 8032, 8030, 8031, 9000, 8888. SSH configuration also forwards 8888 for later Jupyter use.
- Existing `hiveserver_base_container` is stopped and preserved, including its data. Do not start it alongside `hadoop`: their published ports conflict.
- HDFS and YARN are running. `/home/hadoop/Faust_1.txt` was uploaded to `/user/hadoop/Faust_1.txt` (187937 bytes); matching SHA-256 checksums and HDFS fsck HEALTHY verified.
- Preparation stops after "Add Test text file to HDFS". Spark shell, HiveServer2 and Jupyter have not been started in the new container.
- Continue with `ssh big-data`, `sudo docker exec -it --user hadoop --workdir /home/hadoop hadoop bash`, then `spark-shell --master yarn`.
- After a VM restart: `sudo docker start hadoop`, wait for startup, then `sudo docker exec --user hadoop hadoop /home/hadoop/hadoop/sbin/start-all.sh`.

### Previous Hive setup
- Image `marcelmittelstaedt/hiveserver_base:latest` (Hive 3.1.2), Container `hiveserver_base_container`.
- WICHTIG: Container mit `--ulimit nofile=65536:65536` starten. Ohne das Flag hängt `start-all.sh` beim Start, weil `pdsh` mit dem riesigen Standard-nofile-Limit in eine Endlosschleife läuft. Der Container bleibt dann bei "Starting namenodes on [localhost]" stehen.

```
docker run -dit --name hiveserver_base_container --ulimit nofile=65536:65536 \
  -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 10000:10000 -p 9000:9000 \
  marcelmittelstaedt/hiveserver_base:latest
```
- Warten, bis `docker logs hiveserver_base_container` "Container Startup finished." zeigt.
- Alte Container vorher entfernen: `docker rm -f hive_base_container hiveserver_base_container` (löscht auch die HDFS-Daten darin).

## Im Container (als User `hadoop`: `docker exec -it hiveserver_base_container bash`, dann `sudo su hadoop`)
- `start-all.sh`, dann `nohup hive/bin/hiveserver2 > hiveserver2.log 2>&1 &`.
- HiveServer2 braucht 1-3 Minuten, bis Port 10000 offen ist. "Connection refused" heißt meistens nur: noch nicht fertig.
- Test: `beeline -u jdbc:hive2://localhost:10000 -n hadoop`.
- DBeaver: Apache Hive, `jdbc:hive2://localhost:10000/default`, Benutzer `hadoop`, kein Passwort.

## IMDb-Daten
- Dateien in `/home/hadoop/data` im Container, in HDFS unter `/user/hadoop/imdb/{title_basics,title_ratings,name_basics}`. Sie gehen beim Löschen des Containers verloren.
- Die SQL-Dateien für die Tabellen liegen unter `exercises/winter_semester_2026-2027/02_hive/` (`title_basics.sql`, `title_ratings.sql`, `name_basiscs.sql`).
- Bei dynamischem Partitionieren `hive.exec.max.dynamic.partitions` und `hive.exec.max.dynamic.partitions.pernode` erhöhen, zum Beispiel auf 10000. Der Standard von 1000 reicht für die rund 150 Jahre nicht immer aus.

## Windows / SSH-Fallen
- Mehrzeilige Befehle über `ssh ... "bash -s"` brauchen LF statt CRLF.
- Verschachtelte Anführungszeichen in PowerShell sind fehleranfällig. Besser Skripte per `scp` auf die VM kopieren und mit `docker cp` in den Container.
