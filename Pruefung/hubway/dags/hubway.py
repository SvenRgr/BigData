from datetime import datetime
import logging
import os
import shutil
from zipfile import ZipFile

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.hdfs_operations import HdfsMkdirFileOperator, HdfsPutFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator


def clear_hubway_import_directory():
    with os.scandir("/home/airflow/hubway") as entries:
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(entry.path)
            else:
                os.remove(entry.path)
            logging.info("Deleted Hubway import entry: %s", entry.path)


def unzip_hubway_dataset():
    destination = os.path.realpath("/home/airflow/hubway/raw")
    os.makedirs(destination, exist_ok=True)
    with ZipFile("/home/airflow/hubway/hubway.zip") as archive:
        for member in archive.namelist():
            member_path = os.path.realpath(os.path.join(destination, member))
            if os.path.commonpath([destination, member_path]) != destination:
                raise AirflowException(
                    "Archive member would be extracted outside the destination: " + member
                )
        archive.extractall(destination)
    logging.info("Extracted Hubway dataset to %s", destination)

args = {
    'owner': 'airflow'
}

dag = DAG('Hubway', default_args=args, description='Hubway data processing DAG',
          schedule_interval='@monthly',
          start_date=datetime(2026, 11, 1), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='hubway',
    dag=dag,
)


clear_local_import_dir = PythonOperator(
    task_id='clear_import_dir',
    python_callable=clear_hubway_import_directory,
    dag=dag,
)

download_hubway_data = HttpDownloadOperator(
    task_id="download_hubway_data",
    download_uri="https://www.kaggle.com/api/v1/datasets/download/acmeyer/hubway-data",
    save_to="/home/airflow/hubway/hubway.zip",
    dag=dag,
)



unzip_hubway_data = PythonOperator(
    task_id="unzip_hubway_data",
    python_callable=unzip_hubway_dataset,
    dag=dag,
)

create_hubway_stations_hdfs_raw_dir = HdfsMkdirFileOperator(
    task_id="create_hubway_stations_hdfs_raw_dir",
    directory="/user/hadoop/hubway/stations/raw",
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hubway_stations_hdfs_final_dir = HdfsMkdirFileOperator(
    task_id="create_hubway_stations_hdfs_final_dir",
    directory="/user/hadoop/hubway/stations/final",
    hdfs_conn_id="hdfs",
    dag=dag,
)


put_hubway_stations_2017 = HdfsPutFileOperator(
    task_id="put_hubway_stations_2017",
    local_file="/home/airflow/hubway/raw/Hubway_Stations_as_of_July_2017.csv",
    remote_file="/user/hadoop/hubway/stations/raw/Hubway_Stations_as_of_July_2017.csv",
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hubway_tripdata_hdfs_raw_directories = BashOperator(
    task_id="create_hubway_tripdata_hdfs_raw_directories",
    bash_command="""
        set -eo pipefail
        source /home/airflow/.bashrc
        set -u
        shopt -s nullglob
        trip_files=(/home/airflow/hubway/raw/[0-9][0-9][0-9][0-9][0-9][0-9]-hubway-tripdata.csv)
        selected=0

        for csv_file in "${trip_files[@]}"; do
            filename="${csv_file##*/}"
            yyyymm="${filename:0:6}"
            selected=1
            hdfs dfs -mkdir -p "/user/hadoop/hubway/tripdata/raw/${yyyymm}"
        done
        if [[ "$selected" -eq 0 ]]; then
            echo "No Hubway trip CSV files found." >&2
            exit 1
        fi
    """,
    dag=dag,
)

create_hubway_tripdata_hdfs_final_directories = BashOperator(
    task_id="create_hubway_tripdata_hdfs_final_directories",
    bash_command="""
        set -eo pipefail
        source /home/airflow/.bashrc
        set -u
        hdfs dfs -mkdir -p "/user/hadoop/hubway/tripdata/final"
    """,
    dag=dag,
)

import_hubway_tripdata_to_hdfs = BashOperator(
    task_id="import_hubway_tripdata_to_hdfs",
    bash_command="""
        set -eo pipefail
        source /home/airflow/.bashrc
        set -u
        shopt -s nullglob
        trip_files=(/home/airflow/hubway/raw/[0-9][0-9][0-9][0-9][0-9][0-9]-hubway-tripdata.csv)
        selected=0

        for csv_file in "${trip_files[@]}"; do
            filename="${csv_file##*/}"
            yyyymm="${filename:0:6}"
            selected=1
            hdfs_dir="/user/hadoop/hubway/tripdata/raw/${yyyymm}"
            hdfs dfs -put -f "$csv_file" "$hdfs_dir/"
        done
        if [[ "$selected" -eq 0 ]]; then
            echo "No Hubway trip CSV files found." >&2
            exit 1
        fi
    """,
    dag=dag,
)

clean_hubway_tripdata = SparkSubmitOperator(
    task_id="clean_hubway_tripdata",
    conn_id="spark",
    spark_binary="/home/airflow/spark/bin/spark-submit",
    application="/home/airflow/airflow/python/clean_tripdata.py",
    executor_cores=1,
    executor_memory="1g",
    num_executors=1,
    name="hubway_clean_tripdata",
    conf={"spark.pyspark.python": "python3"},
    application_args=[
        "--hdfs_source_dir", "/user/hadoop/hubway/tripdata/raw",
        "--hdfs_target_dir", "/user/hadoop/hubway/tripdata/final",
    ],
    dag=dag,
)

clean_station_tripdata = SparkSubmitOperator(
    task_id="clean_station_tripdata",
    conn_id="spark",
    spark_binary="/home/airflow/spark/bin/spark-submit",
    application="/home/airflow/airflow/python/clean_stationdata.py",
    executor_cores=1,
    executor_memory="1g",
    num_executors=1,
    name="hubway_clean_stationdata",
    conf={"spark.pyspark.python": "python3"},
    application_args=[
        "--hdfs_source_dir", "/user/hadoop/hubway/stations/raw",
        "--hdfs_target_dir", "/user/hadoop/hubway/stations/final",
    ],
    dag=dag,
)

calculate_kpis = SparkSubmitOperator(
    task_id="calculate_kpis",
    conn_id="spark",
    spark_binary="/home/airflow/spark/bin/spark-submit",
    application="/home/airflow/airflow/python/calc_kpis.py",
    packages="com.crealytics:spark-excel_2.11:0.11.1",
    executor_cores=1,
    executor_memory="1g",
    num_executors=1,
    name="hubway_calculate_kpis",
    conf={"spark.pyspark.python": "python3"},
    application_args=[
        "--hdfs_trip_dir", "/user/hadoop/hubway/tripdata/final",
        "--hdfs_station_dir", "/user/hadoop/hubway/stations/final",
        "--hdfs_target_dir", "/user/hadoop/hubway/kpis",
    ],
    dag=dag,
)





create_local_import_dir >> clear_local_import_dir >> download_hubway_data >> unzip_hubway_data
unzip_hubway_data >> create_hubway_stations_hdfs_raw_dir
unzip_hubway_data >> create_hubway_stations_hdfs_final_dir
create_hubway_stations_hdfs_raw_dir >> put_hubway_stations_2017
unzip_hubway_data >> create_hubway_tripdata_hdfs_raw_directories >> import_hubway_tripdata_to_hdfs
unzip_hubway_data >> create_hubway_tripdata_hdfs_final_directories
import_hubway_tripdata_to_hdfs >> clean_hubway_tripdata
create_hubway_tripdata_hdfs_final_directories >> clean_hubway_tripdata
put_hubway_stations_2017 >> clean_station_tripdata
clean_hubway_tripdata >> calculate_kpis
clean_station_tripdata >> calculate_kpis