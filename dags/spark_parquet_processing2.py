import uuid
from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator
)

# Общие переменные
YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_SRC_BUCKET = S3_BUCKET_NAME[:]
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

# Подключения
YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    extra={
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "host": S3_ENDPOINT_URL,
    },
)
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)

# Функции
def setup_airflow_connections(*connections: Connection) -> None:
    session = Session()
    try:
        for conn in connections:
            if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
                session.add(conn)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def run_setup_connections(**kwargs):
    setup_airflow_connections(YC_S3_CONNECTION, YC_SA_CONNECTION)
    return True

def pause_execution(**kwargs):
    pause_minutes = 10
    print(f"Starting {pause_minutes} minute pause between jobs")
    time.sleep(pause_minutes * 60)
    print("Pause completed, starting next job")

# Настройки DAG
with DAG(
    dag_id="data_pipeline2",
    start_date=datetime(year=2025, month=3, day=7),
    schedule_interval=timedelta(minutes=60),
    catchup=False
) as dag:
    
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=run_setup_connections,
    )

    # Первый кластер для 2020-01-19
    create_cluster_1 = DataprocCreateClusterOperator(
        task_id="dp-cluster-create-1",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"dp-{uuid.uuid4()}",
        cluster_description="Cluster for 2020-01-19 processing",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=f"{S3_BUCKET_NAME}/airflow_logs/",
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.0",
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=20,
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=2,
        computenode_count=0,
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id=YC_SA_CONNECTION.conn_id,
        dag=dag,
    )
    
    process_parquet_1 = DataprocCreatePysparkJobOperator(
        task_id="dp-pyspark-job-1",
        main_python_file_uri=f"s3a://{S3_SRC_BUCKET}/src/pyspark_script_base2.py",      # меняю скрипт
        connection_id=YC_SA_CONNECTION.conn_id,
        args=["--bucket", S3_BUCKET_NAME, "--date", "2020-01-19"],                      # меняю дату
        dag=dag,
    )
    
    delete_cluster_1 = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-1",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    
    # Пауза 10 минут
    delay = PythonOperator(
        task_id="10_min_delay",
        python_callable=pause_execution,
        dag=dag,
    )
    
    # Второй кластер для 2020-02-18
    create_cluster_2 = DataprocCreateClusterOperator(
        task_id="dp-cluster-create-2",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"dp-{uuid.uuid4()}",
        cluster_description="Cluster for 2020-02-18 processing",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=f"{S3_BUCKET_NAME}/airflow_logs/",
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.0",
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=20,
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=2,
        computenode_count=0,
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id=YC_SA_CONNECTION.conn_id,
        dag=dag,
    )
    
    process_parquet_2 = DataprocCreatePysparkJobOperator(
        task_id="dp-pyspark-job-2",
        main_python_file_uri=f"s3a://{S3_SRC_BUCKET}/src/pyspark_script_base2.py",   # меняю скрипт 2 раз
        connection_id=YC_SA_CONNECTION.conn_id,
        args=["--bucket", S3_BUCKET_NAME, "--date", "2020-02-18"],                     # меняю дату 2 раз
        dag=dag,
    )
    
    delete_cluster_2 = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-2",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    
    # Последовательность выполнения с задержкой
    (
        setup_connections 
        >> create_cluster_1 
        >> process_parquet_1 
        >> delete_cluster_1 
        >> delay 
        >> create_cluster_2 
        >> process_parquet_2 
        >> delete_cluster_2
    )