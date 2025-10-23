from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "you", "retries": 0}

with DAG(
    dag_id="leads_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["demo"],
) as dag:

    spark_full = BashOperator(
        task_id="spark_fullload",
        bash_command=(
            "docker run --rm --network hadoop_default "
            "-v C:/Users/Admin/Desktop/project_HDFS:/work "
            "bde2020/spark-submit:2.4.5-hadoop2.7 "
            "bash -lc \"/spark/bin/spark-submit /work/01_ProdToHdfs/ingest_to_hdfs_parquet.py FullLoad\""
        ),
    )

    hive_register = BashOperator(
        task_id="hive_register_source_from_spark",
        bash_command=(
            "docker compose -p hadoop -f C:/HadoopDocker/docker-hive/docker-compose.yml "
            "cp C:/Users/Admin/Desktop/project_HDFS/02_HdfsToSource/source_from_spark.sql hive-server:/tmp/ && "
            "docker compose -p hadoop -f C:/HadoopDocker/docker-hive/docker-compose.yml "
            "exec hive-server bash -lc \"/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /tmp/source_from_spark.sql\""
        ),
    )

    presto_check = BashOperator(
        task_id="presto_show_tables",
        bash_command=(
            "docker run --rm --network hadoop_default eclipse-temurin:8-jre "
            "bash -lc \"wget -qO /tmp/presto.jar https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.181/presto-cli-0.181-executable.jar && "
            "java -jar /tmp/presto.jar --server http://presto-coordinator:8080 --catalog hive --schema source --execute 'SHOW TABLES'\""
        ),
    )

    spark_full >> hive_register >> presto_check
