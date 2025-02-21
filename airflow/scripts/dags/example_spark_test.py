from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='example_spark_run',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_script_task = DockerOperator(
        task_id='run_spark_script',
        image='data-ingestion-spark-app-image',
        container_name='data-ingestion-spark-app-container-test',
        network_mode='data-ingestion-network',
        command=[
            'spark-submit', '/app/scripts/bin/controller.py',
            'GCS_COINCAP', 'TestExecution2'
        ],
        entrypoint=[''],  # Overrides the tail -f /dev/null ENTRYPOINT
        auto_remove=True,
        force_pull=False,
        mount_tmp_dir=False,
        dag=dag,
    )
        
