from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='example_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_script_task = DockerOperator(
        task_id='run_python_script_assets',
        image='data-ingestion-python-app-image',
        container_name='data-ingestion-python-app-container-test',
        network_mode='data-ingestion-network',
        command=['python3', '/app/scripts/bin/test.py'],  # script that prints hello world
        entrypoint=[''],  # Overrides the tail -f /dev/null ENTRYPOINT
        auto_remove=True,
        force_pull=False,
        mount_tmp_dir=False,
        dag=dag,
    )