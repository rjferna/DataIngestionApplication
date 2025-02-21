from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='wf_coincap_etl_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_extract_assets_task = DockerOperator(
        task_id='extract_load_assets',
        image='data-ingestion-python-app-image',
        container_name='data-ingestion-python-app-container-assets',
        network_mode='data-ingestion-network',
        command=[
            'python3', '/app/scripts/bin/controller.py',
            '-s', 'REQUEST', 
            '-cn', 'COINCAP', 
            '-a', 'ASSETS', 
            '-c', '/app/scripts/bin/config2.ini', 
            '-l', 'info',
            '--print_log'
        ],
        entrypoint=[''],  # Overrides the tail -f /dev/null ENTRYPOINT
        auto_remove=True,
        force_pull=False,
        mount_tmp_dir=False,
        dag=dag,
    )
       
    run_extract_bitcoin_history_task = DockerOperator(
        task_id='extract_load_bitcoin_history',
        image='data-ingestion-python-app-image',
        container_name='data-ingestion-python-app-container-bitcoin_history',
        network_mode='data-ingestion-network',
        command=[
            'python3', '/app/scripts/bin/controller.py',
            '-s', 'REQUEST', 
            '-cn', 'COINCAP', 
            '-a', 'BITCOIN_HISTORY', 
            '-c', '/app/scripts/bin/config2.ini', 
            '-l', 'info',
            '--print_log'
        ],
        entrypoint=[''],
        auto_remove=True,
        force_pull=False,
        mount_tmp_dir=False,
        dag=dag,
    )

    run_extract_solana_history_task = DockerOperator(
        task_id='extract_load_solana_history',
        image='data-ingestion-python-app-image',
        container_name='data-ingestion-python-app-container-solana_history',
        network_mode='data-ingestion-network',
        command=[
            'python3', '/app/scripts/bin/controller.py',
            '-s', 'REQUEST', 
            '-cn', 'COINCAP', 
            '-a', 'SOLANA_HISTORY', 
            '-c', '/app/scripts/bin/config2.ini', 
            '-l', 'info',
            '--print_log'
        ],
        entrypoint=[''],  
        auto_remove=True,
        force_pull=False,
        mount_tmp_dir=False,
        dag=dag,
    )

    run_bitcoin_linear_regression_task = DockerOperator(
        task_id='bitcoin_history_linear_regression',
        image='data-ingestion-spark-app-image',
        container_name='data-ingestion-spark-app-container-bitcoin-lr',
        network_mode='data-ingestion-network',
        command=[
            'spark-submit', '/app/scripts/bin/controller.py',
            'GCS_COINCAP', 'TestExecution2'
        ],
        entrypoint=[''],  
        auto_remove=True,
        force_pull=False,
        mount_tmp_dir=False,
        dag=dag,
    )


    # Set task Dependencies
    run_extract_assets_task
    run_extract_solana_history_task
    run_extract_bitcoin_history_task >> run_bitcoin_linear_regression_task