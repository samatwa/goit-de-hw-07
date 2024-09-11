from airflow import DAG
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        'download_games',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["oleksiy"]  # Теги для класифікації DAG
) as dag:

    list_files_1 = BashOperator(
            task_id='list_files_1',  # Unique identifier for the task
            bash_command='ls -l',   # Bash command to execute
            dag=dag
        )
    
    download_ftp_file = FTPFileTransmitOperator(
        task_id='download_games_csv',
        ftp_conn_id='ftp_conn',  # The connection ID you set up in Airflow UI
        remote_source_path='/neoversity/olympics_dataset/games.csv',  # Source path on FTP server
        local_dest_path='/tmp/games.csv',  # Local destination path
        operation='get',  # 'get' for downloading the file
        create_intermediate_dirs=True,  # Create intermediate directories if needed
    )
    
    list_files_2 = BashOperator(
            task_id='list_files_2',  # Unique identifier for the task
            bash_command='ls -l',   # Bash command to execute
            dag=dag
        )
    
    # Set task dependencies (if any)
    list_files_1 >> download_ftp_file >> list_files_2
