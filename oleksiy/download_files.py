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
            task_id='download_ftp_file',
            ftp_conn_id='sftp_conn',  # Replace with your connection ID
            local_filepath='.',  # Replace with the local destination path
            remote_filepath='neoversity/olympics_dataset/games.csv',  # Replace with the path to the file on the FTP server
            operation='get',  # 'get' operation to download the file
            dag=dag
        )
    
    list_files_2 = BashOperator(
            task_id='list_files_2',  # Unique identifier for the task
            bash_command='ls -l',   # Bash command to execute
            dag=dag
        )
    
    # Set task dependencies (if any)
    list_files_1 >> download_ftp_file >> list_files_2
