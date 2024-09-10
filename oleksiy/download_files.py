from airflow import DAG
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator
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

# Define the DAG
dag = DAG(
    'download_games.csv',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oleksiy']
)

download_ftp_file = FTPFileTransmitOperator(
        task_id='download_ftp_file',
        ftp_conn_id='sftp_conn',  # Replace with your connection ID
        local_filepath='neoversity/olympics_dataset/games.csv',  # Replace with the local destination path
        remote_filepath='.',  # Replace with the path to the file on the FTP server
        operation='get',  # 'get' operation to download the file
    )

# Set task dependencies (if any)
download_ftp_file
