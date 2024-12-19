from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import logging
import tiktok_etl as te

# Set up logging
logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Absolute paths
#BASE_DIR = os.path.expanduser("~/airflow/tiktok_dag")
#BASE_DIR = os.path.expanduser("~/airflow_docker/dags")
#INPUT_PATH = os.path.join(BASE_DIR, "influencers.csv")
INPUT_PATH = "/opt/airflow/dags/influencers.csv"
OUTPUT_DIR = "/opt/airflow/dags/data/tiktok"
# OUTPUT_DIR = os.path.join(BASE_DIR, "data/tiktok")
os.makedirs(OUTPUT_DIR, exist_ok=True)


with DAG(
    "tiktok_video_dag",
    default_args=default_args,
    description="A simple DAG to fetch TikTok user videos",
    schedule_interval=None,
    start_date=datetime(2024, 12, 19),
    catchup=False,
    tags=['tiktok_videos'],
) as dag:

    def load_usernames(file_path, **context):
        try:
            usernames = te.read_usernames_from_csv(file_path)
            logger.info(f"Successfully loaded {len(usernames)} usernames from {file_path}")
            logger.info(f"Usernames loaded: {usernames}")
            context['ti'].xcom_push(key='usernames', value=usernames)
        except Exception as e:
            logger.error(f"Error while loading usernames: {e}")
            raise

    def fetch_all_user_video_data(**context):
        logger.info("Fetching video data for all usernames...")
        usernames = context['ti'].xcom_pull(key='usernames')
        logger.info(f"Usernames pulled from XCom: {usernames}")

        if not usernames:
            logger.warning("No usernames found, skipping data fetch.")
            return
        for username in usernames:
            logger.info(f"Fetching data for username: {username}")
            try:
                te.tiktok_get_user_video_info(username=username, output_dir=OUTPUT_DIR, **context)
                logger.info(f"Successfully fetched data for username: {username}")
            except Exception as e:
                logger.error(f"Error fetching data for username {username}: {e}", exc_info=True)

    load_usernames_task = PythonOperator(
        task_id='load_usernames',
        python_callable=load_usernames,
        op_kwargs={'file_path': INPUT_PATH},
    )

    fetch_user_videos_task = PythonOperator(
        task_id='fetch_all_user_video_data',
        python_callable=fetch_all_user_video_data,
    )

    load_usernames_task >> fetch_user_videos_task