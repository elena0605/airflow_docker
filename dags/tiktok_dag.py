from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
import os
import logging
import system as sy
import tiktok_etl as te


# Set up logging
logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# File and directories paths
#BASE_DIR = os.path.expanduser("~/airflow/tiktok_dag")
#BASE_DIR = os.path.expanduser("~/airflow_docker/dags")
#INPUT_PATH = os.path.join(BASE_DIR, "influencers.csv")
INPUT_PATH = "/opt/airflow/dags/influencers.csv"
OUTPUT_DIR = "/opt/airflow/dags/data/tiktok"
# OUTPUT_DIR = os.path.join(BASE_DIR, "data/tiktok")
os.makedirs(OUTPUT_DIR, exist_ok=True)


with DAG(
    "tiktok_dag",
    default_args=default_args,
    description="A simple DAG to fetch TikTok user info",
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['tiktok_user_info'],
) as dag:

    def load_usernames(file_path, **context):
        try:
            usernames = sy.read_usernames_from_csv(file_path)
            logger.info(f"Successfully loaded {len(usernames)} usernames from {file_path}")
            logger.info(f"Usernames loaded: {usernames}")
            context['ti'].xcom_push(key='usernames', value=usernames)
        except Exception as e:
            logger.error(f"Error while loading usernames: {e}")
            raise

    def fetch_all_user_data(**context):
        logger.info("Fetching data for all usernames...")
        usernames = context['ti'].xcom_pull(key='usernames')
        logger.info(f"Usernames pulled from XCom: {usernames}")

        if not usernames:
            logger.warning("No usernames found, skipping data fetch.")
            return
        for username in usernames:
            logger.info(f"Fetching data for username: {username}")
            try:
                te.tiktok_get_user_info(username=username, output_dir=OUTPUT_DIR, **context)
                logger.info(f"Successfully fetched data for username: {username}")
            except Exception as e:
                logger.error(f"Error fetching data for username {username}: {e}", exc_info=True)
    
    def store_user_data(**context):
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.airflow_db
        collection = db.tiktok_user_info

        # Fetch all usernames from XCom
        usernames = context['ti'].xcom_pull(task_ids='load_usernames', key='usernames')
        logger.info(f"Usernames pulled from XCom: {usernames}")
    
        if not usernames:
           logger.warning("No usernames found, skipping data storage.")
           return

        for username in usernames:
           # Retrieve the fetched data for this username from XCom
         user_data = context['ti'].xcom_pull(key=f'{username}_info_path')
         if user_data:
            try:
                # Prepare the data to be inserted into MongoDB
                user_data["username"] = username
                user_data["timestamp"] = datetime.now()

                # Insert data into MongoDB
                collection.insert_one(user_data)
                logger.info(f"Data for {username} inserted into MongoDB successfully.")
            except Exception as e:
                logger.error(f"Error inserting data into MongoDB for {username}: {e}", exc_info=True)
         else:
            logger.warning(f"No data found in XCom for username {username}, skipping insertion.")
    
    def transform_to_graph(**context):

     # Connect to MongoDB
     mongo_hook = MongoHook(mongo_conn_id="mongo_default")
     mongo_client = mongo_hook.get_conn()
     db = mongo_client.airflow_db
     collection = db.tiktok_user_info

    # Use Neo4jHook to connect to Neo4j
     hook = Neo4jHook(conn_id="neo4j_default") 
     driver = hook.get_conn() 
     with driver.session() as session:
       
        # Fetch all user data from MongoDB
        user_data = collection.find({})
        for user in user_data:
            # Create or update User nodes in Neo4j
            session.run(
                """
                MERGE (u:TikTokUser {username: $username})
                SET u.display_name = $display_name,
                    u.bio_description = $bio_description,
                    u.bio_url = $bio_url,
                    u.avatar_url = $avatar_url,
                    u.follower_count = $follower_count,
                    u.following_count = $following_count,
                    u.likes_count = $likes_count,
                    u.video_count = $video_count,
                    u.is_verified = $is_verified   
                """,
                username=user.get("username"),
                display_name=user.get("display_name"),
                bio_description=user.get("bio_description"),
                bio_url=user.get("bio_url"),
                avatar_url=user.get("avatar_url"),
                follower_count=user.get("follower_count", 0),
                following_count=user.get("following_count", 0),
                likes_count=user.get("likes_count", 0),
                video_count=user.get("video_count", 0),
                is_verified=user.get("is_verified", False),
            )
        

    load_usernames_task = PythonOperator(
        task_id='load_usernames',
        python_callable=load_usernames,
        op_kwargs={'file_path': INPUT_PATH},
    )

    fetch_user_info_task = PythonOperator(
        task_id='fetch_all_user_data',
        python_callable=fetch_all_user_data,
    )

    store_user_data_task = PythonOperator(
        task_id= 'store_user_data',
        python_callable= store_user_data,
    )

    transform_to_graph_task = PythonOperator(
    task_id="transform_to_graph",
    python_callable=transform_to_graph,
    )

    load_usernames_task >> fetch_user_info_task >> store_user_data_task >> transform_to_graph_task