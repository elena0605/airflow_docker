from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from pymongo.errors import DuplicateKeyError
from callbacks import task_failure_callback, task_success_callback
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
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}


INPUT_PATH = "/opt/airflow/dags/influencers.csv"
OUTPUT_DIR = "/opt/airflow/dags/data/tiktok"
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
           # Strip leading/trailing spaces from each username
        usernames = [username.strip() for username in usernames] 
        
        for username in usernames:
            logger.info(f"Fetching data for username: {username}")
            try:
                te.tiktok_get_user_info(username=username, output_dir=OUTPUT_DIR, **context)
                logger.info(f"Successfully fetched data for username: {username}")
            except Exception as e:
                logger.error(f"Error fetching data for username {username}: {e}", exc_info=True)
                raise
    
    def store_user_data(**context):
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.airflow_db
        collection = db.tiktok_user_info

        # Track new users
        new_usernames = []

        collection.create_index("username", unique=True)

        # Get usernames from previous task
        usernames = context['ti'].xcom_pull(task_ids='load_usernames', key='usernames')
        logger.info(f"Processing {len(usernames)} users")
    
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
                try:
                    collection.insert_one(user_data)
                    new_usernames.append(username)
                    logger.info(f"New user stored: {username}")

                except DuplicateKeyError as e:
                    logger.info(f"User Info for username: {username} already exist. Skipping insertion. Error: {e}")               
                
            except Exception as e:
                logger.error(f"Error inserting data into MongoDB for {username}: {e}", exc_info=True)
                raise
         else:
            logger.warning(f"No data found in XCom for username {username}, skipping insertion.")

        context['task_instance'].xcom_push(key='new_usernames', value=new_usernames)
        logger.info(f"Stored {len(new_usernames)} new users")

    def transform_to_graph(**context):
     # Get newly added usernames from XCom
     new_usernames = context['task_instance'].xcom_pull(
            task_ids='store_user_data',
            key='new_usernames'
        )
     if not new_usernames:
            logger.info("No new users to transform")
            return
            
     logger.info(f"Transforming {len(new_usernames)} new users to graph")      
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
        documents = collection.find({"username": {"$in": new_usernames}})
        for doc in documents:
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
                username=doc.get("username"),
                display_name=doc.get("display_name"),
                bio_description=doc.get("bio_description"),
                bio_url=doc.get("bio_url"),
                avatar_url=doc.get("avatar_url"),
                follower_count=doc.get("follower_count", 0),
                following_count=doc.get("following_count", 0),
                likes_count=doc.get("likes_count", 0),
                video_count=doc.get("video_count", 0),
                is_verified=doc.get("is_verified", False),
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