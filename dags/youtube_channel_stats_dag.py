from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
import logging
from pymongo.errors import DuplicateKeyError
import system as sy
import youtube_etl as ye
from airflow.exceptions import AirflowFailException



# Set up logging
logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}

# Files and directories paths
INPUT_PATH = "/opt/airflow/dags/youtube_influencers.csv"

with DAG(
    "youtube_channel_stats_dag",
     default_args=default_args,
     description= 'A DAG to fetch, store, and transform YouTube channel statistics',
     schedule_interval=None,
     start_date=datetime(2025, 1, 14),
     catchup=False,
     tags=['youtube_channel_stats'],

) as dag:
        def load_channels_ids(file_path, **context):
            try:
                
                channels_ids = sy.read_channel_ids_from_csv(file_path)
                logger.info(f"Successfully loaded channels ids from {file_path}")
                logger.info(f"Usernames loaded: {channels_ids}")
                context['ti'].xcom_push(key='channels_ids', value=channels_ids)
            except Exception as e:
                logger.error(f"Error while loading channles_id: {e}")
                raise

        def fetch_and_store_channel_stats(**context):
             task_instance = context['task_instance']
             hook = MongoHook(mongo_conn_id="mongo_default")
             client = hook.get_conn()
             db = client.airflow_db
             collection = db.youtube_channel_stats

             collection.create_index("channel_id", unique=True )

             channels_ids = context['ti'].xcom_pull(key='channels_ids')
             logger.info(f"channels_ids pulled from XCom: {channels_ids}")

             if not channels_ids:
               logger.warning("No channels_ids found, skipping data fetch and storage.")
               return
             
             for username in channels_ids:
                try:
                    channel_id = channels_ids[username]
                    logger.info(f"Fetching channel stats for channel with username: {username} and channel_id: {channel_id}")
                    channel_stats = ye.get_channels_statistics(channel_id)

                    if channel_stats is None:
                       raise AirflowFailException(f"Failed to fetch statistics for channel ID: {channel_id}")             

                    channel_stats['timestamp'] = datetime.now()                   
                    try:
                        collection.insert_one(channel_stats)
                        logger.info(f"Channel stats for {username} inserted into MongoDB successfully.")

                    except DuplicateKeyError as e: 
                         logger.info(f"Channel stats for channel_id {channel_id} already exist. Skipping insertion. Error: {e}")     
                                                     
                except Exception as e:
                    logger.error(f"Error fetching stats for {username} (channel_id: {channel_id}): {e}")
                    raise e

        def transform_to_graph():
             mongo_hook = MongoHook(mongo_conn_id="mongo_default")
             mongo_client = mongo_hook.get_conn()
             db = mongo_client.airflow_db
             collection = db.youtube_channel_stats

             hook = Neo4jHook(conn_id="neo4j_default") 
             driver = hook.get_conn() 
             with driver.session() as session:
                documents = collection.find({})
                for doc in documents:
                    topic_categories = doc.get("topic_categories", [])
                
                    session.run(
                        """
                        MERGE(c:YouTubeChannel {channel_id: $channel_id})
                        SET c.channel_id = $channel_id,
                            c.title = $title,
                            c.view_count = $view_count,
                            c.subscriber_count = $subscriber_count,
                            c.video_count = $video_count,
                            c.hidden_subscriber_count = $hidden_subscriber_count,
                            c.description = $description,
                            c.keywords = $keywords,
                            c.country = $country,
                            c.topic_categories = $topics
                        """,
                        channel_id = doc.get("channel_id"),
                        title = doc.get("title"),
                        view_count = doc.get("view_count", 0),
                        subscriber_count = doc.get("subscriber_count", 0),
                        video_count = doc.get("video_count", 0),
                        hidden_subscriber_count = doc.get("hidden_subscriber_count", False),
                        description = doc.get("description", ""),
                        keywords = doc.get("keywords", []),
                        country = doc.get("country", "Unknown"),
                        topics = doc.get("topic_categories", []) 
                    )

        load_channels_ids_task = PythonOperator(
            task_id = 'load_channels_ids',
            python_callable=load_channels_ids,
            op_kwargs={'file_path': INPUT_PATH},
        )

        fetch_and_store_channel_stats_task = PythonOperator(
            task_id = 'fetch_and_store_channel_stats',
            python_callable = fetch_and_store_channel_stats,
        )

        transform_to_graph_task = PythonOperator(
            task_id="transform_to_graph",
            python_callable=transform_to_graph,
    )

        load_channels_ids_task >> fetch_and_store_channel_stats_task >> transform_to_graph_task

     

