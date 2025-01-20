from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
import logging
import system as sy
from pymongo.errors import BulkWriteError
import youtube_etl as ye

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

# Files and directories paths
INPUT_PATH = "/opt/airflow/dags/youtube_influencers.csv"

with DAG(
    "youtube_channel_videos",
     default_args=default_args,
     description= 'A DAG to fetch, store, and transform YouTube channel videos',
     schedule_interval=None,
     start_date=datetime(2025, 1, 15),
     catchup=False,
     tags=['youtube_channel_videos'],

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

        def fetch_and_store_channel_videos(**context):
            hook = MongoHook(mongo_conn_id="mongo_default")
            client = hook.get_conn()
            db = client.airflow_db
            collection = db.youtube_channel_videos

            collection.create_index("video_id", unique=True )

            channels_ids = context['ti'].xcom_pull(key='channels_ids')
            logger.info(f"channels_ids pulled from XCom: {channels_ids}")

            if not channels_ids:
               logger.warning("No channels_ids found, skipping data fetch and storage.")
               return

            start_date = "2024-01-01T00:00:00Z"
            end_date = "2024-12-31T23:59:59Z"

            for username, channel_id in channels_ids.items():
                logger.info(f"Fetching videos for {username} (channel_id: {channel_id})")

                try:
                    videos = ye.get_videos_by_date(channel_id, start_date, end_date)
                    if videos:
                        try:
                            collection.insert_many(videos, ordered=False)
                            logger.info(f"Videos for {username} with channel_id: {channel_id} inserted into MongoDB successfully.")

                        except BulkWriteError as e:
                            logger.info(f"Some videos already exist and were skipped. Error details: {e.details}")  
                    else:
                        logger.info(f"No videos found for {username} (channel_id: {channel_id}) in 2024.")
                
                except Exception as e:
                    logger.error(f"Error fetching or storing videos for {username} (channel_id: {channel_id}): {e}")
        

        def transform_to_graph():
            mongo_hook = MongoHook(mongo_conn_id="mongo_default")
            mongo_client = mongo_hook.get_conn()
            db = mongo_client.airflow_db
            collection = db.youtube_channel_videos

            hook = Neo4jHook(conn_id="neo4j_default") 
            driver = hook.get_conn()
            with driver.session() as session:
                documents = collection.find({})
                for doc in documents:
                    thumbnails = doc.get("thumbnails", {})
                    session.run(
                        """
                        MERGE(c:YouTubeChannel {channel_id: $channel_id})
                        MERGE(v:YouTubeVideo {video_id: $video_id})
                        SET
                           v.video_title = $video_title,
                           v.video_id = $video_id,
                           v.published_at = $published_at,
                           v.channel_id = $channel_id,
                           v.video_description = $video_description,
                           v.channel_title = $channel_title,
                           v.thumbnail_url = $thumbnail_url,
                           v.thumbnail_width = $thumbnail_width,
                           v.thumbnail_height = $thumbnail_height
                        MERGE (c)-[:POSTEDONYOUTUBE]->(v)
                        """,
                        video_title = doc.get("video_title"),
                        video_id = doc.get("video_id"),
                        published_at = doc.get("published_at"),
                        channel_id = doc.get("channel_id"),
                        video_description = doc.get("video_description", ""),
                        channel_title = doc.get("channel_title", ""),
                        thumbnail_url=thumbnails.get("url"),
                        thumbnail_width=thumbnails.get("width"),
                        thumbnail_height=thumbnails.get("height")
                    )


        load_channels_ids_task = PythonOperator(
          task_id = 'load_channels_ids',
          python_callable=load_channels_ids,
          op_kwargs={'file_path': INPUT_PATH},
        )
        
        fetch_and_store_channel_videos_task = PythonOperator(
            task_id = 'fetch_and_store_channel_videos',
            python_callable = fetch_and_store_channel_videos,
        )

        transform_to_graph_task = PythonOperator(
            task_id = 'transform_to_graph',
            python_callable = transform_to_graph,
        )

        load_channels_ids_task >> fetch_and_store_channel_videos_task >> transform_to_graph_task