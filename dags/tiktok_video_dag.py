from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from pymongo.errors import BulkWriteError
import os
import logging
import system as sy
import pandas as pd
import json
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

# File and directories paths
INPUT_PATH = "/opt/airflow/dags/influencers.csv"
OUTPUT_DIR = "/opt/airflow/dags/data/tiktok"
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
            usernames = sy.read_usernames_from_csv(file_path)
            logger.info(f"Successfully loaded {len(usernames)} usernames from {file_path}")
            logger.info(f"Usernames loaded: {usernames}")
            context['ti'].xcom_push(key='usernames', value=usernames)
        except Exception as e:
            logger.error(f"Error while loading usernames: {e}")
            raise
    
    def fetch_and_store_user_video(**context):
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.airflow_db
        collection = db.tiktok_user_video


        collection.create_index("video_id", unique=True)
     
        usernames = context['ti'].xcom_pull(key='usernames')
        logger.info(f"Usernames pulled from XCom: {usernames}")

        if not usernames:
            logger.warning("No usernames found, skipping data fetch and storage.")
            return

        all_videos = []
        new_video_ids = set() 

        for username in usernames:
            try:
                logger.info(f"Fetching data for username: {username}")
                user_data_list_df = te.tiktok_get_user_video_info(username=username)
                if isinstance(user_data_list_df, pd.DataFrame):
                    user_data_list = user_data_list_df.to_dict(orient='records')
                else:
                    logger.error(f"Unexpected data format for {username}: {type(user_data_list_df)}")
                    continue

                if user_data_list: 
                    all_videos.extend(user_data_list)
                    logger.info(f"Fetched data for {len(user_data_list)} videos for username: {username}")

            except Exception as e:
                logger.error(f"Error processing data for username {username} : {e}", exc_info=True)
                raise

        if all_videos: 
            try:
                # Try to insert all videos
                result = collection.insert_many(all_videos, ordered=False)

                # Track successfully inserted video IDs
                new_video_ids = set([all_videos[i]["video_id"] for i in range(len(all_videos)) 
                               if all_videos[i]["video_id"] not in [str(e["op"]["_id"]) 
                               for e in getattr(e, "details", {}).get("writeErrors", [])]])

                logger.info(f"Stored {len(new_video_ids)} videos in MongoDB successfully.")

            except BulkWriteError as e:
                 # Get successfully inserted video IDs even when some failed
                new_video_ids = set([all_videos[i]["video_id"] for i in range(len(all_videos)) 
                                   if all_videos[i]["video_id"] not in [str(err["op"]["_id"]) 
                                  for err in e.details.get("writeErrors", [])]])
                logger.info(f"Stored {len(new_video_ids)} new videos. Some videos already existed and were skipped.")

            # Store new video IDs in XCom for the transform task
            context['task_instance'].xcom_push(key='new_video_ids', value=list(new_video_ids))


    def transform_to_graph(**context):
        # Get only new video IDs from previous task
     new_video_ids = context['task_instance'].xcom_pull(
        task_ids='fetch_and_store_user_video',
        key='new_video_ids'
     )
     if not new_video_ids:
        logger.info("No new videos to transform")
        return

        # Connect to MongoDB
     mongo_hook = MongoHook(mongo_conn_id="mongo_default")
     mongo_client = mongo_hook.get_conn()
     db = mongo_client.airflow_db
     collection = db.tiktok_user_video

     # Use Neo4jHook to connect to Neo4j
     hook = Neo4jHook(conn_id="neo4j_default") 
     driver = hook.get_conn() 
     with driver.session() as session:
        # Fetch new video documents from MongoDB
         documents = collection.find({"video_id": {"$in": new_video_ids}})
         for doc in documents:
            username = doc.get("username")
            video_id = doc.get("video_id")

            if not username or not video_id:
                logging.warning(f"Skipping invalid document with username {username} or video_id {video_id}.")
                continue
            try:
                session.run(
                    """
                    MERGE (u:TikTokUser {username: $username})
                    MERGE (v:TikTokVideo {video_id: $video_id})
                    ON CREATE SET
                       v.video_description = $video_description,
                       v.create_time = $create_time,
                       v.region_code = $region_code,
                       v.share_count = $share_count,
                       v.view_count = $view_count,
                       v.like_count = $like_count,
                       v.comment_count = $comment_count,
                       v.music_id = $music_id,
                       v.voice_to_text = $voice_to_text,
                       v.is_stem_verified = $is_stem_verified,
                       v.video_duration = $video_duration, 
                       v.effect_ids = $effect_ids,
                       v.hashtag_info_list = $hashtag_info_list,
                       v.hashtag_names = $hashtag_names,
                       v.video_mention_list = $video_mention_list,
                       v.video_label = $video_label,
                       v.search_id = $search_id,
                       v.username = $username
                    MERGE (u)-[:PUBLISHED_ON_TIKTOK]->(v)  
                    """,
                    username=username,
                    video_id=video_id,
                    video_description=doc.get("video_description"),
                    create_time=doc.get("create_time"),
                    region_code=doc.get("region_code"),
                    share_count=doc.get("share_count"),
                    view_count=doc.get("view_count"),
                    like_count=doc.get("like_count"),
                    comment_count=doc.get("comment_count"),
                    music_id=doc.get("music_id"),
                    voice_to_text=doc.get("voice_to_text"),
                    is_stem_verified=doc.get("is_stem_verified"),
                    video_duration=doc.get("video_duration"),
                    effect_ids=doc.get("effect_ids"),
                    hashtag_info_list = json.dumps(doc.get("hashtag_info_list", [])),
                    hashtag_names = json.dumps(doc.get("hashtag_names", [])),
                    video_mention_list = json.dumps(doc.get("video_mention_list", [])),
                    video_label=json.dumps(doc.get("video_label", {})), 
                    search_id = doc.get("search_id")                                                    
                )
                logging.info(f"Data for username {username} and video_id {video_id} stored in Neo4j successfully.")
            except Exception as e:
                logging.error(f"Error processing data for username {username}and video_id {video_id}: {e}", exc_info=True)     
      

    load_usernames_task = PythonOperator(
        task_id='load_usernames',
        python_callable=load_usernames,
        op_kwargs={'file_path': INPUT_PATH},
    )

    fetch_and_store_user_video_task = PythonOperator(
        task_id='fetch_and_store_user_video',
        python_callable=fetch_and_store_user_video,
    )

    transform_to_graph_task = PythonOperator(
    task_id="transform_to_graph",
    python_callable=transform_to_graph,
    )
   

    load_usernames_task >> fetch_and_store_user_video_task >> transform_to_graph_task