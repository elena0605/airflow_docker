from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
import logging
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
    "retry_delay": timedelta(minutes=2),
}

INPUT_PATH = "/opt/airflow/dags/influencers.csv"

with DAG(
    "tiktok_video_dag_test",
    default_args=default_args,
    description="A simple DAG to fetch TikTok user videos",
    schedule_interval=None,
    start_date=datetime(2024, 12, 19),
    catchup=False,
    tags=['tiktok_videos'],
) as dag:

    @task
    def load_usernames(file_path):
        # Load usernames from CSV file
        usernames = te.read_usernames_from_csv(file_path)
        logger.info(f"Successfully loaded {len(usernames)} usernames from {file_path}")
        return usernames

    @task
    def fetch_and_store(username):
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.airflow_db
        collection = db.user_video

        logger.info(f"Fetching data for username: {username}")
        user_data_list_df = te.tiktok_get_user_video_info(username=username)
        if isinstance(user_data_list_df, pd.DataFrame):
            user_data_list = user_data_list_df.to_dict(orient='records')
        else:
            logger.error(f"Unexpected data format for {username}: {type(user_data_list_df)}")
            return None

        for user_data in user_data_list:
            user_data["timestamp"] = datetime.now()
            collection.insert_one(user_data)
            logger.info(f"Data for {username} stored in MongoDB successfully.")
        
        # Return the username to pass to the next task
        return username

    @task
    def transform(username):
        mongo_hook = MongoHook(mongo_conn_id="mongo_default")
        mongo_client = mongo_hook.get_conn()
        db = mongo_client.airflow_db
        collection = db.user_video

        hook = Neo4jHook(conn_id="neo4j_default")
        driver = hook.get_conn()
        with driver.session() as session:
            documents = collection.find({"username": username})
            for doc in documents:
                video_id = doc.get("id")
                if not video_id:
                    logger.warning(f"Skipping invalid document for username {username}.")
                    continue
                session.run(
                    """
                    MERGE (u:User {username: $username})
                    MERGE (v:Video {video_id: $video_id})
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
                       v.video_label = $video_label
                    MERGE (u)-[:POSTED]->(v)  
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
                    hashtag_info_list=json.dumps(doc.get("hashtag_info_list", [])),
                    hashtag_names=json.dumps(doc.get("hashtag_names", [])),
                    video_mention_list=json.dumps(doc.get("video_mention_list", [])),
                    video_label=json.dumps(doc.get("video_label", {})),
                )
                logger.info(f"Data for username {username} and video_id {video_id} stored in Neo4j successfully.")

    # Load usernames from CSV
    usernames = load_usernames(INPUT_PATH)

    # Fetch and store data for each username, expand the task to parallelize
    fetched_usernames = fetch_and_store.expand(username=usernames)

    # Transform data for each username, expand the task to parallelize
    transform.expand(username=fetched_usernames)
