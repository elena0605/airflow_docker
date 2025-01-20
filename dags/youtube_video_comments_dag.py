from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
import logging
import requests
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

with DAG(
    "youtube_video_comments",
     default_args=default_args,
     description= 'A DAG to fetch, store, and transform YouTube video comments',
     schedule_interval=None,
     start_date=datetime(2025, 1, 19),
     catchup=False,
     tags=['youtube_video_comments'],

) as dag:
        def fetch_and_store_video_comments():
          try:  
            hook = MongoHook(mongo_conn_id="mongo_default")
            client = hook.get_conn()
            db = client.airflow_db
            
            video_collection = db.youtube_channel_videos
            comment_collection = db.youtube_video_comments

            comment_collection.create_index("comment_id", unique=True)
            

            videos = video_collection.find({}, {"video_id": 1, "_id": 0})
            video_ids = [video["video_id"] for video in videos]

            logger.info(f"Found {len(video_ids)} videos to fetch comments for.")

            for video_id in video_ids:
                logger.info(f"Fetching comments for video_id: {video_id}")
                try:
                    comments = ye.get_top_level_comments(video_id)

                    if comments:
                        logger.info(f"Storing {len(comments)} comments for video_id: {video_id}")
                        try:
                            comment_collection.insert_many(comments,ordered=False) 
                            logger.info(f"Comments for video_id: {video_id} inserted into MongoDB successfully.")
                            
                        except BulkWriteError as e:
                            logger.info(f"Some comments for video_id {video_id} already exist and were skipped. Error: {e}")                                            
                    else:
                        logger.info(f"No comments found for video_id: {video_id}")

                except requests.exceptions.RequestException as e: 
                        logger.error(f"Network error while fetching comments for video_id {video_id}: {e}")
                except Exception as e:
                        logger.error(f"Unexpected error while processing video_id {video_id}: {e}")

            logger.info("Finished fetching and storing comments for all videos.")     
          except Exception as e:
              logger.error(f"Error in fetching video comments process: {e}")       

        
        def transform_to_graph():
         try:  
            mongo_hook = MongoHook(mongo_conn_id="mongo_default")
            mongo_client = mongo_hook.get_conn()
            db = mongo_client.airflow_db
            collection = db.youtube_video_comments

            logger.info("Fetching documents from MongoDB...")

            hook = Neo4jHook(conn_id="neo4j_default") 
            driver = hook.get_conn()

            with driver.session() as session:
                documents = collection.find({})              

                for doc in documents:
                    logger.debug(f"Processing comment: {doc.get('comment_id')}")
                    session.run(
                        """
                        MERGE(c:YouTubeVideoComment {comment_id: $comment_id})
                        MERGE(v:YouTubeVideo {video_id: $video_id})
                        SET
                          c.comment_id = $comment_id,
                          c.channel_id = $channel_id,
                          c.video_id = $video_id,
                          c.canReply = $canReply,
                          c.totalReplyCount = $totalReplyCount,
                          c.text = $text,
                          c.authorDisplayName = $authorDisplayName,
                          c.authorProfileImageUrl = $authorProfileImageUrl,
                          c.authorChannelUrl = $authorChannelUrl,
                          c.canRate = $canRate,
                          c.viewerRating = $viewerRating,
                          c.likeCount = $likeCount,
                          c.publishedAt =$publishedAt,
                          c.updatedAt = $updatedAt
                          MERGE (c)-[:COMMENTONYOUTUBEVIDEO]->(v)
                        """,
                        comment_id = doc.get("comment_id"),
                        channel_id = doc.get("channel_id"),
                        video_id = doc.get("video_id"),
                        canReply = doc.get("canReply"),
                        totalReplyCount = doc.get("totalReplyCount"),
                        text = doc.get("text"),
                        authorDisplayName = doc.get("authorDisplayName"),
                        authorProfileImageUrl = doc.get("authorProfileImageUrl"),
                        authorChannelUrl = doc.get("authorChannelUrl"),
                        canRate = doc.get("canRate"),
                        viewerRating = doc.get("viewerRating"),
                        likeCount = doc.get("likeCount"),
                        publishedAt = doc.get("publishedAt"),
                        updatedAt = doc.get("updatedAt"),
                    )
                    logger.info(f"Processed comment: {doc.get('comment_id')} for video: {doc.get('video_id')}")

         except Exception as e:  
                    logger.error(f"Error in transforming comments to Neo4j: {e}")

        fetch_and_store_video_comments_task = PythonOperator(
            task_id = 'fetch_and_store_video_comments',
            python_callable = fetch_and_store_video_comments,
        )

        transform_to_graph_task = PythonOperator(
            task_id = 'transform_to_graph',
            python_callable = transform_to_graph,
        )

        fetch_and_store_video_comments_task >> transform_to_graph_task