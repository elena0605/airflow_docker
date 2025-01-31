from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from airflow.exceptions import AirflowFailException
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
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}

with DAG(
    "youtube_video_replies",
     default_args=default_args,
     description= 'A DAG to fetch, store, and transform YouTube video replies',
     schedule_interval=None,
     start_date=datetime(2025, 1, 30),
     catchup=False,
     tags=['youtube_video_replies'],

) as dag:
        def fetch_and_store_video_replies():
            try:
                hook = MongoHook(mongo_conn_id="mongo_default")
                client = hook.get_conn()
                db = client.airflow_db

                comment_collection = db.youtube_video_comments
                replies_collection = db.youtube_video_replies

                replies_collection.create_index("reply_id", unique=True)

                comments = comment_collection.find({}, {"comment_id": 1, "_id": 0})
                comment_ids = [comment["comment_id"] for comment in comments]              

                for comment_id in comment_ids:
                    logger.info(f"Fetching replies for comment_id: {comment_id}")
                    try:
                        replies = ye.get_replies(comment_id)
                        if replies:
                            logger.info(f"Storing {len(replies)} replies for comment_id: {comment_id}")
                            try:
                                replies_collection.insert_many(replies, ordered=False)
                                logger.info(f"Replies for comment_id: {comment_id} inserted into MongoDB successfully.")

                            except BulkWriteError as e:
                                logger.info(f"Some replies for comment_id {comment_id} already exist and were skipped. Error: {e}")   
                        else:
                          logger.info(f"No replies found for comment_id: {comment_id}")

                    except AirflowFailException as ae:
                       raise               
                    except Exception as e:
                        logger.error(f"Unexpected error while processing comment_id {comment_id}: {e}")
                        raise

                logger.info("Finished fetching and storing replies for all comments.")  

            except Exception as e:
              logger.error(f"Error in fetching replies process: {e}") 
              raise AirflowFailException(f"Failed to fetch and store replies: {e}")    
        
        def transform_to_graph():
            try:
                mongo_hook = MongoHook(mongo_conn_id="mongo_default")
                mongo_client = mongo_hook.get_conn()
                db = mongo_client.airflow_db
                collection = db.youtube_video_replies

                hook = Neo4jHook(conn_id="neo4j_default") 
                driver = hook.get_conn()

                with driver.session() as session:
                    documents = collection.find({}) 
                    for doc in documents:
                        logger.debug(f"Processing reply: {doc.get('reply_id')}")
                        session.run(
                            """
                            MERGE (r:YouTubeVideoReply {reply_id: $reply_id})
                            MERGE (c:YouTubeVideoComment {comment_id: $parent_id})                            
                            SET
                              r.reply_id = $reply_id,
                              r.channel_id = $channel_id,
                              r.parent_id = $parent_id,
                              r.text = $text,
                              r.authorDisplayName = $authorDisplayName,
                              r.authorProfileImageUrl = $authorProfileImageUrl,
                              r.authorChannelUrl = $authorChannelUrl,
                              r.canRate = $canRate,
                              r.viewerRating = $viewerRating,
                              r.likeCount = $likeCount,
                              r.publishedAt =$publishedAt,
                              r.updatedAt = $updatedAt
                            MERGE (r)-[:REPLYTOYOUTUBECOMMENT]->(c)
                            """,
                            reply_id = doc.get("reply_id"),
                            channel_id = doc.get("channel_id"),
                            parent_id = doc.get("parent_id"),
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
                        logger.info(f"Processed reply: {doc.get('reply_id')} for comment: {doc.get('parent_id')}")
            except Exception as e:  
                    logger.error(f"Error in transforming replies to Neo4j: {e}")
                    raise
           
        fetch_and_store_video_replies_task = PythonOperator(
            task_id = 'fetch_and_store_video_replies',
            python_callable = fetch_and_store_video_replies,
        )
        transform_to_graph_task = PythonOperator(
            task_id = 'transform_to_graph',
            python_callable = transform_to_graph,
        )

        fetch_and_store_video_replies_task >> transform_to_graph_task