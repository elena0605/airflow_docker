from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from pymongo.errors import BulkWriteError
import logging
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

with DAG(
    "tiktok_video_comments_dag",
    default_args=default_args,
    description="DAG to fetch and store TikTok video comments",
    schedule_interval=None,
    start_date=datetime(2025, 2, 13),
    catchup=False,
    tags=['tiktok_comments'],
) as dag:

    def fetch_and_store_comments(**context):
        # Connect to MongoDB
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = client.airflow_db
        videos_collection = db.tiktok_user_video
        comments_collection = db.tiktok_video_comments

        # Create indexes
        comments_collection.create_index("id", unique=True)
        comments_collection.create_index("video_id") # For faster lookups
        
        try:        
            # Get only videos that don't have comments fetched yet
            video_documents = videos_collection.find(
             {
                "comments_fetched": {"$ne": True}  # Only get videos where comments haven't been fetched
             }, 
             {"video_id": 1, "username": 1, "_id": 0}
            )  
            
            videos_processed = 0
            new_comments_count = 0

            for video_doc in video_documents:
                video_id = video_doc.get("video_id")
                username = video_doc.get("username")  

                if not video_id or not username:
                    continue

                logger.info(f"Fetching comments for video: {video_id}")
                
                try:
                    # Get comments for the video
                    comments = te.tiktok_get_video_comments(video_id)
                    
                    if comments:
                        for comment in comments:
                            comment["username"] = username
                            comment["fetched_at"] = datetime.now()
                        try:
                            # Insert comments with ordered=False to continue on duplicate key errors
                           result = comments_collection.insert_many(comments, ordered=False)
                           new_comments = len(result.inserted_ids)
                           new_comments_count += new_comments
                           logger.info(f"Stored {new_comments} new comments for video {video_id}")

                        except BulkWriteError as bwe:
                            # Count successful inserts even if some were duplicates
                            successful_inserts = len(comments) - len(bwe.details.get('writeErrors', []))
                            new_comments_count += successful_inserts
                            logger.info(f"Stored {successful_inserts} new comments for video {video_id} (some were duplicates)")
                           
                    else:
                        logger.info(f"No comments found for video {video_id}")
                    
                    # Mark video as processed
                    videos_collection.update_one(
                      {"video_id": video_id},
                      {
                        "$set": {
                            "comments_fetched": True,
                            "comments_fetched_at": datetime.now(),
                            "comments_count": len(comments) if comments else 0
                        }
                      }
                    )
                    videos_processed += 1

                except Exception as e:
                    logger.error(f"Error processing comments for video {video_id}: {e}", exc_info=True)
                    continue

            logger.info(f"Processed {videos_processed} videos, fetched {new_comments_count} new comments total")

            # Store stats in XCom
            context['task_instance'].xcom_push(key='comments_stats', value={
            'videos_processed': videos_processed,
            'new_comments_count': new_comments_count
            })

        except Exception as e:
            logger.error(f"Error in fetch_and_store_comments: {e}", exc_info=True)
            raise

    def transform_comments_to_graph(**context):
        # Get stats from previous task
        stats = context['task_instance'].xcom_pull(
            task_ids='fetch_and_store_comments',
            key='comments_stats'
        )

        if not stats or stats.get('new_comments_count', 0) == 0:
            logger.info("No new comments to transform")
            return
              
        # Connect to MongoDB
        mongo_hook = MongoHook(mongo_conn_id="mongo_default")
        mongo_client = mongo_hook.get_conn()
        db = mongo_client.airflow_db
        comments_collection = db.tiktok_video_comments

        # Connect to Neo4j
        hook = Neo4jHook(conn_id="neo4j_default")
        driver = hook.get_conn()

        with driver.session() as session:
            try:
                # Fetch only comments that haven't been processed for Neo4j
                comments = comments_collection.find(
                {"processed_for_neo4j": {"$ne": True}}
                )
                comments_processed = 0

                for comment in comments:
                    try:
                        # Create comment node and relationships
                        session.run(
                            """
                            MATCH (v:TikTokVideo {video_id: $video_id})
                            MERGE (c:TikTokComment {comment_id: $comment_id})
                            ON CREATE SET
                                c.text = $text,
                                c.like_count = $like_count,
                                c.reply_count = $reply_count,
                                c.create_time = datetime($create_time),
                                c.username = $username,
                                c.video_id = $video_id,
                                c.comment_id = $comment_id,
                                c.parent_comment_id = $parent_comment_id
                            MERGE (c)-[:COMMENTONTIKTOK]->(v)
                            """,
                            video_id=comment.get("video_id"),
                            comment_id=comment.get("id"),
                            text=comment.get("text"),
                            like_count=comment.get("like_count"),
                            reply_count=comment.get("reply_count"),
                            parent_comment_id=comment.get("parent_comment_id"),
                            create_time=comment.get("create_time"),
                            username=comment.get("username") 
                        )
                        # Mark comment as processed
                        comments_collection.update_one(
                            {"_id": comment["_id"]},
                            {"$set": {"processed_for_neo4j": True}}
                        )
                        comments_processed += 1
                        logger.debug(f"Processed comment {comment.get('id')} for video {comment.get('video_id')}")
                        
                    except Exception as e:
                        logger.error(f"Error processing comment {comment.get('id')}: {e}", exc_info=True)
                        continue  # Continue with next comment even if one fails

                logger.info(f"Successfully processed {comments_processed} comments to Neo4j")
                
            except Exception as e:
                logger.error(f"Error in transform_comments_to_graph: {e}", exc_info=True)
                raise

    # Define tasks
    fetch_and_store_comments_task = PythonOperator(
        task_id='fetch_and_store_comments',
        python_callable=fetch_and_store_comments,
    )

    transform_comments_to_graph_task = PythonOperator(
        task_id='transform_comments_to_graph',
        python_callable=transform_comments_to_graph,
    )

    # Set task dependencies
    fetch_and_store_comments_task >> transform_comments_to_graph_task 