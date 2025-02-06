import requests
import configparser
import logging
from datetime import datetime
from airflow.exceptions import AirflowFailException
import gridfs
from pymongo import MongoClient

# Set up logging - log to airflow logs & console
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.DEBUG)  # Set the log level
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
if not logger.hasHandlers():  # Avoid duplicate handlers
    logger.addHandler(stream_handler)

# Reading the config file for accessing the API keys
# config = configparser.ConfigParser()
# config.read('/opt/airflow/dags/config.ini')
# api_key = config["YOUTUBE"]["API_KEY"]
#API_KEY = 'AIzaSyCu9avifWhxwAiGCrOhhkcsMIfXdVIVdX0'
API_KEY = 'AIzaSyBB6dXRFOT2LnlF1TabeR9OEwRF50dR_rs'

def save_thumbnail(image_url, video_id, channel_title):
    client = MongoClient("mongodb://airflow:tiktok@mongodb:27017/")
    db = client["airflow_db"]
    fs = gridfs.GridFS(db)
    try:
        response = requests.get(image_url)
        response.raise_for_status()
        filename = f"{video_id}_{channel_title.replace(' ', '_')}_{image_url.split('/')[-1]}"
        return fs.put(response.content, filename=filename, video_id = video_id, channel_title = channel_title)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download image: {image_url}, Error: {e}")
        return None

def get_channels_statistics(channel_id):

    logger.debug(f"Fetching statistics for channel ID: {channel_id}")
    url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics,brandingSettings&id={channel_id}&key={API_KEY}'

    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        logger.debug(f"Received data for channel ID: {channel_id} - {data}")

    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}") from http_err
        
    except requests.exceptions.RequestException as req_err:
          raise Exception(f"Request failed for channel ID: {channel_id} - {req_err}") from req_err
             
    if 'items' in data and len(data['items']) > 0:
        item = data['items'][0]
        stats = item.get('statistics', {})
        branding = item.get('brandingSettings', {}).get('channel', {})
        title = branding.get('title', 'Unknown')
        logger.info(f"Statistics fetched successfully for channel ID: {channel_id}")
        return {
             'channel_id': channel_id,
             'title': title,
             'view_count': stats.get('viewCount', '0'),
             'subscriber_count': stats.get('subscriberCount', '0'),
             'video_count': stats.get('videoCount', '0'),
             'hidden_subscriber_count': stats.get('hiddenSubscriberCount', False)

        }
    else:
        raise Exception(f"No items found for channel ID: {channel_id}")
        


def get_videos_by_date(channel_id, start_date, end_date):
    base_url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&type=video&order=date&maxResults=50&key={API_KEY}'
    videos = []
    next_page_token = None
    logger.info(f"Fetching videos for channel_id: {channel_id} from {start_date} to {end_date}")

    while True:
     url = base_url + f'&publishedAfter={start_date}&publishedBefore={end_date}'
     if next_page_token:
            url += f'&pageToken={next_page_token}'
     try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        logger.debug(f"Fetched {len(data.get('items', []))} videos from page.")

        for item in data.get('items', []):
            video_title = item['snippet']['title']
            video_id = item['id']['videoId']
            published_at = item['snippet']['publishedAt']
            video_description = item['snippet']['description']
            channelTitle = item['snippet']['channelTitle']
            thumbnails = item['snippet']['thumbnails']['high']['url']
             
            # Save thumbnail and get GridFS ID
            thumbnail_id = save_thumbnail(thumbnails, video_id, channelTitle)

            videos.append({
                          'video_title': video_title, 
                          'video_id': video_id, 
                          'published_at': published_at, 
                          'channel_id': channel_id, 
                          'video_description': video_description, 
                          'channel_title' : channelTitle,
                          'thumbnails': {'gridfs_id': thumbnail_id},
                          "fetched_time":datetime.now()       
                           })

        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            logger.info("No more pages to fetch.")
            break

     except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise AirflowFailException(f"HTTP error fetching videos for channel {channel_id}: {http_err}")

     except requests.exceptions.RequestException as req_err:
            logger.error(f"Request failed: {req_err}")
            raise AirflowFailException(f"Request failed for channel {channel_id}: {req_err}")

     except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise AirflowFailException(f"Unexpected error fetching videos for channel {channel_id}: {e}")    

    logger.info(f"Total videos fetched: {len(videos)}")
    return videos



def get_top_level_comments(video_id):
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        'part': 'snippet',
        'videoId': video_id,
        'maxResults': 100, 
        'key': API_KEY
    }

    comments = []
    next_page_token = None

    logger.debug(f"Starting to fetch top-level comments for video_id: {video_id}")

    while True:
        if next_page_token:
            params['pageToken'] = next_page_token
        try:
            response = requests.get(url, params=params)
            response.raise_for_status() 

            data = response.json()
            logger.debug(f"Fetched {len(data.get('items', []))} comments for video_id: {video_id}")

            for item in data.get('items', []):
                top_comment = {
                    'comment_id': item['snippet']['topLevelComment']['id'],
                    'channel_id': item['snippet']['channelId'],
                    'video_id': item['snippet']['videoId'],
                    'canReply': item['snippet']['canReply'],
                    'totalReplyCount': item['snippet']['totalReplyCount'],
                    'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'authorDisplayName': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'authorProfileImageUrl': item['snippet']['topLevelComment']['snippet']['authorProfileImageUrl'],
                    'authorChannelUrl': item['snippet']['topLevelComment']['snippet']['authorChannelUrl'],
                    'canRate': item['snippet']['topLevelComment']['snippet']['canRate'],
                    'viewerRating': item['snippet']['topLevelComment']['snippet']['viewerRating'],
                    'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],
                    'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'updatedAt': item['snippet']['topLevelComment']['snippet']['updatedAt'],
                    'fetched_time': datetime.now()
                }
                comments.append(top_comment)

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                logger.info(f"Completed fetching comments for video_id: {video_id}")
                break
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise AirflowFailException(f"HTTP error while fetching comments for video_id {video_id}: {http_err}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise  AirflowFailException(f"Network error while fetching comments for video_id {video_id}: {e}")    

    logger.debug(f"Total comments fetched for video_id: {video_id}: {len(comments)}")     
    return comments

def get_replies(parent_id):
    url = "https://www.googleapis.com/youtube/v3/comments"
    params = {
        'part': 'snippet',
        'parentId': parent_id,
        'maxResults': 100,
        'key': API_KEY
    }

    replies = []
    next_page_token = None
    

    while True:
        if next_page_token:
            params['pageToken'] = next_page_token
        try:
            logger.debug(f"Starting to fetch replies for a comment id: {parent_id}")
            response = requests.get(url, params=params)
            response.raise_for_status() 
            data = response.json()

            for item in data.get('items', []):
                reply = {
                    'reply_id': item['id'],
                    'authorDisplayName': item['snippet']['authorDisplayName'],
                    'authorProfileImageUrl': item['snippet']['authorProfileImageUrl'],
                    'authorChannelUrl': item['snippet']['authorChannelUrl'],
                    'channel_id': item['snippet']['channelId'],
                    'text': item['snippet']['textDisplay'],
                    'parent_id': item['snippet']['parentId'],
                    'canRate': item['snippet']['canRate'],
                    'viewerRating': item['snippet']['viewerRating'],
                    'likeCount': item['snippet']['likeCount'],
                    'publishedAt': item['snippet']['publishedAt'],
                    'updatedAt': item['snippet']['updatedAt'],
                    'fetched_time': datetime.now()
                }                
                replies.append(reply)
            
                # nested_replies = get_replies(reply['reply_id'])
                # replies.extend(nested_replies)
            
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break
            

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise AirflowFailException(f"HTTP error while fetching replies for comment with comment_id {parent_id}: {http_err}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise  AirflowFailException(f"Network error while fetching replies for comment with comment_id: {parent_id}: {e}")    

    logger.debug(f"Replies were fetched for comment: {parent_id}")             
    return replies




