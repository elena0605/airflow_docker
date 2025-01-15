import requests
import configparser
import logging


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
API_KEY = 'AIzaSyCu9avifWhxwAiGCrOhhkcsMIfXdVIVdX0'

def get_channels_statistics(channel_id):
    logger.debug(f"Fetching statistics for channel ID: {channel_id}")
    url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics,brandingSettings&id={channel_id}&key={API_KEY}'

    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        logger.debug(f"Received data for channel ID: {channel_id} - {data}")
    except requests.exceptions.RequestException as e:
          logger.error(f"Request failed for channel ID: {channel_id} - {e}")
          return {
            'channel_id': channel_id,
            'title': 'Unknown',
            'view_count': '0',
            'subscriber_count': '0',
            'video_count': '0',
            'hidden_subscriber_count': False
          }
       
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
        logger.warning(f"No items found for channel ID: {channel_id}")
        return{
            'channel_id': channel_id,
            'title': 'Unknown',
            'view_count': '0',
            'subscriber_count': '0',
            'video_count': '0',
            'hidden_subscriber_count': False
        }


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
        data = response.json()
        logger.debug(f"Fetched {len(data.get('items', []))} videos from page.")

        for item in data.get('items', []):
            video_title = item['snippet']['title']
            video_id = item['id']['videoId']
            published_at = item['snippet']['publishedAt']
            video_description = item['snippet']['description']
            channelTitle = item['snippet']['channelTitle']

            videos.append({
                          'video_title': video_title, 
                          'video_id': video_id, 
                          'published_at': published_at, 
                          'channel_id': channel_id, 
                          'video_description': video_description, 
                          'channel_title' : channelTitle
                           })

        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            logger.info("No more pages to fetch.")
            break

     except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching videos: {e}")
        break

    logger.info(f"Total videos fetched: {len(videos)}")
    return videos




