from datetime import datetime
import requests
import pandas as pd
import os
import csv
import json
import logging
import time
import configparser
import pymongo
from bson import ObjectId

# MongoDB client and database setup
#client = pymongo.MongoClient("mongodb://airflow:tiktok@localhost:27017/") use this client if app is running outside the container
client = pymongo.MongoClient("mongodb://airflow:tiktok@mongodb:27017/")
db = client["tiktok_db"]  # Database name
#collection = db["user_info"]  # Collection name

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
config = configparser.ConfigParser()
config.read('/opt/airflow/dags/config.ini')

def load_token_from_config():
    """
    Load token information from the config.ini file.
    """
    if "TIKTOK" in config:
        token = config["TIKTOK"].get("TOKEN", None)
        expires_at_str = config["TIKTOK"].get("EXPIRES_AT", None)

        try:
            expires_at = float(expires_at_str) if expires_at_str else None
        except (ValueError, TypeError):
            logger.info(f"load_token_from_config: Invalid EXPIRES_AT value in config.ini: {expires_at_str}. Resetting to None.")
            expires_at = None

        return {
            "token": token,
            "expires_at": expires_at
        }
    return {"token": None, "expires_at": None}


def save_token_to_config(token_info):
    """
    Save token information to the config.ini file.
    """
    if "TIKTOK" not in config:
        config["TIKTOK"] = {}

    # Save the token
    config["TIKTOK"]["TOKEN"] = token_info["token"]

    # Save the expiration timestamp as a string
    expires_at = token_info.get("expires_at")
    config["TIKTOK"]["EXPIRES_AT"] = str(expires_at) if expires_at else ""

    # Write the updated configuration back to the file
    with open("config.ini", "w") as configfile:
        config.write(configfile)


def get_new_token(client_key, client_secret):
    """
    Fetch a new token using TikTok API.
    """
    logger.info("Fetching new token...")
    url = "https://open.tiktokapis.com/v2/oauth/token/"
    body = {
        "client_key": client_key,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        logger.info(f"Requesting TikTok API with URL: {url}, Headers: {headers}, Body: {body}")
        response = requests.post(url, headers=headers, data=body)
        response.raise_for_status()
        resp = response.json()

        # Calculate the expiration time as a UNIX timestamp
        expires_at = time.time() + resp["expires_in"]

        token_info = {
            "token": "Bearer " + resp["access_token"],
            "expires_at": expires_at
        }

        # Save the token and expiration time to the config
        save_token_to_config(token_info)

        logger.info(f"New token obtained: {token_info['token']}, expires at {datetime.fromtimestamp(expires_at)}")
        return token_info

    except Exception as e:
        logger.error(f"Failed to fetch token: {e}", exc_info=True)
        raise

def is_token_expired(token_info):
    """
    Check if the current token is expired.
    """
    logger.info(f"Checking token expiry: {token_info}")
    if token_info["token"] is None or token_info["expires_at"] is None:
        return True
    return time.time() >= token_info["expires_at"]


def tiktok_get_user_info(username: str, output_dir:str, **context):
    collection = db["user_info"]
    if context is None:
        context = {} 
    # Load the token
    token_info = load_token_from_config()

    if is_token_expired(token_info):
         client_key = config["TIKTOK"]["CLIENT_KEY"]
         client_secret = config["TIKTOK"]["CLIENT_SECRET"]
         token_info = get_new_token(client_key, client_secret)



    logger.info(f"Now in function tiktok_get_user_info, getting {username}")
    url = 'https://open.tiktokapis.com/v2/research/user/info/'
    params = {"fields": "display_name, bio_description, is_verified, follower_count, following_count, likes_count, video_count"}
    body = {"username": username}
    headers = {"Authorization":   token_info["token"], "Content-Type" : "application/json"}
    
    try:
        logger.info(f"Requesting TikTok API with URL: {url}, Headers: {headers}, Body: {body}")
        # request
        response = requests.request("POST", url, headers = headers, params = params ,json = body,  timeout=10)
        logger.info("Call is done...")

        # If the request was successful, process the response
        response.raise_for_status()  # This will raise an exception for 4xx/5xx errors

        # Access the response data
        resp = response.json()
        logger.info(f"Now in function tiktok_get_user_info, getting resp {resp}")

        # add current timestamp to the filename
        # file_path = store_data(resp["data"], f"user_info_{username}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json", output_dir)

         # Insert data into MongoDB
        store_data_in_mongodb(resp["data"], username, collection)

        if 'ti' in context:
            resp["data"]["_id"] = str(resp["data"].get("_id", ObjectId()))
            context['ti'].xcom_push(key=f'{username}_info_path', value=resp["data"])

        df = pd.DataFrame([resp["data"]])  # Wrap the dictionary in a list to create a single-row DataFrame

        return df

    except requests.exceptions.HTTPError as http_err:
        logger.info("TIKTOK request requests.exceptions.HTTPError")
        # Print detailed information about the error
        logger.info(f"HTTP error occurred: {http_err}", exc_info=True)
        logger.info(f"Status Code: {response.status_code}", exc_info=True)
        logger.info(f"Response Content: {response.text}", exc_info=True)
        logger.info(f"Response Headers: {response.headers}", exc_info=True)
        
    except requests.exceptions.RequestException as err:
        logger.info("TIKTOK request requests.exceptions.RequestException", exc_info=True)
        logger.info(f"Other error occurred: {err}", exc_info=True)

    except Exception as e:
        logger.info("TIKTOK request All Other Exception")
        logger.info(f"An unexpected error occurred: {e}", exc_info=True)

def tiktok_get_user_video_info(username:str, output_dir:str, **context):
    collection = db["user_video"]
    if context is None:
        context = {} 
    # Load the token
    token_info = load_token_from_config()

    if is_token_expired(token_info):
         client_key = config["TIKTOK"]["CLIENT_KEY"]
         client_secret = config["TIKTOK"]["CLIENT_SECRET"]
         token_info = get_new_token(client_key, client_secret)
    
    
    logger.info(f"Now in function tiktok_get_user_video_info, getting {username}")
    url = 'https://open.tiktokapis.com/v2/research/video/query/'
    headers = {"Authorization" : token_info["token"], "Content-Type" : "application/json"}
    params = {"fields": "id, video_description, create_time, region_code, share_count, view_count, like_count, comment_count, music_id, hashtag_names, username, effect_ids, playlist_id,voice_to_text, is_stem_verified, video_duration, hashtag_info_list, video_mention_list, video_label"}
    body = {"query": {"and" : [{"operation": "IN","field_name": "username", "field_values": [username]}]},"max_count": 100, "start_date": "20241123", "end_date": "20241219"}

    try:
        logger.info(f"Requesting TikTok API with URL: {url}, Headers: {headers}, Body: {body}")
        # request
        response = requests.request("POST", url, headers = headers, params = params ,json = body,  timeout=10)
        logger.info("Call is done...")

        # If the request was successful, process the response
        response.raise_for_status()  # This will raise an exception for 4xx/5xx errors

        # Access the response data
        resp = response.json()
        logger.info(f"Now in function tiktok_get_user_info, getting resp {resp}")

        # add current timestamp to the filename
        # file_path = store_data(resp["data"], f"user_info_{username}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json", output_dir)

         # Insert data into MongoDB
        store_data_in_mongodb(resp["data"], username,collection)

        if 'ti' in context:
            resp["data"]["_id"] = str(resp["data"].get("_id", ObjectId()))
            context['ti'].xcom_push(key=f'{username}_info_path', value=resp["data"])

        df = pd.DataFrame([resp["data"]])  # Wrap the dictionary in a list to create a single-row DataFrame

        return df

    except requests.exceptions.HTTPError as http_err:
        logger.info("TIKTOK request requests.exceptions.HTTPError")
        # Print detailed information about the error
        logger.info(f"HTTP error occurred: {http_err}", exc_info=True)
        logger.info(f"Status Code: {response.status_code}", exc_info=True)
        logger.info(f"Response Content: {response.text}", exc_info=True)
        logger.info(f"Response Headers: {response.headers}", exc_info=True)
        
    except requests.exceptions.RequestException as err:
        logger.info("TIKTOK request requests.exceptions.RequestException", exc_info=True)
        logger.info(f"Other error occurred: {err}", exc_info=True)

    except Exception as e:
        logger.info("TIKTOK request All Other Exception")
        logger.info(f"An unexpected error occurred: {e}", exc_info=True)

# def store_data(data, file_name, output_dir):
#     if not os.path.exists(output_dir):
#         logger.info(f'Creting directory: {output_dir}')
#         os.makedirs(output_dir)

#     file_path = os.path.join(output_dir, file_name)
#     with open(file_path, "w") as f:
#         json.dump(data, f)
#     return file_path

def store_data_in_mongodb(data, username, collection):
    """
    Insert TikTok user info data into MongoDB collection.
    """
    # Add username and timestamp to the data
    data["username"] = username
    data["timestamp"] = datetime.now()

    try:
        collection.insert_one(data)  # Insert the data into the MongoDB collection
        logger.info(f"Data for {username} inserted into MongoDB successfully.")
    except Exception as e:
        logger.error(f"Error inserting data into MongoDB: {e}", exc_info=True)

def read_usernames_from_csv(file_path):
    """Reads usernames from a local CSV file."""
    usernames = []
    with open(file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            usernames.append(row["username"])
    return usernames