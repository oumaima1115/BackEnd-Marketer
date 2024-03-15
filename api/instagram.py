import re
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging
import json 
from models import ScrapConfig

def get_response(urlinfo, headers, params):
    try:
        response = requests.get(urlinfo, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
        return json_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error in making request: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error in decoding JSON response: {e}")
        raise

def instagram_scrap(instagram_rapid_api_url, 
                    mention: str, 
                    items: int, 
                    instagram_source: str, 
                    hashtag_pattern: str, 
                    mention_pattern: str, 
                    y_m_dTHM_format: str, 
                    post_type: str, 
                    user_id: str, 
                    scrap_config) -> list:

    MAX_RETRIES = 2
    retry_count = 0

    while retry_count <= MAX_RETRIES:
        try:
            instagram_captions_list = []
            next_cursor = None
            has_next_page = True
            initial_mention = mention

            while has_next_page and len(instagram_captions_list) < items:
                tokens = mention.split()
                if len(tokens) > 3:
                    mention = mention
                else:
                    mention = mention

                print("Current mention:", mention)
                querystring = {
                    "keywords": mention,
                    "count": str(min(items, items - len(instagram_captions_list))),
                    "publish_time": "0",
                    "sort_type": "0"
                }
                if next_cursor:
                    querystring["next"] = next_cursor

                headers = {
                    "X-RapidAPI-Key": "c9788783c0mshdb6ccd3cf29d9acp1074ddjsn64524cd7cb97",
                    "X-RapidAPI-Host": "instagram-profile1.p.rapidapi.com"
                }
                url_with_mention = instagram_rapid_api_url + mention

                response = get_response(url_with_mention, headers=headers, params=querystring)
                data = response.get('data')
                if not data:
                    logging.log.warning("No data found in response")
                    break

                media_entries = data.get('media')
                media_count_for_request = 0

                next_cursor = data.get("next")
                has_next_page = data.get("has_next_page", False)

                if has_next_page and not next_cursor:
                    logging.log.warning("API indicates more pages, but no cursor provided for the next page.")
                    break

                for media_entry in media_entries:
                    media_count_for_request += 1
                    try:
                        caption_text = media_entry.get("caption", "")
                        scrap_config.set_mention_pattern(caption_text)
                        mention_pattern = scrap_config.mention_pattern
                        mentions = scrap_config.extract_mentions(caption_text, mention_pattern)
                        hashtags = re.findall(hashtag_pattern, caption_text)

                        video_date = datetime.fromtimestamp(media_entry.get("created_at", 0)).strftime(y_m_dTHM_format)
                        author_id = media_entry.get("owner", {}).get("id", "")
                        
                        instagram_post_source_link = media_entry.get("link_to_post")

                        instagram_document = {
                            "user_id": user_id,
                            "text": caption_text,
                            "type": post_type,
                            "source": instagram_source,
                            "source_link": instagram_post_source_link,
                            "date": video_date,
                            "mention": initial_mention,
                            "nbr_mentions": len(mentions),
                            "nbr_hashtags": len(hashtags),
                            "mentions_texts": mentions,
                            "hashtags_texts": hashtags,
                            "author": author_id
                        }

                        instagram_captions_list.append(instagram_document)

                    except KeyError:
                        continue
                print(f"Processed {media_count_for_request} media entries for this request.")

            if not instagram_captions_list:
                logging.log.error(f"No Instagram posts found for mention {mention}")
            else:
                print(f"Number of records in instagram_captions_list: {len(instagram_captions_list)}")
            break

        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count > MAX_RETRIES:
                logging.log.error("Max retry attempts reached. Exiting.")
                return []
            logging.log.warning(f"Encountered an error. Retrying... ({retry_count}/{MAX_RETRIES})")
            time.sleep(2)
        except json.JSONDecodeError as e:
            logging.log.error(f"Error in decoding JSON response: {e}")
            raise

    urlprofile = "https://instagram-scraper-api2.p.rapidapi.com/v1.2/posts"
    headers1 = {
        "X-RapidAPI-Key": "9edcbd1b9fmsha32eed8d12dc817p103628jsn9b4da960ede0",
        "X-RapidAPI-Host": "instagram-scraper-api2.p.rapidapi.com"
    }
    unique_authors = {media_entry['author'] for media_entry in instagram_captions_list if 'author' in media_entry}
    author_data = {}

    with ThreadPoolExecutor() as executor:
        futures = []
        for author in unique_authors:
            querystring = {"username_or_id_or_url": author}
            future = executor.submit(get_response, urlprofile, headers=headers1, params=querystring)
            futures.append((future, author))
            time.sleep(0.5)

        for future, author in futures:
            try:
                user_data = future.result()
                if user_data and 'data' in user_data and 'items' in user_data['data']:
                    user_videos = user_data['data']['items']
                    likes = [item.get("like_count") for item in user_videos if item.get("like_count") is not None]
                    list_titles = [caption.get("text") for item in user_videos if (caption := item.get("caption")) is not None]
                    user_name = [item.get("user", {}).get("full_name") for item in user_videos if item.get("user") is not None]
                    author_data[author] = {"like": likes, "list_titles": list_titles, "user_name": user_name}
            except Exception as e:
                print(f"Failed to process data for author {author}: {e}")

    for media_entry in instagram_captions_list:
        username = media_entry.get("author")
        if username in author_data:
            media_entry.update({
                "like": author_data[username]["like"],
                "user_name": author_data[username]["user_name"][0] if author_data[username]["user_name"] else "Unknown User",
                "description": author_data[username]["list_titles"],
            })

    print("Number of Instagram posts:", len(instagram_captions_list))
    return instagram_captions_list

# Test
instagram_rapid_api_url = "https://instagram-profile1.p.rapidapi.com/hashtags/"
mention = "nike"
items = 3
instagram_source = "instagram_app"
hashtag_pattern = "#\w+"
mention_pattern = "@\w+"
y_m_dTHM_format = "%Y-%m-%dT%H:%M"
post_type = "video"
user_id = "12345"
scrap_config =ScrapConfig()

print(instagram_scrap(instagram_rapid_api_url, mention, items, instagram_source, hashtag_pattern, mention_pattern, y_m_dTHM_format, post_type, user_id, scrap_config))
