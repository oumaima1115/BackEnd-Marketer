from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent
import requests
import re
import time
def fetch_author_data(author_id, session, tweet_url, headers):
    querystring = {"id": author_id, "count": "2"}
    response = session.get(tweet_url, headers=headers, params=querystring).json()
    likes = []
    instructions_list = response['data']['user']['result']['timeline_v2']['timeline']['instructions']
    entries_data = next((i['entries'] for i in instructions_list if i.get('type') == 'TimelineAddEntries'), [])
    for entry in entries_data:
        try:
            # print("entry", entry)
            tweet_data = entry['content']['itemContent']['tweet_results']['result']
            like_count = tweet_data.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("media_count", 0)
            likes.append(like_count)
        except KeyError:
            print("Error: Required keys not found in tweet data.")
            continue
    average_likes = sum(likes) / len(likes) if likes else 0
    return {"Author_id": author_id, "Like": average_likes, "Entries": entries_data}

def fetch_author_info(tweets_list, headers):
    tweet_url = "https://twitter135.p.rapidapi.com/v2/UserTweets/"
    author_data = []
    with requests.Session() as session:
        session.headers.update(headers)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Use executor to submit the task
            future = executor.submit(fetch_author_data, tweets_list['author'], session, tweet_url, headers)
            result = future.result()
            if result:
                author_data.append(result)
            time.sleep(1)

    # Sorting and selecting top authors' tweets
    author_data_sorted = sorted(author_data, key=lambda x: x["Like"], reverse=True)[:2]
    # print("author data", author_data_sorted)
    list_final_tweets = []
    for author in author_data_sorted:
        entries = author["Entries"]
        author_tweets = []
        try:
            for entry in entries:
                tweet_data = entry['content']['itemContent']['tweet_results']['result']
                like = tweet_data.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("media_count", 0)
                name = tweet_data.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("name", "")
                title = tweet_data.get("legacy", {}).get("full_text", "")
                author_tweets.append({"Source": "twitter", "Like": like, "Author": name, "Text": title})
        except KeyError:
            print("Error: Response structure does not match expectations.")
            continue
        if author_tweets:
            list_final_tweets.append(sorted(author_tweets, key=lambda x: x["Like"], reverse=True)[:2])

    return list_final_tweets

def twitter_scrap(
                  hashtag_pattern: str,
                  mention_pattern: str,
                  twitter_original_date_format: str,
                  y_m_dTHM_format: str,
                  post_type: str,
                  twitter_source: str,
                  mention: str,
                  user_id: str) -> list:

    tweets_list = []
    headers = {
        "X-RapidAPI-Key": "9edcbd1b9fmsha32eed8d12dc817p103628jsn9b4da960ede0",
        "X-RapidAPI-Host": "twitter135.p.rapidapi.com"
    }
    next_cursor = None
    previous_cursor = None
    tweets_collected = 0
    max_tweets = 2
    while tweets_collected < max_tweets:
        querystring = {
            "q": mention
        }
        if next_cursor:
            querystring["cursor"] = next_cursor

        response = requests.request("GET", twitter_rapid_api_url, headers=headers, params=querystring).json()
        instructions_list = response['data']['search_by_raw_query']['search_timeline']['timeline']['instructions']

        # Extract entries_data from the first TimelineAddEntries instruction
        entries_data = next((i['entries'] for i in instructions_list if i.get('type') == 'TimelineAddEntries'), [])

        for entry in entries_data:
            try:
                tweet_data = entry['content']['itemContent']['tweet_results']['result']['legacy']
                tweet_id = tweet_data.get("id_str")
                tweet_full_text = tweet_data.get("full_text")
                hashtags = re.findall(hashtag_pattern, tweet_full_text)

                tweet_source_link = f"https://twitter.com/user/status/{tweet_id}"
                twitter_document = {
                    "user_id": str(user_id),
                    "text": tweet_full_text,
                    "type": post_type,
                    "author": tweet_data.get("user_id_str"),
                    "source": twitter_source,
                    "source_link": tweet_source_link,
                    "mention": mention,
                    "nbr_hashtags": len(hashtags),
                    "hashtags_texts": hashtags
                }

                tweets_list.append(twitter_document)
                tweets_collected += 1                
            except KeyError:
                continue

        # Accessing the cursor in a streamlined manner
        next_cursor = next((e['content'].get('value') for e in entries_data if e['content'].get('entryType') == 'TimelineTimelineCursor'), None)

        if not next_cursor or next_cursor == previous_cursor:
            break

        previous_cursor = next_cursor

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_author_info, tweet, headers) for tweet in tweets_list]
        concurrent.futures.wait(futures)
        tweets_list = [future.result() for future in futures]
    print("tweet_list", tweets_list)
    return tweets_list

    # print(tweets_list)
    return tweets_list

twitter_rapid_api_url = "https://twitter135.p.rapidapi.com/Search/"
mention = "nike"
twitter_source = "twitter_app"
hashtag_pattern = "#\w+"
mention_pattern = "@\w+"
y_m_dTHM_format = "%Y-%m-%dT%H:%M"
twitter_original_date_format = "%a %b %d %H:%M:%S %z %Y"
post_type = "video"
user_id = "12345"

twitter_scrap(hashtag_pattern,
                  mention_pattern,
                  twitter_original_date_format,
                  y_m_dTHM_format,
                  post_type,
                  twitter_source,
                  mention,
                  user_id)