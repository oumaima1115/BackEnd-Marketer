from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent
import re
from datetime import datetime
from django.http import JsonResponse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import uuid
from .models import ScrapConfig
import time
import praw
import requests
from bs4 import BeautifulSoup
import json
import csv
import os
import logging
# import log

#
#
#
# Youtube
#
#
#
#

def get_video_details_in_bulk(youtube,
                                youtube_config,
                                video_id_batch,
                                video_ids,
                                document,
                                snippet,
                                hashtag_pattern,
                                mention_pattern,
                                y_m_dTHMSZ_format,
                                y_m_dTHM_format,
                                post_type,
                                youtube_source,
                                user_id,
                                mention,
                                scrap_config):

    video_detail_list = []
    joined_video_ids = ",".join(video_ids)
    video_response = youtube.videos().list(id=joined_video_ids, part=snippet).execute()

    if not video_response.get("items"):
        return []

    if isinstance(video_response, dict):
        for video_item in video_response.get("items", []):
            video_id = video_item.get('id')
            video_info = video_item.get(snippet, {})
            document_text = video_info.get("description")
            source_link = f"https://www.youtube.com/watch?v={video_id}"
            scrap_config.set_mention_pattern(document_text)
            mention_pattern = scrap_config.mention_pattern
            mentions = scrap_config.extract_mentions(document_text, mention_pattern)
            hashtags = re.findall(hashtag_pattern, document_text)
            if mentions or hashtags:
                post_uuid = str(uuid.uuid4())
                document_date = datetime.strptime(video_info.get("publishedAt"), y_m_dTHMSZ_format).strftime(y_m_dTHM_format)
                youtube_document = {
                    "Source": youtube_source,
                    "Author": video_info.get("channelTitle"),
                    "User_id": user_id,
                    "Text": document_text,
                    "Type": post_type,
                    "Source_link": source_link,
                    "Date": document_date,
                    "Mention": mention,
                    "Nbr_mentions": len(mentions),
                    "Nbr_hashtags": len(hashtags),
                    "Mentions_texts": mentions,
                    "Hashtags_texts": hashtags,
                    "Post_uuid": post_uuid
                }

                channel_id = video_info.get("channelId")
                if channel_id in document:
                    channel_details = document[channel_id]
                    youtube_document.update({
                        'Description': channel_details.get('description', ''),
                        'Like': channel_details.get('viewCount', '')
                    })
                video_detail_list.append(youtube_document)

    sorted_videos = sorted(video_detail_list, key=lambda x: int(x["Like"]), reverse=True)[:4]
    
    return sorted_videos


def youtube_scrap(request):

    youtube_config={
        "service_version":"v3", 
        "google_key":"AIzaSyBZdHYAp2WWgm413pasORoGG2nJ179DYVg", 
        "snippet_part":"snippet",
        "type":"any",
        "max_results":5
    }
    start_date = datetime(2024, 2, 1).isoformat() + 'Z'
    end_date = datetime(2024, 2, 28).isoformat() + 'Z'
    hashtag_pattern = "#\w+"
    mention_pattern = "@\w+"
    y_m_dTHM_format = "%Y-%m-%dT%H:%M"
    y_m_dTHMSZ_format = "%Y-%m-%dT%H:%M:%SZ"
    post_type="video"
    youtube_source="youtube"
    mention= "surfing"
    user_id="123456"
    scrap_config = ScrapConfig()
    
    def get_video_ids_based_on_date(youtube, start_date, end_date):
        search_response = youtube.search().list(q=mention,
                                                type=youtube_config["type"],
                                                maxResults=youtube_config["max_results"],
                                                publishedAfter=start_date,
                                                publishedBefore=end_date,
                                                part=youtube_config["snippet_part"]).execute()
        
        #print("search response = ", search_response)
        video_ids = []
        channel_ids = []

        for item in search_response.get('items', []):
            if 'videoId' in item['id']: 
                video_ids.append(item['id']['videoId'])
                channel_ids.append(item['snippet']['channelId'])

        return video_ids, channel_ids

    snippet = youtube_config["snippet_part"]
    youtube = build(youtube_source, youtube_config["service_version"], developerKey=youtube_config["google_key"])
    video_ids, channel_ids = get_video_ids_based_on_date(youtube, start_date, end_date)
    video_id_batches = [video_ids[i:i + 5] for i in range(0, len(video_ids), 5)]

    #print("channel_ids = ", channel_ids)
    request = youtube.channels().list(
        maxResults=3,
        part="snippet,contentDetails,statistics",
        id=",".join(channel_ids)
    )
    response = request.execute()
    document = {}
    for item in response.get('items', []):
        channel_id = item['id']
        description = item['snippet']['description']
        view_count = item['statistics']['viewCount']
        document[channel_id] = {'description': description, 'viewCount': view_count}
    print("Document = ", document)

    def worker_function(video_id_batch):
        local_youtube = build(youtube_source,
                            youtube_config["service_version"], 
                            developerKey=youtube_config["google_key"])
        return get_video_details_in_bulk(local_youtube,
                                        youtube_config,
                                        video_id_batch,
                                        video_ids,
                                        document,
                                        snippet,
                                        hashtag_pattern,
                                        mention_pattern,
                                        y_m_dTHMSZ_format,
                                        y_m_dTHM_format,
                                        post_type,
                                        youtube_source,
                                        user_id,
                                        mention,
                                        scrap_config)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_batch = {executor.submit(worker_function, batch): batch for batch in video_id_batches}
        all_results = [future.result() for future in as_completed(future_to_batch)]

    youtube_posts_comments_list = [item for sublist in all_results for item in sublist]
    
    for i, item in enumerate(youtube_posts_comments_list):
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, bytes):
                    youtube_posts_comments_list[i][key] = value.decode('utf-8')
        else:
            youtube_posts_comments_list.pop(i)

    folder_name = "csv_data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    csv_file_path = os.path.join(folder_name, "youtube_posts.csv")

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
        fieldnames = youtube_posts_comments_list[0].keys() if youtube_posts_comments_list else []
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(youtube_posts_comments_list)

    return JsonResponse(youtube_posts_comments_list, safe=False)

#
#
#
# Redite
#
#
#
#

def process_comment(comment,
                    mention,
                    y_m_dTHM_format,
                    user_id,
                    comment_type,
                    reddit_source,
                    mention_pattern,
                    hashtag_pattern,
                    scrap_config):

    document_text = comment.body
    scrap_config.set_mention_pattern(document_text)
    mention_pattern = scrap_config.mention_pattern
    mentions = scrap_config.extract_mentions(document_text, mention_pattern)
    hashtags = re.findall(hashtag_pattern, document_text)
    document_author = comment.author
    comment_id = comment.id 
    post_id = comment.submission.id 
    reddit_comment_source_link = f"https://www.reddit.com/r/{comment.subreddit}/comments/{post_id}/-/{comment_id}"
    
    if (mentions or hashtags) and document_author is not None:
        comment_date = datetime.utcfromtimestamp(comment.created_utc).strftime(y_m_dTHM_format)

        reddit_document = {
            "user_id": user_id,
            "text": document_text,
            "type": comment_type,
            "author": document_author.name,
            "source": reddit_source,
            "source_link": reddit_comment_source_link,
            "date": comment_date,
            "mention": mention,
            "nbr_mentions": len(mentions),
            "nbr_hashtags": len(hashtags),
            "mentions_texts": mentions,
            "hashtags_texts": hashtags
        }
        return reddit_document
    return None
    
def process_profile_reddit(reddit_list,headers):
    reddit_author_data = []

    for reddit in reddit_list:
        user_id = reddit["user_id"]
        text = reddit["text"]
        type = reddit["type"]
        author_id = reddit["author"]
        source = reddit["source"]
        source_link = reddit["source_link"]
        date = reddit["date"]
        mention = reddit["mention"]
        nbr_mentions = reddit["nbr_mentions"]
        nbr_hashtags = reddit["nbr_hashtags"]
        mentions_texts = reddit["mentions_texts"]
        hashtags_texts = reddit["hashtags_texts"]

        profile_url = f"https://api.reddit.com/user/{author_id}/submitted"
        response = requests.get(profile_url, headers=headers)
        
        # if response.status_code == 200:
        try:
            data = response.json()
            
            likes = []
            reddit_post = data['data']['children']
            for post in reddit_post:
                reddit_data = post['data']
                likes.append(reddit_data.get('upvote_ratio', 0) )

            reddit_author_data.append({"Data":reddit_post, 
                                "Like":sum(likes)/len(likes) if likes else 0, 
                                "Author_id":author_id, 
                                "User_id": user_id,
                                "Text":text,
                                "Type":type,
                                "Source":source,
                                "Source_link":source_link,
                                "Date":date,
                                "Mention":mention,
                                "Nbr_mentions":nbr_mentions,
                                "Nbr_hashtags":nbr_hashtags,
                                "Mentions_texts":mentions_texts,
                                "Hashtags_texts":hashtags_texts
                                })
        except KeyError:
            print("Error: Response structure does not match expectations.")

    list_best_reddit_authors_sorted = sorted(reddit_author_data, key=lambda x: x["Like"], reverse=True)[:2]
    list_final_reddit=[]

    for author_data in list_best_reddit_authors_sorted:
        author_id = author_data["Author_id"]
        data_reddits=author_data["Data"]
        user_id = author_data["User_id"]
        text = author_data["Text"]
        type = author_data["Type"]
        source = author_data["Source"]
        source_link = author_data["Source_link"]
        date = author_data["Date"]
        mention = author_data["Mention"]
        nbr_mentions = author_data["Nbr_mentions"]
        nbr_hashtags = author_data["Nbr_hashtags"]
        mentions_texts = author_data["Mentions_texts"]
        hashtags_texts = author_data["Hashtags_texts"]

        authors_reddit = []
        for data_reddit_posts in data_reddits:
            like = data_reddit_posts['data']['upvote_ratio']
            selftext = data_reddit_posts['data']['selftext']
            title_reddit = data_reddit_posts['data']['title']

            authors_reddit.append({"Source": source,
                                        "Like": like,
                                        "Author": author_id, 
                                        "Description": selftext if selftext else title_reddit,
                                        "User_id": user_id,
                                        "Text":text,
                                        "Type":type,
                                        "Source_link":source_link,
                                        "Date":date,
                                        "Mention":mention,
                                        "Nbr_mentions":nbr_mentions,
                                        "Nbr_hashtags":nbr_hashtags,
                                        "Mentions_texts":mentions_texts,
                                        "Hashtags_texts":hashtags_texts})
    list_final_reddit.append(sorted(authors_reddit, key=lambda x: x["Like"], reverse=True)[:2])
    print("authors_reddit = ",authors_reddit)
    return list_final_reddit

def reddit_scrap(request):
    REDDIT_HEADERS = {"User-Agent": "MyBot/0.0.1"}
    reddit = praw.Reddit(
                client_id="q7J0g1si5uXJjRJkoMhtjQ",
                client_secret="xb4ag_Wr9iIY7i912LhIt_-LZUbqAA",
                username="oumaima_ayachi",
                password="W8HpJ4E5mT#L$5*",
                user_agent=REDDIT_HEADERS["User-Agent"]
            )
    subreddit=reddit.subreddit("python")
    limit=5
    y_m_dTHMSZ_format = "%Y-%m-%dT%H:%M:%SZ"
    start_date = datetime(2024, 2, 1).isoformat() + 'Z'
    end_date = datetime(2024, 2, 28).isoformat() + 'Z'
    hashtag_pattern = "#\w+"
    mention_pattern = "@\w+"
    y_m_dTHM_format = "%Y-%m-%dT%H:%M"
    post_type="post"
    comment_type="text"
    reddit_source="reddit"
    mention="nike"
    user_id="12345"
    scrap_config= ScrapConfig()
    start_time = time.time()

    initial_start_date = start_date
    initial_end_date = end_date

    start_date = datetime.strptime(initial_start_date, y_m_dTHMSZ_format)
    start_timestamp = int(start_date.timestamp())

    end_date = datetime.strptime(initial_end_date, y_m_dTHMSZ_format)
    end_timestamp = int(end_date.timestamp())

    reddit_posts_comments_list = []
    posts = subreddit.search("python", limit=5)

    post_list = list(posts)
    # print(f"Number of posts found: {len(post_list)}")

    if post_list:
        for post in post_list:
            mentions = ["nike","girl"]
            hashtags = []
            reddit_post_source_link = f"https://www.reddit.com{post.permalink}"
            document_text = post.title + post.selftext
            hashtags = re.findall(hashtag_pattern, document_text)
            document_author = post.author
            if (mentions or hashtags) and document_author is not None:
                document_date = datetime.utcfromtimestamp(post.created_utc).strftime(y_m_dTHM_format)
                reddit_document = {
                    "user_id": user_id,
                    "text": document_text,
                    "type": post_type,
                    "author": document_author.name,
                    "source": reddit_source,
                    "source_link": reddit_post_source_link,
                    "date": document_date,
                    "mention": mention,
                    "nbr_mentions": len(mentions),
                    "nbr_hashtags": len(hashtags),
                    "mentions_texts": mentions,
                    "hashtags_texts": hashtags
                }
                reddit_posts_comments_list.append(reddit_document)
                comments = post.comments
                comments.replace_more(limit=limit)

                with ThreadPoolExecutor() as executor:
                    args_for_comments = [(comment, mention, y_m_dTHM_format, user_id, comment_type, reddit_source, mention_pattern, hashtag_pattern, scrap_config) for comment in comments.list()]
                    results = list(executor.map(lambda args: process_comment(*args), args_for_comments))
                reddit_posts_comments_list.extend(filter(None, results))

    list_reddit = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_profile_reddit, reddit_posts_comments_list,REDDIT_HEADERS)]
        for future in concurrent.futures.as_completed(futures):
            reddit_profile_data = future.result()
            if reddit_profile_data:
                list_reddit.extend(reddit_profile_data)
                
    flat_list_reddit = [item for sublist in list_reddit for item in sublist]
    print("length of flat_list_reddit = ", len(flat_list_reddit))

    folder_name = "csv_data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    csv_file = os.path.join(folder_name, "reddit_posts.csv")

    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "Source",
                "Like",
                "Author",
                "Description",
                "User_id",
                "Text",
                "Type",
                "Source_link",
                "Date",
                "Mention",
                "Nbr_mentions",
                "Nbr_hashtags",
                "Mentions_texts",
                "Hashtags_texts"
            ]
        )
        writer.writeheader()
        for post in flat_list_reddit:
            writer.writerow(post)

    print(f"CSV file saved successfully at: {csv_file}")

    end_time = time.time()
    duration = end_time - start_time
    print(f"the reddit function took {duration:2f} seconds to complete ")
    # print(f"Number of records in reddit_posts_comments_list: {len(reddit_posts_comments_list)}")
    return JsonResponse(flat_list_reddit, safe=False)


#
#
#
# Twitter
#
#
#
#

def fetch_author_info(tweets_list, headers):
    tweet_url = "https://twitter135.p.rapidapi.com/v2/UserTweets/"
    author_data = []

    for tweet in tweets_list:
        user_id = tweet["user_id"]
        text = tweet["text"]
        type = tweet["type"]
        author_id = tweet["author"]
        source = tweet["source"]
        source_link = tweet["source_link"]
        date = tweet["date"]
        mention = tweet["mention"]
        nbr_mentions = tweet["nbr_mentions"]
        nbr_hashtags = tweet["nbr_hashtags"]
        mentions_texts = tweet["mentions_texts"]
        hashtags_texts = tweet["hashtags_texts"]

        querystring = {"id": author_id, "count": "3"}
        response = requests.get(tweet_url, headers=headers, params=querystring).json()

        try:
            instructions_list = response['data']['user']['result']['timeline_v2']['timeline']['instructions']
            entries_data = next((i['entries'] for i in instructions_list if i.get('type') == 'TimelineAddEntries'), [])
            
            likes=[]
            for entry in entries_data:
                try:
                    tweet_data = entry['content']['itemContent']['tweet_results']['result']
                    likes.append(tweet_data.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("media_count", 0))
                except KeyError:
                    print("Error: Required keys not found in tweet data.")
            author_data.append({"Entries":entries_data, 
                                "Like":sum(likes)/len(likes), 
                                "Author_id":author_id, 
                                "User_id": user_id,
                                "Text":text,
                                "Type":type,
                                "Source":source,
                                "Source_link":source_link,
                                "Date":date,
                                "Mention":mention,
                                "Nbr_mentions":nbr_mentions,
                                "Nbr_hashtags":nbr_hashtags,
                                "Mentions_texts":mentions_texts,
                                "Hashtags_texts":hashtags_texts
                            })
        except KeyError:
            print("Error: Response structure does not match expectations.")

    list_best_authors_sorted = sorted(author_data, key=lambda x: x["Like"], reverse=True)[:2]

    list_final_tweets=[]
    for author_data in list_best_authors_sorted:
        author_id = author_data["Author_id"]
        entries=author_data["Entries"]
        user_id = author_data["User_id"]
        text = author_data["Text"]
        type = author_data["Type"]
        source = author_data["Source"]
        source_link = author_data["Source_link"]
        date = author_data["Date"]
        mention = author_data["Mention"]
        nbr_mentions = author_data["Nbr_mentions"]
        nbr_hashtags = author_data["Nbr_hashtags"]
        mentions_texts = author_data["Mentions_texts"]
        hashtags_texts = author_data["Hashtags_texts"]

        authors_tweets = []
        for entry in entries:
            try:
                tweet_data = entry['content']['itemContent']['tweet_results']['result']
                like = tweet_data.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("media_count", 0)
                name = tweet_data.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("name", "")
                description = tweet_data.get("legacy", {}).get("full_text", "")
                authors_tweets.append({"Source": "twitter",
                                        "Like": like,
                                        "Author": name, 
                                        "Description": description,
                                        "User_id": user_id,
                                        "Text":text,
                                        "Type":type,
                                        "Source_link":source_link,
                                        "Date":date,
                                        "Mention":mention,
                                        "Nbr_mentions":nbr_mentions,
                                        "Nbr_hashtags":nbr_hashtags,
                                        "Mentions_texts":mentions_texts,
                                        "Hashtags_texts":hashtags_texts})
            except KeyError:
                print("Error: Required keys not found in tweet data.")
        list_final_tweets.append(sorted(authors_tweets, key=lambda x: x["Like"], reverse=True)[:2])
    print("list_final_tweets = ",list_final_tweets)
    return list_final_tweets


def twitter_scrap(request):
    twitter_rapid_api_url = "https://twitter135.p.rapidapi.com/Search/"
    mention = "nike"
    twitter_source = "twitter_app"
    hashtag_pattern = "#\w+"
    mention_pattern = "@\w+"
    y_m_dTHM_format = "%Y-%m-%dT%H:%M"
    twitter_original_date_format = "%a %b %d %H:%M:%S %z %Y"
    post_type = "video"
    user_id = "12345"
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

        querystring = {"q":"nike","count":"3","safe_search":"true"}
        if next_cursor:
            querystring["cursor"] = next_cursor

        response = requests.get(twitter_rapid_api_url, headers=headers, params=querystring).json()
        # print("first response : ", response)

        try:
            instructions_list = response['data']['search_by_raw_query']['search_timeline']['timeline']['instructions']
            entries_data = next((i['entries'] for i in instructions_list if i.get('type') == 'TimelineAddEntries'), [])

            for entry in entries_data:
                try:
                    tweet_data = entry['content']['itemContent']['tweet_results']['result']['legacy']
                    tweet_id = tweet_data.get("id_str")
                    tweet_full_text = tweet_data.get("full_text")
                    mentions = re.findall(mention_pattern, tweet_full_text)
                    hashtags = re.findall(hashtag_pattern, tweet_full_text)
                    tweet_date = datetime.strptime(
                        tweet_data.get("created_at"),
                        twitter_original_date_format
                    ).strftime(y_m_dTHM_format)

                    tweet_source_link = f"https://twitter.com/user/status/{tweet_id}"
                    twitter_document = {
                        "user_id": str(user_id),
                        "text": tweet_full_text,
                        "type": post_type,
                        "author": tweet_data.get("user_id_str"),
                        "source": twitter_source,
                        "source_link": tweet_source_link,
                        "date": tweet_date,
                        "mention": mention,
                        "nbr_mentions": len(mentions),
                        "nbr_hashtags": len(hashtags),
                        "mentions_texts": mentions,
                        "hashtags_texts": hashtags
                    }

                    tweets_list.append(twitter_document)
                    tweets_collected += 1                
                except KeyError:
                    continue

            next_cursor = next((e['content'].get('value') for e in entries_data if e['content'].get('entryType') == 'TimelineTimelineCursor'), None)

            if not next_cursor or next_cursor == previous_cursor:
                break
 
            previous_cursor = next_cursor
        except KeyError:
            print("Error: 'data' key not found in response.")
            break
        
    print("Number of tweets collected =", len(tweets_list))
    list_twitter = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_author_info, tweets_list, headers)]
        for future in concurrent.futures.as_completed(futures):
            twitter_profile_data = future.result()
            if twitter_profile_data:
                list_twitter.extend(twitter_profile_data)

    flat_list_twitter = [item for sublist in list_twitter for item in sublist]
    print("length of flat_list_twitter = ", len(flat_list_twitter))

    folder_name = "csv_data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    csv_file = os.path.join(folder_name, "twitter_posts.csv")

    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "Source",
                "Like",
                "Author",
                "Description",
                "User_id",
                "Text",
                "Type",
                "Source_link",
                "Date",
                "Mention",
                "Nbr_mentions",
                "Nbr_hashtags",
                "Mentions_texts",
                "Hashtags_texts"
            ]
        )
        writer.writeheader()
        for post in flat_list_twitter:
            writer.writerow(post)

    print(f"CSV file saved successfully at: {csv_file}")

    return JsonResponse(flat_list_twitter, safe=False)

#
#
#
#
# Tiktok
#
#
#
#
#

def process_tiktok(tiktoks_list, headers):
    tiktok_url = "https://tiktok-video-feature-summary.p.rapidapi.com/user/posts"
    author_data=[]
    for tiktok in tiktoks_list:
        user_id = tiktok["user_id"]
        text = tiktok["text"]
        type = tiktok["type"]
        author_id = tiktok["author"]
        source = tiktok["source"]
        source_link = tiktok["source_link"]
        date = tiktok["date"]
        mention = tiktok["mention"]
        nbr_mentions = tiktok["nbr_mentions"]
        nbr_hashtags = tiktok["nbr_hashtags"]
        mentions_texts = tiktok["mentions_texts"]
        hashtags_texts = tiktok["hashtags_texts"]

        querystring = {"user_id": author_id, "count": 3}
        response = requests.get(tiktok_url, headers=headers, params=querystring)
        data_tiktoks = response.json().get('data', {}).get('videos', [])
        try:
            likes=[]
            for data_tiktok in data_tiktoks:
                print("data_tiktok: ",data_tiktok)
                likes.append(data_tiktok.get("digg_count", 0))
            author_data.append({"Data":data_tiktoks,
                                "Like":sum(likes)/len(likes)if likes else 0, 
                                "Author_id":author_id, 
                                "User_id": user_id,
                                "Text":text,
                                "Type":type,
                                "Source":source,
                                "Source_link":source_link,
                                "Date":date,
                                "Mention":mention,
                                "Nbr_mentions":nbr_mentions,
                                "Nbr_hashtags":nbr_hashtags,
                                "Mentions_texts":mentions_texts,
                                "Hashtags_texts":hashtags_texts
                            })
        except KeyError:
            print("Error: Response structure does not match expectations.")
    list_best_authors_sorted = sorted(author_data, key=lambda x: x["Like"], reverse=True)[:2]

    list_final_tiktok=[]
    for author_data in list_best_authors_sorted:
        author_id = author_data["Author_id"]
        datatiktok=author_data["Data"]
        user_id = author_data["User_id"]
        text = author_data["Text"]
        type = author_data["Type"]
        source = author_data["Source"]
        source_link = author_data["Source_link"]
        date = author_data["Date"]
        mention = author_data["Mention"]
        nbr_mentions = author_data["Nbr_mentions"]
        nbr_hashtags = author_data["Nbr_hashtags"]
        mentions_texts = author_data["Mentions_texts"]
        hashtags_texts = author_data["Hashtags_texts"]

        authors_tiktoks = []
        for data in datatiktok:
            try:
                like=data.get("digg_count", 0)
                description=data.get("title", "")
                authors_tiktoks.append({"Source": source,
                                        "Like": like,
                                        "Author": author_id, 
                                        "Description": description,
                                        "User_id": user_id,
                                        "Text":text,
                                        "Type":type,
                                        "Source_link":source_link,
                                        "Date":date,
                                        "Mention":mention,
                                        "Nbr_mentions":nbr_mentions,
                                        "Nbr_hashtags":nbr_hashtags,
                                        "Mentions_texts":mentions_texts,
                                        "Hashtags_texts":hashtags_texts
                                    })
            except KeyError:
                print("Error: Required keys not found in tweet data.")
        list_final_tiktok.append(sorted(authors_tiktoks, key=lambda x: x["Like"], reverse=True)[:2])
    print("list_final_tiktok = ",list_final_tiktok)

    return list_final_tiktok

def tiktok_scrap(request):
    mention = "nike"
    tiktok_videos_list = []
    cursor = 0
    hashtag_pattern = "#\w+"
    mention_pattern = "@\w+"
    has_more = True
    tiktok_rapid_api_url = 'https://tiktok-video-feature-summary.p.rapidapi.com/feed/search'
    tiktok_config_items = 5
    items = 5
    post_type = "video"

    while has_more and len(tiktok_videos_list) < items:
        print(f"Querying TikTok for mention '{mention}' with cursor at {cursor} for {min(tiktok_config_items, items - len(tiktok_videos_list))} items.")
        querystring = {
            "keywords": mention,
            "count": str(min(tiktok_config_items, items - len(tiktok_videos_list))),
            "cursor": str(cursor),
            "publish_time": "0",
            "sort_type": "0"
        }

        headers = {
            "X-RapidAPI-Key": "c9788783c0mshdb6ccd3cf29d9acp1074ddjsn64524cd7cb97",
            "X-RapidAPI-Host": "tiktok-video-feature-summary.p.rapidapi.com"
        }

        response = requests.get(tiktok_rapid_api_url, headers=headers, params=querystring)
        data = response.json()
        videos = data.get('data', {}).get('videos', [])
        cursor = data.get("cursor", 0)
        has_more = data.get("hasMore", False)

        for video in videos:
            try:
                video_id = video.get("id")
                author_username = video.get("author", {}).get("unique_id", "")
                video_title = video.get("title", "")
                tiktok_source = video.get("play", {})
                author_id = video.get("author", {}).get("id", "")
                
                mentions = re.findall(mention_pattern, video_title)
                hashtags = re.findall(hashtag_pattern, video_title)

                tiktok_source_link = f"https://www.tiktok.com/@{author_username}/video/{video_id}"
                print(f"Processing Video ID: {video_id}, Source Link: {tiktok_source_link}")
                tiktok_document = {
                    "user_id":12356,
                    "text": video_title,
                    "type": post_type,
                    "author": author_id,
                    "source": tiktok_source,
                    "source_link": tiktok_source_link,
                    "date": "03-08-2024",
                    "mention": mention,
                    "nbr_mentions": len(mentions),
                    "nbr_hashtags": len(hashtags),
                    "mentions_texts": mentions,
                    "hashtags_texts": hashtags
                }

                tiktok_videos_list.append(tiktok_document)
            except KeyError:
                continue
    
    list_tiktok=[]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_tiktok, tiktok_videos_list, headers)]
        for future in concurrent.futures.as_completed(futures):
            tiktok_profile_data = future.result()
            if tiktok_profile_data:
                list_tiktok.extend(tiktok_profile_data)
    
    flat_list_tiktok = [item for sublist in list_tiktok for item in sublist]
    print("length of flat_list_tiktok = ", len(flat_list_tiktok))

    folder_name = "csv_data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    csv_file = os.path.join(folder_name, "tiktok_posts.csv")

    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "Source",
                "Like",
                "Author",
                "Description",
                "User_id",
                "Text",
                "Type",
                "Source_link",
                "Date",
                "Mention",
                "Nbr_mentions",
                "Nbr_hashtags",
                "Mentions_texts",
                "Hashtags_texts"
            ]
        )
        writer.writeheader()
        for post in flat_list_tiktok:
            writer.writerow(post)

    print(f"CSV file saved successfully at: {csv_file}")
    
    return JsonResponse(flat_list_tiktok, safe=False)

#
#
#
#
# Instagram
#
#
#

def get_response(self, urlinfo, headers,params):
        response = requests.get(urlinfo, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()
        return json_data  

def instagram_scrap(request):
        
    instagram_rapid_api_url="https://instagram-profile1.p.rapidapi.com/hashtags/"
    headers = {
        "X-RapidAPI-Key":"c9788783c0mshdb6ccd3cf29d9acp1074ddjsn64524cd7cb97",
        "X-RapidAPI-Host":"instagram-profile1.p.rapidapi.com"
    }
    mention = "nike"
    items = 3
    instagram_source = "instagram_app"
    hashtag_pattern = "#\w+"
    mention_pattern = "@\w+"
    y_m_dTHM_format = "%Y-%m-%dT%H:%M"
    post_type = "video"
    user_id = "12345"
    scrap_config = {
        "mention_pattern": mention_pattern 
    }

    MAX_RETRIES = 2
    retry_count = 0

    while retry_count <= MAX_RETRIES:
        try:
            instagram_captions_list = []
            next_cursor = None
            has_next_page = True
            initial_mention = mention
            length=len(instagram_captions_list)
            while has_next_page and length < items:
                tokens = mention.split()
                if len(tokens) > 3:
                    mention = mention
                else:
                    mention = mention

                print("Current mention:", mention)
                querystring = {
                    "keywords": mention,
                    "count": 3,
                    "publish_time": "0",
                    "sort_type": "0"
                }
                if next_cursor:
                    querystring["next"] = next_cursor

                url_with_mention = instagram_rapid_api_url + mention

                response = requests.get(url_with_mention, headers=headers, params=querystring)
                print("response = ", response.json())
                data = response.json()
                media_entries = data.get('data').get('media')
                media_count_for_request = 0

                next_cursor = data.get("data").get("next")
                has_next_page = data.get("data").get("has_next_page", False)

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

        except TypeError as e:
            if "'NoneType' object is not iterable" in str(e):
                retry_count += 1
                if retry_count > MAX_RETRIES:
                    logging.log.error("Max retry attempts reached. Exiting.")
                    return []
                logging.log.warning("Encountered an error. Retrying... ({}/{})".format(retry_count, MAX_RETRIES))
                time.sleep(2) 
            else:
                raise
    urlprofile = "https://instagram-scraper-api2.p.rapidapi.com/v1.2/posts"
    unique_authors = {media_entry['author'] for media_entry in instagram_captions_list if 'author' in media_entry}
    author_data = {}

    headers1 = {
        "X-RapidAPI-Key": "9edcbd1b9fmsha32eed8d12dc817p103628jsn9b4da960ede0",
        "X-RapidAPI-Host": "instagram-scraper-api2.p.rapidapi.com"
    }
    with ThreadPoolExecutor() as executor:
        futures = []
        for author in unique_authors:
            querystring = {"username_or_id_or_url":author}
            future = executor.submit(get_response, urlprofile, headers=headers1,params=querystring )
            futures.append((future, author))
            time.sleep(0.5)
            
        for future, author in futures:
            try:
                user_data = future.result()
                if user_data and 'data' in user_data and 'items' in user_data['data']:
                    user_videos = user_data['data']['items']
                    likes = [item.get("like_count") for item in user_videos if item.get("like_count") is not None]
                    average_likes_count = int(sum(likes) / len(likes)) if likes else 0
                    list_titles = [caption.get("text") for item in user_videos if (caption := item.get("caption")) is not None]
                    user_name = [item.get("user", {}).get("full_name") for item in user_videos if item.get("user") is not None]
                    author_data[author] = {"likes": average_likes_count, "list_titles": list_titles, "user_name": user_name}
            except Exception as e:
                print(f"Failed to process data for author {author}: {e}")

    for media_entry in instagram_captions_list:
        username = media_entry.get("author")
        if username in author_data:
            media_entry.update({
                "likes": author_data[username]["likes"],
                "user_name": author_data[username]["user_name"][0] if author_data[username]["user_name"] else "Unknown User",
                "list_title": author_data[username]["list_titles"],
            })
            
    print("number instagram list", len(instagram_captions_list))
    return instagram_captions_list