from concurrent.futures import ThreadPoolExecutor
import concurrent
import re
from datetime import datetime
import time
import requests
from logging import log
import praw
from models import ScrapConfig

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
        comment_id = comment.id  # Extract the comment ID
        post_id = comment.submission.id  # Extract the post ID
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
            print("reddit document", reddit_document)
            return reddit_document
        return None
    
def process_profile_reddit(reddit_list,headers):
    author_id = reddit_list["author"]
    profile_url = f"https://api.reddit.com/user/{author_id}/submitted"
    response = requests.get(profile_url, headers=headers)
    data = response.json()
    reddit_post = data['data']['children']
    like = 0
    selftext = ""
    title_reddit = ""
    for author_data in reddit_post:
        like = author_data['data']['upvote_ratio']
        selftext = author_data['data']['selftext']
        title_reddit = author_data['data']['title']

    return {"like": like, "author": author_id, "description": selftext, "title": title_reddit}

def reddit_scrap(y_m_dTHMSZ_format : str,
                 start_date : str,
                 end_date : str,
                 hashtag_pattern : str,
                 mention_pattern : str,
                 y_m_dTHM_format : str,
                 post_type : str,
                 comment_type : str,
                 reddit_source : str,
                 mention: str,
                 user_id: str,
                 scrap_config) -> list:
    start_time = time.time()
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
    initial_start_date = start_date
    initial_end_date = end_date

    start_date = datetime.strptime(initial_start_date, y_m_dTHMSZ_format)
    start_timestamp = int(start_date.timestamp())

    end_date = datetime.strptime(initial_end_date, y_m_dTHMSZ_format)
    end_timestamp = int(end_date.timestamp())

    reddit_posts_comments_list = []

    posts = subreddit.search("python", limit=5)

    print("posts", posts)
    post_list = list(posts)
    print("posts lists", post_list)
    print(f"Number of posts found: {len(post_list)}")

    if post_list:
        for post in post_list:
            mentions = []
            hashtags = []

            reddit_post_source_link = f"https://www.reddit.com{post.permalink}"
            document_text = post.title + post.selftext
            print("document text", document_text)
            mentions = scrap_config.extract_mentions(document_text, mention_pattern)
            hashtags = re.findall(hashtag_pattern, document_text)
            document_author = post.author
            if (mentions or hashtags) and document_author is not None:
                document_date = datetime.utcfromtimestamp(post.created_utc).\
                    strftime(y_m_dTHM_format)
                reddit_document = {"user_id": user_id,
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
                print("reddit document", reddit_document)

        # Retrieve all top-level comments
        comments = post.comments
        comments.replace_more(limit=limit)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            args_for_comments = [(comment, mention, y_m_dTHM_format, user_id, comment_type, reddit_source, mention_pattern, hashtag_pattern, scrap_config) for comment in comments.list()]
            results = list(executor.map(lambda args: process_comment(*args), args_for_comments))

        reddit_posts_comments_list.extend(filter(None, results))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_post = {executor.submit(process_profile_reddit, post, REDDIT_HEADERS): post for post in reddit_posts_comments_list}
    for future in concurrent.futures.as_completed(future_to_post):
        post = future_to_post[future]
        try:
            reddit_profile_data = future.result()  # Fetch result from the future
            # Update the original post dictionary with fetched profile info
            post.update(reddit_profile_data)  
        except Exception as exc:
            print(f"Error fetching profile info for post: {exc}")
    end_time = time.time()
    duration = end_time - start_time
    print(f"The Reddit function took {duration:.2f} seconds to complete")
    print(f"Number of records in reddit_posts_comments_list: {len(reddit_posts_comments_list)}")
    print("Flat list Reddit:", reddit_posts_comments_list)
    return reddit_posts_comments_list

####################Test #######################

mention = "nike"
items = 3
reddit_source = "reddit_app"
hashtag_pattern = "#\w+"
mention_pattern = "@\w+"
y_m_dTHM_format = "%Y-%m-%dT%H:%M"
post_type = "video"
user_id = "12345"
scrap_config = ScrapConfig()
comment_type="commet"
y_m_dTHMSZ_format = "%Y-%m-%dT%H:%M:%SZ"
start_date = datetime(2024, 2, 1).isoformat() + 'Z'
end_date = datetime(2024, 2, 28).isoformat() + 'Z'
print(reddit_scrap(y_m_dTHMSZ_format,
                 start_date,
                 end_date,
                 hashtag_pattern,
                 mention_pattern,
                 y_m_dTHM_format,
                 post_type,
                 comment_type ,
                 reddit_source,
                 mention,
                 user_id,
                 scrap_config))