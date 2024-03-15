import aiohttp
import asyncio
import re
from datetime import datetime
import time
from models import ScrapConfig
import requests
from concurrent.futures import ThreadPoolExecutor
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent

async def fetch_page(session, url, headers, params):
    try:
        async with session.get(url, headers=headers, params=params) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"Aiohttp client error occurred: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

async def fetch_posts_linkedin(linkedin_per_author, urlprofile, headers):
    payload = {
        "profileUrl": linkedin_per_author["profileURL"],
        "includeProfile": True,
        "includeExperiences": True,
        "includeEducations": True,
        "includeCertifications": True,
        "includeSkills": True,
        "includePosts": True
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(urlprofile, json=payload, headers=headers) as response:
            data = await response.json()
            print("data = ", data)
            if data.get("result", {}).get("posts"):
                posts = data["result"]["posts"]
                likes = []
                post_descriptions = []
                for post in posts:
                    likes.append(post.get("reactionCount", 0))
                    post_descriptions.append(post.get("postDescription", ""))

                return {"like": likes, "description": post_descriptions}
            else:
                return None

async def linkedin_scrap(linkedin_rapid_api_url:str,
                         mention: str, 
                         items: int, 
                         linkedin_source: str, 
                         hashtag_pattern: str,
                         mention_pattern: str,
                         y_m_dTHM_format: str,
                         post_type: str,
                         user_id: str,
                         scrap_config) -> list:
    start_time = time.time()
    linkedin_descriptions_list = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "X-RapidAPI-Key": "0fad9a3463mshf8561e48c7f404dp171289jsn3584d0200f72",
        "X-RapidAPI-Host": "linkedin-public-search.p.rapidapi.com"
    }

    total_pages = 3

    async with aiohttp.ClientSession() as session:
        tasks = []
        for page in range(1, total_pages + 1):
            params = {
                "keyword": mention,
                "page": str(page),
                "includePostURLs": "true",
                "limit": "10"
            }
            task = asyncio.ensure_future(fetch_page(session, 
                                                    linkedin_rapid_api_url,
                                                    headers, 
                                                    params))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        
        for data in responses:
            results = data.get("result", [])
            if results is None:
                continue
            for description in results:
                try:
                    description_title = description.get("postDescription")
                    if description_title is None or not isinstance(description_title, str):
                        continue
                    profile_url = description.get("postURL")
                    mentions = scrap_config.extract_mentions(description_title, mention_pattern)
                    hashtags = re.findall(hashtag_pattern, description_title)

                    post_utc_date_str = description.get("postUtcDate")
                    if post_utc_date_str:
                        if len(post_utc_date_str.split('.')) > 1:
                            date_part, time_part = post_utc_date_str.split('T')
                            time_main, time_frac = time_part.split('.')
                            time_frac = time_frac[:6]
                            post_utc_date_str = f"{date_part}T{time_main}.{time_frac}"
                        
                        try:
                            video_date = datetime.fromisoformat(post_utc_date_str.rstrip("Z")).strftime(y_m_dTHM_format)
                        except ValueError:
                            video_date = None 
                    else:
                        video_date = None
                    author_id = description.get("nameSurname", {})
                    profileURL = description.get("profileURL", {})

                    linkedin_document = {
                        "user_id": user_id,
                        "text": description_title,
                        "type": post_type,
                        "source": linkedin_source,
                        "source_link": profile_url,
                        "date": video_date,
                        "mention": mention,
                        "nbr_mentions": len(mentions),
                        "nbr_hashtags": len(hashtags),
                        "mentions_texts": mentions,
                        "hashtags_texts": hashtags,
                        "author": author_id,
                        "profileURL": profileURL
                    }

                    linkedin_descriptions_list.append(linkedin_document)
                except KeyError:
                    continue

    if not linkedin_descriptions_list:
        print(f"No LinkedIn posts found for mention {mention}")  
    else:
        urlprofile = "https://linkedin-public-search.p.rapidapi.com/getpeoplebyurl"

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_linkedin = {executor.submit(fetch_posts_linkedin, linkedin_author, urlprofile, headers): linkedin_author for linkedin_author in linkedin_descriptions_list}
        
        for future in concurrent.futures.as_completed(future_to_linkedin):
            linkedin = future_to_linkedin[future]
            author_info = future.result() 
            print("author_info", author_info)
            linkedin.update(author_info)

    end_time = time.time()
    duration = end_time - start_time
    print(f"The LinkedIn function took {duration:.2f} seconds to complete ")
    print(f"Number of records in linkedin_descriptions_list: {len(linkedin_descriptions_list)}") 

    return linkedin_descriptions_list

########### TEST ###################

async def main():
    linkedin_rapid_api_url = 'https://linkedin-public-search.p.rapidapi.com/postsearch'
    mention = "nike"
    items = 3
    linkedin_source = "instagram_app"
    hashtag_pattern = "#\w+"
    mention_pattern = "@\w+"
    y_m_dTHM_format = "%Y-%m-%dT%H:%M"
    post_type = "video"
    user_id = "12345"
    scrap_config = ScrapConfig()
    scrap_config.set_mention_pattern(mention_pattern)
    result = await linkedin_scrap(linkedin_rapid_api_url, mention, items, linkedin_source, hashtag_pattern, mention_pattern, y_m_dTHM_format, post_type, user_id, scrap_config)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())




###############################################################################""
# def testfunc():

#     url = "https://linkedin-public-search.p.rapidapi.com/getpeoplebyurl"

#     payload = {
#         "profileUrl": "https://www.linkedin.com/in/moosa-kirmani",
#         "includeProfile": True,
#         "includeExperiences": True,
#         "includeEducations": True,
#         "includeCertifications": True,
#         "includeSkills": True,
#         "includePosts": True
#     }
#     headers = {
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
#         "X-RapidAPI-Key": "0fad9a3463mshf8561e48c7f404dp171289jsn3584d0200f72",
#         "X-RapidAPI-Host": "linkedin-public-search.p.rapidapi.com"
#     }

#     response = requests.post(url, json=payload, headers=headers)

#     print(response.json())
# print(testfunc())
