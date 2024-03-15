import pandas as pd
import os
import tiktoken
from openai import OpenAI
import csv 
import shutil
import string
import re

# client = OpenAI()
# def get_embedding(text, model):
#     text = preprocess_text(text)
#     return client.embeddings.create(input=[text], model=model).data[0].embedding

def preprocess_text(text):
    return re.sub(r"\s+", "", text)

def remove_symbols(text):
    pattern = r"[^\w\s]" 
    return re.sub(pattern, "", text)

def remove_icons(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F" 
                               u"\U0001F300-\U0001F5FF"  
                               u"\U0001F680-\U0001F6FF"  
                               u"\U0001F1E0-\U0001F1FF"  
                               u"\U00002500-\U00002BEF" 
                               u"\U00002702-\U000027B0"
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\U00010000-\U0010ffff"
                               u"\u2640-\u2642"
                               u"\u2600-\u2B55"
                               u"\u200d"
                               u"\u23cf"
                               u"\u23e9"
                               u"\u231a"
                               u"\ufe0f"  
                               u"\u3030"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

current_dir = os.path.dirname(os.path.realpath(__file__))
csv_data_folder = os.path.join(current_dir, '..', 'csv_data')
csv_reddit = os.path.join(csv_data_folder, 'reddit_posts.csv')
csv_twitter = os.path.join(csv_data_folder, 'twitter_posts.csv')
csv_youtube = os.path.join(csv_data_folder, 'youtube_posts.csv')
combined_csv = os.path.join(csv_data_folder, 'combined_posts.csv')

with open(combined_csv, 'a', newline='', encoding='utf-8') as combined_file:
    writer = csv.DictWriter(combined_file, fieldnames=["Source", "Like", "Author", "Description", "User_id", "Text", "Type", "Source_link", "Date", "Mention", "Nbr_mentions", "Nbr_hashtags", "Mentions_texts", "Hashtags_texts"])

    with open(csv_reddit, 'r', newline='', encoding='utf-8') as reddit_file:
        reddit_reader = csv.DictReader(reddit_file)
        for row in reddit_reader:
            row["Description"] = remove_icons(remove_symbols(preprocess_text(row["Description"])))
            writer.writerow(row)
            
    with open(csv_twitter, 'r', newline='', encoding='utf-8') as twitter_file:
        twitter_reader = csv.DictReader(twitter_file)
        for row in twitter_reader:
            row["Description"] = remove_icons(remove_symbols(preprocess_text(row["Description"])))
            writer.writerow(row)

    with open(csv_youtube, 'r', newline='', encoding='utf-8') as youtube_file:
        youtube_reader = csv.DictReader(youtube_file)
        for row in youtube_reader:
            row["Description"] = remove_icons(remove_symbols(preprocess_text(row["Description"])))
            writer.writerow(row)

combined_csv_destination = os.path.join(csv_data_folder, 'combined_posts.csv')
shutil.move(combined_csv, combined_csv_destination)

print(f"Combined CSV file moved successfully to: {combined_csv_destination}")

df = pd.read_csv(combined_csv_destination)
df = df.dropna()

print(df.columns)
print(df.head(10))

embedding_model = "text-embedding-3-small"
embedding_encoding = "cl100k_base"
max_tokens = 800

top_n = 400

encoding = tiktoken.get_encoding(embedding_encoding)

df["n_tokens"] = df.Text.apply(lambda x: len(encoding.encode(x)))
df = df[df.n_tokens <= max_tokens].tail(top_n)
print(len(df))

# df["embedding"] = df.Text.apply(lambda x: get_embedding(x, model=embedding_model))
# output_csv_file_path = os.path.join(csv_data_folder, 'reddit_posts_with_embeddings_100.csv')
# df.to_csv(output_csv_file_path)

# a = get_embedding("hi", model=embedding_model)
# print("a =", a)
