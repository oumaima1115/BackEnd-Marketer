from django.db import models
import re

class ScrapConfig:
    def __init__(self):
        self.mention_pattern = r"@\w+"

    def set_mention_pattern(self, text):
        self.mention_pattern = r"@\w+"

    def extract_mentions(self, text, mention_pattern):
        mentions = re.findall(mention_pattern, text)
        return mentions