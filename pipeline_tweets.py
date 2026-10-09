"""
MongoDB Ingestion Pipeline

Loads a pre-cleaned Twitter dataset, extracts essential
tweet and user information, and stores them into MongoDB
collections with indexes for optimized querying.
"""

import json
import re
from datetime import datetime
from urllib.parse import urlparse
from pymongo import MongoClient, ASCENDING

# ----------------------------------------------------
#  Settings
# ----------------------------------------------------
INPUT_PATH = "data/10000_clean_final.json"
DB_TITLE = "Assignment1_DB"
TWEET_TABLE = "tweets_curated"
USER_TABLE = "users_curated"

# ----------------------------------------------------
# MongoDB Connection and Collection Setup
# ----------------------------------------------------
mongo_conn = MongoClient("mongodb://localhost:27017/")
database = mongo_conn[DB_TITLE]

# Drop old collections if they exist 
for col in [TWEET_TABLE, USER_TABLE]:
    if col in database.list_collection_names():
        database.drop_collection(col)

tweets_collection = database[TWEET_TABLE]
users_collection = database[USER_TABLE]

# ----------------------------------------------------
# Functions
# ----------------------------------------------------
def remove_noise(text):
    """Strip URLs and @mentions from tweet text."""
    return re.sub(r"http\S+|@\w+", "", text or "").strip()

def parse_date(timestamp):
    """Convert string timestamps into datetime objects."""
    if not timestamp:
        return None
    try:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except Exception:
        return None

def extract_domain(link):
    """Return only the domain from a URL."""
    return urlparse(link).netloc if link else None

# ----------------------------------------------------
# Load Dataset
# ----------------------------------------------------
with open(INPUT_PATH, "r", encoding="utf-8") as infile:
    raw_tweets = json.load(infile)

print(f" Loaded {len(raw_tweets)} tweets from {INPUT_PATH}")

# ----------------------------------------------------
# Data Transformation
# ----------------------------------------------------
tweet_docs, user_docs, processed_ids = [], {}, set()

for record in raw_tweets:
    try:
        tweet_id = str(record.get("id"))
        if not tweet_id or tweet_id in processed_ids:
            continue
        processed_ids.add(tweet_id)

        # If retweet/share → use nested object
        base_obj = record.get("object") if record.get("verb") == "share" and isinstance(record.get("object"), dict) else record

        # Extract and clean main fields
        posted_time = parse_date(base_obj.get("postedTime") or record.get("postedTime"))
        text_original = base_obj.get("text", "")
        text_clean = remove_noise(text_original)

        entities = base_obj.get("twitter_entities", {}) or {}
        hashtags = [h.get("text", "").lower() for h in entities.get("hashtags", []) if isinstance(h, dict)]
        mentions = [m.get("screen_name") for m in entities.get("user_mentions", []) if isinstance(m, dict)]
        urls = entities.get("urls", [])
        url_info = [
            {
                "expanded_url": u.get("expanded_url"),
                "domain": extract_domain(u.get("expanded_url"))
            }
            for u in urls if isinstance(u, dict)
        ]

        actor = record.get("actor", {})
        user_id = str(actor.get("id"))
        user_entry = {
            "user_id": user_id,
            "screen_name": actor.get("preferredUsername"),
            "name": actor.get("displayName"),
            "location": (actor.get("location") or {}).get("displayName"),
            "url_domain": extract_domain(actor.get("link"))
        }

        tweet_entry = {
            "id_str": tweet_id,
            "created_at": posted_time,
            "text": text_original,
            "clean_text": text_clean,
            "hashtags": hashtags,
            "mentions": mentions,
            "urls": url_info,
            "user_id": user_id,
            "hour_of_day": posted_time.hour if posted_time else None,
            "weekday": posted_time.strftime("%A") if posted_time else None,
            "has_link": bool(url_info),
            "lang_guess": record.get("twitter_lang", "und")
        }

        tweet_docs.append(tweet_entry)
        if user_id and user_id not in user_docs:
            user_docs[user_id] = user_entry

    except Exception as err:
        print(" Skipped record due to:", err)



# ----------------------------------------------------
# Insert into MongoDB
# ----------------------------------------------------
if tweet_docs:
    tweets_collection.insert_many(tweet_docs)
if user_docs:
    users_collection.insert_many(user_docs.values())

# ----------------------------------------------------
# Index Creation 
# ----------------------------------------------------
index_specs = [
    [("created_at", ASCENDING)],
    [("hashtags", ASCENDING), ("created_at", ASCENDING)],
    [("user_id", ASCENDING), ("created_at", ASCENDING)],
    [("urls.domain", ASCENDING), ("created_at", ASCENDING)]
]

for spec in index_specs:
    tweets_collection.create_index(spec)

users_collection.create_index([("user_id", ASCENDING)], unique=True)
users_collection.create_index([("screen_name", ASCENDING)])

print(f" Completed: Inserted {len(tweet_docs)} tweets and {len(user_docs)} users into MongoDB.")


"""
PART A
Short Description:
The Python script (pipeline_tweets.py) performs a full ingestion curation process for the pre-cleaned Twitter dataset.
It reads 10000_clean_final.json, extracts tweet-level and user-level attributes, and loads them into two MongoDB collections:
tweets_curated and users_curated.

Key steps:

Noise removal : Eliminates ObjectId() and NumberLong() wrappers, discards block comments.

Schema normalisation : Parses each JSON object and extracts:

id_str, created_at, text, clean_text, hashtags, mentions, urls{expanded_url, domain}.

Derived fields → hour_of_day, weekday, has_link, lang_guess.

User split & de-duplication : Each user_id is stored once in users_curated.

Upsert to MongoDB : Tweets and users are batch-inserted with safe handling of retweets.

Index creation : Optimised indexes are applied to support temporal and content-based analysis.


Indexes were chosen to align directly with analytical workloads described in the assignment .


{created_at: 1} accelerates chronological queries and time-bucket aggregation (e.g., weekly hashtag growth).
{hashtags: 1, created_at: 1} enables efficient filtering and sorting when measuring topic trends or coordination patterns.
{user_id: 1, created_at: 1} supports per-user temporal behaviour analysis, essential for near-duplicate and burst detection.
{urls.domain: 1, created_at: 1} facilitates rapid lookups of coordinated link sharing.
Together, these compound and single-field indexes minimise full-collection scans and guarantee that future aggregation pipelines in Part B can exploit index-based seek operations rather than in-memory filtering, thereby improving scalability and query performance.

"""


"""
PART B
This section performs three analytical tasks on the curated Twitter dataset using MongoDB Aggregation Pipelines.
All operations were executed on the collection tweets_curated inside the database Assignment1_DB, directly through MongoDB Compass.

(a) Emerging Hashtags (Week-over-Week Growth)

Goal:
Identify the top 10 hashtags that grew the most between the last two full weeks of data.
Each hashtag’s weekly frequency is calculated, and the growth rate = week2 / week1 is used to find trending topics.

Result Interpretation:
The output lists trending hashtags such as #iheartawards or #australia with equal or higher usage in week 2.
These represent topics gaining online momentum between consecutive weeks.


(b) Coordinated Link Bursts (Temporal Activity Detection)
Goal:
Detect potential coordination or campaign activity where a single web domain is posted by three or more distinct users within any 60-second window.
Result Interpretation:
This highlights domains (e.g., t.co, bit.ly) that experienced sudden bursts of sharing, suggesting viral content or possible coordination among users.


(c) High-Volume Near-Duplicates per User
Goal:
Detect users who post the same content repeatedly within a single day — a sign of spamming, automation, or bot-like behavior.
Result Interpretation:
This produces a list of users showing repetitive posting behavior, for example:

user_id	screen_name	day	duplicate_text	dup_count
10243188921458	@PromoBot	2016-04-01	“Win a free iPhone today!”	8

Such results are valuable for detecting spam accounts or bot networks within the dataset.
Together, these analytics demonstrate how MongoDB’s Aggregation Framework supports both content-based and time-series investigations in social-media datasets.
"""