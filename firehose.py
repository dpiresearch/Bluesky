import os
import uuid

from atproto import (
    CAR,
    AtUri,
    Client as Bclient,
    FirehoseSubscribeReposClient,
    firehose_models,
    models,
    parse_subscribe_repos_message,
)
from dotenv import load_dotenv
from typing import List, Dict, Tuple
import numpy as np

# Load environment variables
load_dotenv()

from supabase import create_client, Client as Sclient

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Sclient = create_client(url, key)

# Bluesky credentials
BLUESKY_USERNAME = os.getenv("BLUESKY_USERNAME")
BLUESKY_PASSWORD = os.getenv("BLUESKY_PASSWORD")

# Create a Bluesky client
client = Bclient("https://bsky.social")
firehose = FirehoseSubscribeReposClient()

import vecs

# create vector store client
vx = vecs.create_client("postgresql://postgres.mdftmckzbmqpccxrmowd:dG22SQWjMj2ZZNIY@aws-0-us-west-1.pooler.supabase.com:5432/postgres")

# create a collection of vectors with 3 dimensions
docs = vx.get_or_create_collection(name="bsky_collection", dimension=384)
docs.create_index()

from sentence_transformers import SentenceTransformer

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

records: List[Tuple[str, np.ndarray, Dict]] = []
count_rec = 0

def process_vectors(record):
#    print(record)

    global model, docs, records, count_rec
    lang = record['langs'][0] if 'langs' in record and record['langs'] else 'unknown'
    # lang = record['langs'][0]
    uid = record['author']
    cid = record['cid']
    post_time = record['createdAt']
    text = record['text']
    print(f"cid: {str(cid)} Langs: {str(lang)} UID: {uid}, Post Time: {post_time}, Text: {text}")

    embedding = model.encode(text)
    records.append((f"{cid}", embedding, {"text": text, "lang": lang[0], "uid": uid, "time":post_time}))
    if count_rec % 5 == 0:
        docs.upsert(records)
#        records: List[Tuple[str, np.ndarray, Dict]] = []
        records = []
        print("Performed upsert at count: " + str(count_rec))
    count_rec += 1

def process_record(record):
#    print(record)
    lang = record['langs'][0] if 'langs' in record and record['langs'] else 'unknown'
    # lang = record['langs'][0]
    uid = record['author']
    post_time = record['createdAt']
    text = record['text']
    print(f"Langs: {str(lang)} UID: {uid}, Post Time: {post_time}, Text: {text}")

    # Generate a random UUID.
    unique_uuid = uuid.uuid4()

    data, count = supabase.table('bsky_mmrd').insert({"lang": lang[0], "uid": uid, "time":post_time, "text":text}).execute()

def process_operation(
    op: models.ComAtprotoSyncSubscribeRepos.RepoOp,
    car: CAR,
    commit: models.ComAtprotoSyncSubscribeRepos.Commit,
) -> None:
    uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")

    if op.action == "create":
        if not op.cid:
            return

        record = car.blocks.get(op.cid)
        if not record:
            return

        record = {
            "uri": str(uri),
            "cid": str(op.cid),
            "author": commit.repo,
            **record,
        }
    

        if uri.collection == models.ids.AppBskyFeedPost:
            # This logs the text of every post off the firehose.
            # Just for fun :)
            # Delete before actually using
            # process_record(record)
            process_vectors(record)
            # print(record)
            # print(record['text'])
        
            if "hack-bot" in record["text"]:
                # get some info about the poster, their posts, and the thread they tagged the bot in
                poster_posts = client.get_author_feed(
                    actor=record["author"], cursor=None, filter=None, limit=100
                ).feed
                poster_follows = client.get_follows(actor=record["author"]).follows
                poster_profile = client.get_profile(actor=record["author"])
                posts_in_thread = client.get_post_thread(uri=record["uri"])

                # send a reply to the post
                record_ref = {"uri": record["uri"], "cid": record["cid"]}
                reply_ref = models.AppBskyFeedPost.ReplyRef(
                    parent=record_ref, root=record_ref
                )
                client.send_post(
                    reply_to=reply_ref,
                    text=f"Hey, {poster_profile.display_name}. You have {len(poster_posts)} posts and {len(poster_follows)} follows. Your bio is: {poster_profile.description}. There are {len(posts_in_thread)} posts in the thread.",
                )

        # elif uri.collection == models.ids.AppBskyFeedLike:
        #     print("Created like: ", record)
        # elif uri.collection == models.ids.AppBskyFeedRepost:
        #     print("Created repost: ", record)
        # elif uri.collection == models.ids.AppBskyGraphFollow:
        #     print("Created follow: ", record)

    if op.action == "delete":
        # Process delete(s)
        return

    if op.action == "update":
        # Process update(s)
        return

    return


# No need to edit this function - it processes messages from the firehose
def on_message_handler(message: firehose_models.MessageFrame) -> None:
    commit = parse_subscribe_repos_message(message)
    if not isinstance(
        commit, models.ComAtprotoSyncSubscribeRepos.Commit
    ) or not isinstance(commit.blocks, bytes):
        return

    car = CAR.from_bytes(commit.blocks)

    for op in commit.ops:
        process_operation(op, car, commit)


def main() -> None:
    client.login(BLUESKY_USERNAME, BLUESKY_PASSWORD)
    print("🤖 Bot is listening")
    firehose.start(on_message_handler)


if __name__ == "__main__":
    main()
