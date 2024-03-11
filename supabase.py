#
# Query the supabase db for top k similar posts
#
import os
import uuid

from supabase import create_client, Client

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
key_2: str = os.environ.get("SUPABASE_JWT")
supabase: Client = create_client(url, key)

# unique_uuid = uuid.uuid4()

#data, count = supabase.table('countries').insert({"name": "Denmark"}).execute()

from sentence_transformers import SentenceTransformer

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

import vecs

# create vector store client
vx = vecs.create_client("postgresql://postgres.mdftmckzbmqpccxrmowd:"+key_2+"@aws-0-us-west-1.pooler.supabase.com:5432/postgres")

docs = vx.get_or_create_collection(name="bsky_collection", dimension=384)
docs.create_index()

while (True):
    query_str = input("What are you searching for? ")
    query_embeddings = model.encode(query_str)

    # print(query_embeddings)

    # query the collection filtering metadata for "year" = 2012
    ids = docs.query(
        data=query_embeddings,      # required
        limit=10,                         # number of records to return
        #     filters={"lang": {"$eq": "en"}}, # metadata filters
    )

    # print("ids: " + str(ids))

    for id in ids:
        # Load the next row from the dataset
        query_results = docs.fetch(ids=[f'{id}'])
    
        (query_id, query_embedding, query_meta) = query_results[0]

        # Retrieve the original text from the row's metadata
        query_text = query_meta["text"]
        lang = query_meta["lang"]
        time = query_meta["time"]

        print(query_text + " lang : " + lang + " time: " + str(time))
