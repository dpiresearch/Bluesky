# Bluesky hackathon code

Read from firehose into Supabase, then query

Demo video is here: https://youtu.be/m-Rcv4162PY

## Usage

Use firehose.py to read from the Bluesky firehose, embed the content, and store in Supabase.

Use Supabase.py to establish a connection with the same vector db and start performing queries

bot-python is based off of https://github.com/skygaze-ai/bot-python

## Details

### Firehose.py

Content read from bluesky is extracted into author, language, time created, and text
Embeddings are done with sentence-transformers/all-MiniLM-L6-v2 and stored in Supabase with 384 dimensions

### Supabase.py

User query is embedded using the same model and top k (10) content is retrieved from Supabase.



