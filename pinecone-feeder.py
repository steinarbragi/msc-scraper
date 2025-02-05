from openai import OpenAI
import pandas as pd
from pinecone import Pinecone, ServerlessSpec
from sqlalchemy import create_engine
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()
pc = Pinecone()    

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Function to get embeddings from OpenAI
def get_embedding(text):
    response = client.embeddings.create(
        model="text-embedding-3-small",  # or "text-embedding-ada-002"
        input=text
    )
    return response.data[0].embedding

# Create an index
index_name = "bokai"

# Delete existing index
#if pc.has_index(index_name):
#    pc.delete_index(index_name)

if not pc.has_index(index_name):
    pc.create_index(
        name=index_name,
        dimension=1536,
        metric="cosine",
        spec=ServerlessSpec(
            cloud='aws', 
            region='us-east-1'
        ) 
    ) 

# Create SQLAlchemy engine
DATABASE_URL = f"postgresql://{os.getenv('dbuser')}:{os.getenv('dbpassword')}@{os.getenv('dbhost')}:{os.getenv('dbport')}/{os.getenv('dbname')}"
engine = create_engine(DATABASE_URL)

# Fetch data from PostgreSQL using SQLAlchemy
query = "SELECT id, title, description, image_url, url FROM books"
df = pd.read_sql_query(query, engine)

# Function to generate unique IDs based on data
def generate_id(title, description, image_url, url):
    return hashlib.md5(f"{title}{description}{image_url}{url}".encode('utf-8')).hexdigest()

# Function to save progress
def save_progress(processed_ids):
    with open('progress.txt', 'w') as f:
        for id in processed_ids:
            f.write(f"{id}\n")

# Function to load progress
def load_progress():
    try:
        with open('progress.txt', 'r') as f:
            return set(line.strip() for line in f)
    except FileNotFoundError:
        return set()

# Load previously processed IDs
processed_ids = load_progress()

# Prepare data for Pinecone
vectors = []
batch_size = 100  # Adjust this value based on your needs
total_records = len(df)
processed_count = 0

for idx, row in df.iterrows():
    _id = generate_id(row['title'], row['description'], row['image_url'], row['url'])
    
    # Skip if already processed
    if _id in processed_ids:
        continue
        
    # Combine title and description for embedding
    text_for_embedding = f"{row['title']} {row['description']}"
    embedding = get_embedding(text_for_embedding)
    
    vectors.append({
        "id": _id,
        "values": [float(x) for x in embedding],
        "metadata": {
            "title": row['title'],
            "description": row['description'],
            "image_url": row['image_url'],
            "url": row['url']
        }
    })
    
    processed_count += 1
    
    # When batch is full or at end of data, upsert to Pinecone
    if len(vectors) >= batch_size or idx == len(df) - 1:
        print(f"Upserting batch... Progress: {processed_count}/{total_records} records ({(processed_count/total_records)*100:.2f}%)")
        pc.Index(index_name).upsert(vectors=vectors)
        
        # Save progress
        processed_ids.update(v["id"] for v in vectors)
        save_progress(processed_ids)
        
        # Clear vectors for next batch
        vectors = []

print("Data successfully loaded into Pinecone!")

# Dispose of the SQLAlchemy engine
engine.dispose()