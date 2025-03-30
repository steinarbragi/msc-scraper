from openai import OpenAI
import pandas as pd
from pinecone import Pinecone, ServerlessSpec
from sqlalchemy import create_engine
import hashlib
import os
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv()
pc = Pinecone(
    api_key=os.getenv('PINECONE_API_KEY')
)    

# Function to get embeddings using Pinecone's hosted E5 model
def get_embedding(text):
    embeddings = pc.inference.embed(
        model="multilingual-e5-large",
        inputs=[text],
        parameters={"input_type": "passage"}  # Use 'passage' for content being stored
    )
    # Convert DenseEmbedding to list of floats
    return [float(x) for x in embeddings[0].values]

# Create an index
index_name = os.getenv('pinecone_index')

# Delete existing index
#if pc.has_index(index_name):
#    pc.delete_index(index_name)

if not pc.has_index(index_name):
    pc.create_index(
        name=index_name,
        dimension=1024,  # E5 large has 1024 dimensions
        metric="cosine",  # E5 was trained using cosine similarity
        spec=ServerlessSpec(
            cloud='aws', 
            region='us-east-1'
        ) 
    ) 

# Create SQLAlchemy engine
DATABASE_URL = f"postgresql://{os.getenv('dbuser')}:{os.getenv('dbpassword')}@{os.getenv('dbhost')}:{os.getenv('dbport')}/{os.getenv('dbname')}"
engine = create_engine(DATABASE_URL)

# Fetch data from PostgreSQL using SQLAlchemy
query = "SELECT id, title, description, image_url, url, age_group FROM scraped_books"
df = pd.read_sql_query(query, engine)
print(f"Read {len(df)} records from database")

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
print(f"Found {len(processed_ids)} previously processed IDs")

# Prepare data for Pinecone
vectors = []
batch_size = 100
total_records = len(df)
processed_count = 0
skipped_count = 0

for idx, row in df.iterrows():
    print(f"Processing record {idx}...")  # Debug print
    _id = generate_id(row['title'], row['description'], row['image_url'], row['url'])
    
    # Skip if already processed
    if _id in processed_ids:
        skipped_count += 1
        continue
        
    # Print first record's embedding to verify dimension
    if idx == 0:
        text_for_embedding = f"{row['title']}\n{row['description']}"
        if pd.notna(row['age_group']):
            text_for_embedding += f"\nAldurshópur: {row['age_group']}"
        print(f"Creating first embedding for text: {text_for_embedding[:100]}...")  # Debug print
        embedding = get_embedding(text_for_embedding)
        print(f"First embedding dimension: {len(embedding)}")
    
    # Combine title, description, and age group for embedding
    text_for_embedding = f"{row['title']}\n{row['description']}"
    if pd.notna(row['age_group']):
        text_for_embedding += f"\nAldurshópur: {row['age_group']}"
    print(f"Creating embedding for record {idx}...")  # Debug print
    embedding = get_embedding(text_for_embedding)
    print(f"Embedding created for record {idx}")  # Debug print
    
    # Prepare metadata
    metadata = {
        "title": row['title'],
        "description": row['description'],
        "image_url": row['image_url'],
        "url": row['url']
    }
    
    # Add age group to metadata if it exists
    if pd.notna(row['age_group']):
        metadata["age_group"] = row['age_group']
    
    vectors.append({
        "id": _id,
        "values": embedding,
        "metadata": metadata
    })
    
    processed_count += 1
    
    # When batch is full or at end of data, upsert to Pinecone
    if len(vectors) >= batch_size or idx == len(df) - 1:
        print(f"Attempting to upsert {len(vectors)} vectors...")
        try:
            pc.Index(index_name).upsert(vectors=vectors)
            stats = pc.Index(index_name).describe_index_stats()
            print(f"Upsert successful. Total vectors in index: {stats.total_vector_count}")
        except Exception as e:
            print(f"Error during upsert: {e}")
            print(f"First vector in batch: {vectors[0]}")
        
        # Save progress
        processed_ids.update(v["id"] for v in vectors)
        save_progress(processed_ids)
        
        # Clear vectors for next batch
        vectors = []

print(f"\nSummary:")
print(f"Total records: {total_records}")
print(f"Skipped records: {skipped_count}")
print(f"Processed records: {processed_count}")

# Dispose of the SQLAlchemy engine
engine.dispose()