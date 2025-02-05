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

if not pc.has_index(index_name):
    pc.create_index(
        name=index_name,
        dimension=1024,
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

# Prepare data for Pinecone
vectors = []
for idx, row in df.iterrows():
    _id = generate_id(row['title'], row['description'], row['image_url'], row['url'])
    # Combine title and description for embedding
    text_for_embedding = f"{row['title']} {row['description']}"
    embedding = get_embedding(text_for_embedding)
    vectors.append({
        "id": _id,
        "values": [float(x) for x in embedding],  # Convert all values to float
        "metadata": {
            "title": row['title'],
            "description": row['description'],
            "image_url": row['image_url'],
            "url": row['url']
        }
    })

# Upsert data into Pinecone
pc.Index(index_name).upsert(vectors=vectors)

print("Data successfully loaded into Pinecone!")

# Dispose of the SQLAlchemy engine
engine.dispose()