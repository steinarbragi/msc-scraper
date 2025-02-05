This project contains two scripts. A book scraper and a pinecone feeder.


## Book Scraper

This scraper will:

1. Iterate through all pages of children's and teenage books on the website
2. For each book:
   - Scrape the title, description, and metadata
   - Download the book cover image
   - Store image filename reference in the CSV
3. Save all data to a CSV file with UTF-8 encoding (important for Icelandic characters)

To use the scraper:

1. Install required packages:
```python
pip install requests beautifulsoup4 pandas
```

2. Run the script:
```python
python book-scraper.py
```

The script will create:
- A folder named `book_covers` containing all downloaded images
- A CSV file named `icelandic_children_books.csv` with all book information

Features:
- Respectful scraping with 1-second delays between requests
- Error handling for failed requests/downloads
- Clean filename generation for images
- UTF-8 encoding support for Icelandic characters

## Pinecone Feeder

This script will:

1. Connect to a PostgreSQL database
2. Fetch data from a table
3. Prepare data for Pinecone
4. Upsert data into Pinecone

To use the feeder:  

1. set up environment variables in a .env file

```
PINECONE_API_KEY="FILL"
OPENAI_API_KEY="FILL"
dbname="neondb"
dbuser="neondb_owner"
dbpassword="FILL"
dbhost="FILL"
dbport="5432"
```



2. Install required packages:
```python
pip install pinecone pandas psycopg2 python-dotenv
pip install "pinecone[grpc]" #for the gprc version
```   

3. Run the script:
```python
python pinecone-feeder.py
```





