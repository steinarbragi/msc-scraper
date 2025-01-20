
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
python scraper.py
```

The script will create:
- A folder named `book_covers` containing all downloaded images
- A CSV file named `icelandic_children_books.csv` with all book information

Features:
- Respectful scraping with 1-second delays between requests
- Error handling for failed requests/downloads
- Clean filename generation for images
- UTF-8 encoding support for Icelandic characters

Would you like me to modify anything about the scraper? For example, I could:
- Add more metadata fields
- Change the file naming convention
- Add more error handling
- Modify the scraping delay