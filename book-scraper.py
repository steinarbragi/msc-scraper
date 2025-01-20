import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from urllib.parse import urljoin
import re

def clean_filename(filename):
    """Clean filename from invalid characters"""
    return re.sub(r'[<>:"/\\|?*]', '', filename)

def download_image(url, folder):
    """Download image and return filename"""
    if not url:
        return None
    
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            # Extract filename from URL and clean it
            filename = clean_filename(os.path.basename(url))
            filepath = os.path.join(folder, filename)
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            return filename
    except Exception as e:
        print(f"Error downloading image {url}: {e}")
    return None

def scrape_book_details(url):
    """Scrape individual book page"""
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get book details
        title = soup.find('h1', class_='product_title').text.strip() if soup.find('h1', class_='product_title') else ''
        description = soup.find('div', class_='woocommerce-product-details__short-description')
        description = description.text.strip() if description else ''
        
        # Get metadata table
        metadata = {}
        table = soup.find('table', class_='woocommerce-product-attributes')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                key = row.find('th').text.strip()
                value = row.find('td').text.strip()
                metadata[key] = value
        
        # Get main image URL
        image_url = None
        image_div = soup.find('div', class_='woocommerce-product-gallery__image')
        if image_div:
            image_url = image_div.find('a')['href'] if image_div.find('a') else None
        
        return {
            'title': title,
            'description': description,
            'metadata': metadata,
            'image_url': image_url
        }
    except Exception as e:
        print(f"Error scraping book page {url}: {e}")
        return None

def main():
    base_url = 'https://www.forlagid.is/voruflokkur/barna-og-unglingabaekur/'
    books_data = []
    
    # Create images folder if it doesn't exist
    image_folder = 'book_covers'
    os.makedirs(image_folder, exist_ok=True)
    
    # Get all pages of books
    page = 1
    while True:
        print(f"Scraping page {page}")
        url = f"{base_url}page/{page}/" if page > 1 else base_url
        
        response = requests.get(url)
        if response.status_code != 200:
            break
            
        soup = BeautifulSoup(response.text, 'html.parser')
        books = soup.find_all('li', class_='product')
        
        if not books:
            break
            
        # Process each book on the page
        for book in books:
            link = book.find('a', class_='woocommerce-LoopProduct-link')
            if link:
                book_url = link['href']
                book_data = scrape_book_details(book_url)
                
                if book_data:
                    # Download image
                    if book_data['image_url']:
                        image_filename = download_image(book_data['image_url'], image_folder)
                    else:
                        image_filename = None
                    
                    # Prepare data for CSV
                    book_row = {
                        'Title': book_data['title'],
                        'Description': book_data['description'],
                        'Image_Filename': image_filename
                    }
                    
                    # Add metadata fields
                    for key, value in book_data['metadata'].items():
                        book_row[key] = value
                    
                    books_data.append(book_row)
                    
                    # Be nice to the server
                    time.sleep(1)
        
        page += 1
    
    # Create DataFrame and save to CSV
    if books_data:
        df = pd.DataFrame(books_data)
        df.to_csv('icelandic_children_books.csv', index=False, encoding='utf-8-sig')
        print(f"Scraped {len(books_data)} books successfully!")

if __name__ == "__main__":
    main()
