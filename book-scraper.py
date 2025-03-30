import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import re
from requests.exceptions import RequestException

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}

def clean_filename(filename):
    """Clean filename from invalid characters"""
    return re.sub(r'[<>:"/\\|?*]', '', filename)

def download_image(url, folder):
    """Download image and return filename"""
    if not url:
        return None
    
    try:
        response = requests.get(url, stream=True, headers=HEADERS)
        if response.status_code == 200:
            filename = clean_filename(os.path.basename(url))
            filepath = os.path.join(folder, filename)
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            return filename
    except Exception as e:
        print(f"Error downloading image {url}: {e}")
    return None

def get_highest_res_image(img_tag):
    """Get the highest resolution image from srcset"""
    if 'srcset' in img_tag.attrs:
        srcset = img_tag['srcset']
        images = [x.strip() for x in srcset.split(',')]
        if images:
            return images[0].split()[0]
    return img_tag.get('src')

def find_description(soup):
    """Find the correct description element that contains either p tags or articles"""
    desc_divs = soup.find_all('div', class_='jet-listing-dynamic-field__content')
    for div in desc_divs:
        description_parts = []
        
        # Check for articles
        articles = div.find_all('article')
        if articles:
            for article in articles:
                # Get text directly from article if it has content
                article_text = article.get_text(strip=True)
                if article_text:
                    description_parts.append(article_text)
            if description_parts:
                return '\n\n'.join(description_parts)
        
        # If no articles with content, check for p tags
        p_tags = div.find_all('p')
        if p_tags and any(p.get_text(strip=True) for p in p_tags):
            description_parts = [p.get_text(strip=True) for p in p_tags if p.get_text(strip=True)]
            if description_parts:
                return '\n\n'.join(description_parts)
        
        # If no p tags, check if the div itself contains direct text
        direct_text = div.get_text(strip=True)
        if direct_text and not div.find('div'):  # Ensure we don't get text from nested divs
            return direct_text
            
    return ''

def extract_age_group(soup):
    """Extract age group from breadcrumb navigation"""
    try:
        # Look for the elementor widget containing the breadcrumb with the specific data-id
        breadcrumb_div = soup.find('div', class_='elementor-widget-text-editor', attrs={'data-id': '8c4e534'})
        if breadcrumb_div:
            # Find all links in the breadcrumb
            links = breadcrumb_div.find_all('a')
            # Collect all age groups
            age_groups = []
            for link in links:
                text = link.text.strip()
                # Check if the text matches an age group pattern
                if any(pattern in text.lower() for pattern in ['ára', 'ár']):
                    age_groups.append(text)
            
            if age_groups:
                result = ", ".join(age_groups)
                print(f"Found age groups: {result}")  # Debug log
                return result
            else:
                print("No age groups found in breadcrumb")  # Debug log
        else:
            print("Breadcrumb div not found")  # Debug log
    except Exception as e:
        print(f"Error extracting age group: {e}")
    return None

def scrape_book_details(url):
    """Scrape individual book page"""
    try:
        print(f"Scraping book: {url}")
        response = requests.get(url, headers=HEADERS, timeout=8)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get title from h2
        title_elem = soup.find('h2', class_='product_title entry-title')
        if not title_elem:
            print(f"Warning: Could not find title element on {url}")
            return None
        title = title_elem.text.strip()
        
        # Get description
        description = find_description(soup)
        if not description:
            print(f"Warning: Could not find description for {title}")
        
        # Get metadata from the product attributes table
        metadata = {}
        table = soup.find('table', class_='woocommerce-product-attributes')
        if table:
            for row in table.find_all('tr'):
                key = row.find('th').text.strip()
                value = row.find('td').text.strip()
                metadata[key] = value
        
        # Get age group from breadcrumb
        age_group = extract_age_group(soup)
        if age_group:
            metadata['age_group'] = age_group
            print(f"Added age group '{age_group}' to metadata for {title}")  # Debug log
        else:
            print(f"No age group found for {title}")  # Debug log
        
        # Get main product image
        image_url = None
        gallery = soup.find('div', class_='woocommerce-product-gallery')
        if gallery:
            img = gallery.find('img', class_='wp-post-image')
            if img:
                image_url = get_highest_res_image(img)
        
        return {
            'title': title,
            'description': description,
            'metadata': metadata,
            'image_url': image_url,
            'url': url  # Adding the URL for reference
        }
    except Exception as e:
        print(f"Error scraping book details from {url}: {e}")
        return None

def main():
    base_url = 'https://www.forlagid.is/voruflokkur/barna-og-unglingabaekur/'
    books_data = []
    
    # Create images folder if it doesn't exist
    image_folder = 'book_covers'
    os.makedirs(image_folder, exist_ok=True)
    
    page = 1
    while True:
        print(f"\n{'='*50}")
        print(f"Scraping page {page}")
        
        url = f"{base_url}page/{page}/" if page > 1 else base_url
        try:
            response = requests.get(url, headers=HEADERS, timeout=8)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all book links within jet-listing-dynamic-image divs
            book_elements = soup.find_all('div', class_='jet-listing-dynamic-image')
            
            if not book_elements:
                print("No more books found on this page. Ending scrape.")
                break
            
            print(f"Found {len(book_elements)} books on page {page}")
            
            # Process each book
            for book_elem in book_elements:
                link = book_elem.find('a', class_='jet-listing-dynamic-image__link')
                if link and 'href' in link.attrs:
                    book_url = link['href']
                    book_data = scrape_book_details(book_url)
                    
                    if book_data:
                        # Get the cover image URL from the listing page
                        list_img = link.find('img')
                        if list_img:
                            cover_image_url = get_highest_res_image(list_img)
                            image_filename = download_image(cover_image_url, image_folder)
                        else:
                            image_filename = None
                        
                        # Prepare row for CSV
                        book_row = {
                            'title': book_data['title'],
                            'description': book_data['description'],
                            'image_filename': image_filename,
                            'url': book_data['url']  # Include the URL in the CSV
                        }
                        
                        # Add metadata fields
                        for key, value in book_data['metadata'].items():
                            # Convert metadata keys to snake_case
                            snake_key = key.lower().replace(' ', '_')
                            book_row[snake_key] = value
                        
                        books_data.append(book_row)
                        print(f"Successfully processed: {book_data['title']}")
                        print(f"Current number of books in data: {len(books_data)}")
                    
                    # Be nice to the server
                    time.sleep(1)
            
            # Save progress after each page
            if books_data:
                try:
                    df = pd.DataFrame(books_data)
                    print(f"\nSaving CSV with {len(df)} rows and columns: {df.columns.tolist()}")
                    df.to_csv('icelandic_children_books.csv', index=False, encoding='utf-8-sig')
                    print(f"Progress saved! Current total: {len(books_data)} books")
                except Exception as e:
                    print(f"Error saving CSV: {e}")
            
            page += 1
            
        except RequestException as e:
            print(f"Network error on page {page}: {e}")
            break
        except Exception as e:
            print(f"Error on page {page}: {e}")
            break
    
    # Final save
    if books_data:
        try:
            df = pd.DataFrame(books_data)
            print(f"\nFinal save - Saving CSV with {len(df)} rows and columns: {df.columns.tolist()}")
            df.to_csv('icelandic_children_books.csv', index=False, encoding='utf-8-sig')
            print(f"\nScraping completed. Total books scraped: {len(books_data)}")
        except Exception as e:
            print(f"Error in final CSV save: {e}")
    else:
        print("\nNo books were successfully scraped.")

if __name__ == "__main__":
    main()