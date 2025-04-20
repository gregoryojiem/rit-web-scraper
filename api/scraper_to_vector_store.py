import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from markdownify import markdownify as md
from io import BytesIO
from typing import Dict, List, Union

from .vector_store_util import make_vector_store, update_existing_vector_store, fetch_existing_vector_store


def sanitize_path_part(part):
    """Replaces invalid characters in paths with underscores"""
    return re.sub(r'[\\/:*?"<>|]', '_', part).strip()


def get_local_path(url, domain):
    """Converts URL to a file system path"""
    parsed = urlparse(url)
    if parsed.netloc != domain:
        return None

    path = parsed.path.strip('/')
    path_parts = path.split('/') if path else []

    sanitized_path_parts = [sanitize_path_part(part) for part in path_parts]

    if parsed.path.endswith('/') or not path_parts:
        sanitized_path_parts.append('index')

    local_path = '/'.join(sanitized_path_parts)
    
    return local_path


def html_to_markdown(url, domain, session, headers):
    """Converts HTML from URL to markdown"""
    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        final_url = response.url
        if urlparse(final_url).netloc != domain:
            print(f"Skipping external redirect: {final_url}")
            return None

        content_type = response.headers.get('Content-Type', '').split(';')[0]
        
        if content_type != 'text/html':
            return None

        try:
            html_content = response.content.decode('utf-8', errors='ignore')
        except UnicodeDecodeError:
            html_content = response.content.decode('latin-1', errors='ignore')

        markdown_content = md(html_content)
        local_path = get_local_path(url, domain)
        
        if local_path:
            return {
                'url': url,
                'path': local_path,
                'content': markdown_content
            }
        
        return None

    except Exception as e:
        print(f"Failed to download {url}: {str(e)}")
        return None


def download_static_website(base_url):
    """Crawls website and converts pages to markdown"""
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36"
    }

    parsed_base = urlparse(base_url)
    domain = parsed_base.netloc

    visited = set()
    queue = [base_url]
    visited.add(base_url)
    markdown_files = []

    while queue:
        current_url = queue.pop(0)
        if base_url not in current_url or '#' in current_url:
            continue

        print(f"Crawling: {current_url}")

        markdown_data = html_to_markdown(current_url, domain, session, headers)
        
        if markdown_data:
            markdown_files.append(markdown_data)
        else:
            continue

        try:
            response = session.get(current_url, headers=headers, timeout=10)
            if response.status_code != 200:
                continue
                
            html_content = response.content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, 'html.parser')

            for element in soup.find_all('a', href=True):
                absolute_url = urljoin(current_url, element['href'])
                parsed_url = urlparse(absolute_url)
                if parsed_url.netloc == domain and absolute_url not in visited:
                    visited.add(absolute_url)
                    queue.append(absolute_url)
                    
        except Exception as e:
            print(f"Error processing HTML {current_url}: {str(e)}")

    return markdown_files


def get_knowledge_source(urls_with_refresh: Union[List[str], Dict[str, str]], ks_name: str):
    """Gets or creates a vector store from the given URLs"""
    if isinstance(urls_with_refresh, list):
        urls = urls_with_refresh
        refresh_times = {url: "1 day" for url in urls}
    else:
        urls = list(urls_with_refresh.keys())
        refresh_times = urls_with_refresh
    
    existing_id, is_mapping, urls_to_refresh = fetch_existing_vector_store(ks_name, urls, refresh_times)
    
    if existing_id and not urls_to_refresh:
        print(f"Found existing knowledge source with name '{ks_name}'. Using ID: {existing_id}")
        return existing_id, False
    
    url_files_map = {}
    refreshed_urls = []
    
    urls_to_scrape = urls_to_refresh if existing_id else urls
    
    for url in urls_to_scrape:
        print(f"Processing {url}...")
        markdown_data = download_static_website(url)
        if markdown_data:
            url_files = []
            for i, md_data in enumerate(markdown_data):
                filename = f"{md_data['path'].replace('/', '_')}.md" if md_data['path'] else f"content_{i}.md"
                
                file_obj = BytesIO(md_data['content'].encode('utf-8'))
                file_obj.name = filename
                url_files.append(file_obj)
            
            url_files_map[url] = url_files
            refreshed_urls.append(url)
    
    if not url_files_map and not existing_id:
        print("No content was scraped and no existing vector store found. Aborting.")
        return None, False
    
    if existing_id and not url_files_map:
        print(f"No new content to add to existing vector store. Using ID: {existing_id}")
        return existing_id, True
    
    if existing_id:
        update_existing_vector_store(
            vector_store_id=existing_id,
            url_files_map=url_files_map,
            refreshed_urls=refreshed_urls
        )
        print(f"Updated vector store with ID: {existing_id}")
        return existing_id, True
    else:
        vector_store_id = make_vector_store(
            url_files_map=url_files_map,
            vector_store_name=ks_name,
            source_refresh_times=refresh_times
        )
        print(f"Vector store created with ID: {vector_store_id}")
        return vector_store_id, True


if __name__ == "__main__":
    urls_to_test = {
        "https://www.rit.edu/computing/coms/": "1 minute",
        "https://www.rit.edu/liberalarts/expressive-communication-center": "1 week"
    }
    
    result = get_knowledge_source(urls_to_test, "rit_departments")
    print(result)
