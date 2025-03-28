import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import re
from markdownify import markdownify as md


OUTPUT_FOLDER = "output"
DEFAULT_URL = "https://www.rit.edu/policies"


def sanitize_path_part(part):
    """
    Replaces invalid characters in file paths with underscores
    """
    return re.sub(r'[\\/:*?"<>|]', '_', part).strip()


def get_local_path(url, domain, output_dir):
    """
    Converts URL to a path that can be saved as a local file path
    """
    parsed = urlparse(url)
    if parsed.netloc != domain:
        return None

    path = parsed.path.strip('/')
    path_parts = path.split('/') if path else []

    sanitized_path_parts = [sanitize_path_part(part) for part in path_parts]

    if parsed.path.endswith('/'):
        sanitized_path_parts.append('index.html')
    elif not path_parts:
        sanitized_path_parts.append('index.html')

    local_path = os.path.join(output_dir, *sanitized_path_parts)

    if '.' not in os.path.basename(local_path):
        local_path += '.html'

    return local_path


def remove_dir_safe(path):
    """
    Tries to remove a directory when needed (e.g. resource couldn't be saved so we don't need an empty folder)
    """
    try:
        os.rmdir(os.path.dirname(path))
        return True
    except OSError as rm_dir_failed:
        print(f"Failed to remove directory at path {path}: {str(rm_dir_failed)}")
        return False


def try_make_dir_safe(path):
    """
    Tries to make a directory, returning False if there was a problem
    """
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return True
    except Exception as e:
        print(f"Failed to make directory at path {path}: {str(e)}")
        return False


def save_resource_to_file(local_path, content_type, response, convert_html_to_markdown,
                          end_markdown_with_txt_extension):
    """
    Handles saving a file to a local
    """
    # handles markdown saving
    if convert_html_to_markdown and content_type == 'text/html':
        extension = '.md'
        if end_markdown_with_txt_extension:
            extension = '.txt'
        markdown_path = os.path.splitext(local_path)[0] + extension
        os.makedirs(os.path.dirname(markdown_path), exist_ok=True)

        with open(markdown_path, 'w', encoding='utf-8') as f:
            f.write(md(response.content))
        print(f"Saved html as markdown at: {markdown_path}")

        return response.content, content_type

    # handle html saving
    if content_type == 'text/html':
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"Temporarily saved: {local_path}")

        new_path = force_html_extension(local_path)
        if new_path != local_path:
            os.replace(local_path, new_path)
            print(f"Renamed to: {new_path}")

    else:  # handle any other data types (e.g. pdf, png, docx, etc.)
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"Saved: {local_path}")


def save_resource(url, domain, output_dir, session, headers, convert_html_to_markdown, end_markdown_with_txt_extension):
    """
    Handles the fetching of a resource at a given url
    Returns the content saved and the content type (e.g. html, text, pdf)
    """
    local_path = get_local_path(url, domain, output_dir)
    if not local_path:
        return None, None

    if not try_make_dir_safe(local_path):
        return None, None

    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        final_url = response.url
        if urlparse(final_url).netloc != domain:
            print(f"Skipping external redirect: {final_url}")
            remove_dir_safe(local_path)
            return None, None

        content_type = response.headers.get('Content-Type', '').split(';')[0]
        save_resource_to_file(local_path, content_type, response, convert_html_to_markdown,
                              end_markdown_with_txt_extension)

        return response.content, content_type

    except Exception as e:
        print(f"Failed to download {url}: {str(e)}")
        remove_dir_safe(local_path)
        return None, None


def force_html_extension(path):
    """
    Ensure HTML files are adjusted if their extension is broken
    (Mainly occurs when path ends in .com or is an email address)
    """
    if path.lower().endswith(('.html', '.htm')):
        return path

    new_path = f"{path}.html"

    # handle potential existing files
    counter = 1
    while os.path.exists(new_path):
        new_path = f"{path}_{counter}.html"
        counter += 1

    return new_path


def download_static_website(base_url, output_dir=OUTPUT_FOLDER, convert_html_to_markdown=False,
                            end_markdown_with_txt_extension=False):
    """
    Main program that crawls through all pages linked to the base_url, and
    saves all associated files and subpages
    """
    session = requests.Session()
    # Set the user agent to avoid being immediately blocked
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " 
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 "
                      "Safari/537.36"
    }

    parsed_base = urlparse(base_url)
    domain = parsed_base.netloc
    os.makedirs(output_dir, exist_ok=True)

    visited = set()
    queue = [base_url]
    visited.add(base_url)
    all_resources = set()

    # use bfs to crawl html pages and collect relevant resources
    while queue:
        current_url = queue.pop(0)
        if base_url not in current_url or '#' in current_url:
            continue

        print(f"Crawling at: {current_url}")

        content, content_type = save_resource(current_url, domain, output_dir, session, headers,
                                              convert_html_to_markdown, end_markdown_with_txt_extension)

        if content is None:
            continue

        all_resources.add(current_url)

        if content_type == 'text/html':
            try:
                html_content = content.decode('utf-8', errors='ignore')
            except UnicodeDecodeError:
                html_content = content.decode('latin-1', errors='ignore')

            soup = BeautifulSoup(html_content, 'html.parser')

            # all the relevant tags for types of content we might want to scrape
            tags = {
                'link': 'href',
                'script': 'src',
                'img': 'src',
                'source': 'src',
                'meta': 'content',
                'a': 'href'
            }

            # extract links to other html pages found on the current page

            for tag_name, attr in tags.items():
                for element in soup.find_all(tag_name, {attr: True}):
                    url = element[attr]
                    absolute_url = urljoin(current_url, url)
                    all_resources.add(absolute_url)

            for element in soup.find_all('a', href=True):
                url = element['href']
                absolute_url = urljoin(current_url, url)
                parsed_url = urlparse(absolute_url)
                if parsed_url.netloc == domain and absolute_url not in visited:
                    visited.add(absolute_url)
                    queue.append(absolute_url)

    # download any non-HTML resources (images, documents)
    resources_to_download = all_resources - visited
    for url in resources_to_download:
        if base_url not in url or "google_tag" in url or "Drupal" in url:
            continue
        save_resource(url, domain, output_dir, session, headers, convert_html_to_markdown)

    # process CSS urls to get any relevant embedded resources
    css_urls = [url for url in resources_to_download if url.lower().endswith('.css')]
    new_resources = set()

    for css_url in css_urls:
        try:
            response = session.get(css_url, headers=headers, timeout=10)
            if response.status_code == 200:
                css_content = response.text
                for match in re.findall(r'url\((["\']?)(.*?)\1\)', css_content):
                    css_resource_url = match[1]
                    absolute_css_url = urljoin(css_url, css_resource_url)
                    if absolute_css_url not in all_resources and absolute_css_url not in new_resources:
                        new_resources.add(absolute_css_url)
        except Exception as e:
            print(f"Error processing CSS {css_url}: {str(e)}")

    # download the resources found from CSS urls
    for url in new_resources:
        save_resource(url, domain, output_dir, session, headers, convert_html_to_markdown)


def get_user_url():
    """
    Prompt user for URL input when script is double-clicked
    """
    print("\n" + "=" * 50)
    print("RIT website downloading tool")
    print("=" * 50 + "\n")
    print(f"The default URL is: {DEFAULT_URL}")
    print("Press Enter to use that URL, or you can enter one yourself (use the full link, including https://)")
    user_input = input("Enter link here: ").strip()

    if user_input:
        if not user_input.startswith(('http://', 'https://')):
            print("URL must start with http or https://")
            return None
        return user_input
    return DEFAULT_URL


if __name__ == "__main__":
    try:
        url = get_user_url()
        if url:
            os.makedirs(OUTPUT_FOLDER, exist_ok=True)
            download_static_website(url, convert_html_to_markdown=True, end_markdown_with_txt_extension=True)
            print("Finished web scraping!!")
    except Exception as e:
        print(e)
        input()