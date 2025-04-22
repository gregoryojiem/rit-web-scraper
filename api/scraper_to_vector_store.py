import aiohttp
import asyncio
from urllib.parse import urljoin, urlparse
import re
import time
import cProfile
import pstats
import io
import socket
from markdownify import markdownify as md
from io import BytesIO
from typing import Dict, List, Union, Set, Tuple, Optional, Deque
import concurrent.futures
from collections import deque
import os
from api.vector_store_util import make_vector_store, update_existing_vector_store, fetch_existing_vector_store


def sanitize_path_part(part):
    return re.sub(r'[\\/:*?"<>|]', '_', part).strip()


def get_local_path(url, domain):
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


def is_processable_url(url: str) -> bool:
    extensions_to_skip = ['.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
                          '.zip', '.rar', '.mp3', '.mp4', '.avi', '.mov', '.jpg',
                          '.jpeg', '.png', '.gif', '.svg', '.csv', '.json', '.xml']

    parsed = urlparse(url)
    path = parsed.path.lower()

    _, ext = os.path.splitext(path)
    if ext in extensions_to_skip:
        print(f"Skipping file with extension {ext}: {url}")
        return False

    if '%link%' in url or not url.startswith('http'):
        return False

    return True


def pre_resolve_dns(hostname: str) -> Optional[str]:
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


async def async_html_to_markdown(url: str, domain: str, session: aiohttp.ClientSession, timeout: int = 15) -> Optional[
    Dict]:
    if not is_processable_url(url):
        return None

    try:
        async with session.get(url, timeout=timeout, allow_redirects=True) as response:
            if response.status != 200:
                print(f"Status {response.status} for {url}")
                return None

            final_url = str(response.url)
            if urlparse(final_url).netloc != domain:
                print(f"Skipping external redirect: {final_url}")
                return None

            content_type = response.headers.get('Content-Type', '').split(';')[0].lower()

            if 'text/html' not in content_type:
                print(f"Skipping non-HTML content ({content_type}): {url}")
                return None

            html_content = await response.text(errors='ignore')
            if not html_content or len(html_content) < 100:
                print(f"Skipping empty/small page: {url}")
                return None

            try:
                loop = asyncio.get_event_loop()
                markdown_content = await asyncio.wait_for(
                    loop.run_in_executor(None, md, html_content),
                    timeout=5
                )
            except asyncio.TimeoutError:
                print(f"Markdown conversion timeout for {url}")
                return None

            local_path = get_local_path(url, domain)
            if local_path:
                return {
                    'url': url,
                    'path': local_path,
                    'content': markdown_content
                }

            return None

    except asyncio.TimeoutError:
        print(f"Timeout downloading {url}")
        return None
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return None


async def extract_links(html_content: str, base_url: str, domain: str) -> List[str]:
    try:
        link_pattern = re.compile(r'<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
        all_links = link_pattern.findall(html_content)

        new_urls = set()
        for href in all_links:
            if not href or href.startswith(('javascript:', 'mailto:')) or href == '#':
                continue

            absolute_url = urljoin(base_url, href)

            if not is_processable_url(absolute_url):
                continue

            if (absolute_url.startswith(base_url) and
                    '#' not in absolute_url and
                    '?' not in absolute_url):
                new_urls.add(absolute_url)

        return list(new_urls)
    except Exception as e:
        print(f"Error extracting links: {str(e)}")
        return []


class URLProcessor:
    def __init__(self, base_url: str, max_pages: int, max_concurrent: int):
        self.base_url = base_url
        self.max_pages = max_pages
        self.max_concurrent = max_concurrent
        self.domain = urlparse(base_url).netloc

        self.ip_address = pre_resolve_dns(self.domain)
        if self.ip_address:
            print(f"Pre-resolved {self.domain} to {self.ip_address}")

        self.visited = set()
        self.pending = set()
        self.queue = deque([base_url])
        self.skipped = set()
        self.failed_urls = set()
        self.retry_queue = {}
        self.max_retries = 5

        self.markdown_files = []
        self.processed_count = 0
        self.success_count = 0
        self.actual_skipped_count = 0

        self.start_time = time.time()
        self.url_times = {}

        self.current_concurrency = max_concurrent
        self.success_rates = deque(maxlen=10)

        self.semaphore = asyncio.Semaphore(self.current_concurrency)

        self.session = None

    def adjust_concurrency(self, success_rate: float):
        self.success_rates.append(success_rate)

        if len(self.success_rates) < 5:
            return

        avg_success_rate = sum(self.success_rates) / len(self.success_rates)

        if avg_success_rate > 0.8 and self.current_concurrency < self.max_concurrent * 1.5:
            self.current_concurrency = min(int(self.current_concurrency * 1.2), int(self.max_concurrent * 1.5))
            print(f"Increasing concurrency to {self.current_concurrency}")
            self.semaphore = asyncio.Semaphore(self.current_concurrency)
        elif avg_success_rate < 0.5 and self.current_concurrency > 10:
            self.current_concurrency = max(int(self.current_concurrency * 0.8), 10)
            print(f"Decreasing concurrency to {self.current_concurrency}")
            self.semaphore = asyncio.Semaphore(self.current_concurrency)

    async def process_url(self, url: str) -> bool:
        url_start_time = time.time()

        async with self.semaphore:
            if url in self.visited or url in self.skipped or url in self.failed_urls:
                return False

            if not is_processable_url(url):
                self.skipped.add(url)
                self.actual_skipped_count += 1
                return False

            self.pending.add(url)
            print(f"Crawling: {url}")

            try:
                markdown_data = await async_html_to_markdown(url, self.domain, self.session, timeout=20)

                if markdown_data:
                    self.markdown_files.append(markdown_data)
                    self.success_count += 1

                    async with self.session.get(url, timeout=10) as response:
                        if response.status == 200:
                            html_content = await response.text(errors='ignore')
                            new_urls = await extract_links(html_content, self.base_url, self.domain)

                            for new_url in new_urls:
                                if (new_url not in self.visited and new_url not in self.pending and
                                        new_url not in self.skipped and new_url not in self.failed_urls and
                                        new_url not in self.queue):
                                    self.queue.append(new_url)

                    self.visited.add(url)
                    self.pending.remove(url)
                    self.url_times[url] = time.time() - url_start_time
                    return True
                else:
                    self.pending.remove(url)

                    if not is_processable_url(url):
                        self.skipped.add(url)
                        self.actual_skipped_count += 1
                        return False

                    retry_count = self.retry_queue.get(url, 0) + 1

                    if retry_count <= self.max_retries:
                        self.retry_queue[url] = retry_count
                        self.queue.append(url)
                    else:
                        self.failed_urls.add(url)
                        print(f"Failed after {retry_count} retries: {url}")

                    return False

            except Exception as e:
                if url in self.pending:
                    self.pending.remove(url)

                retry_count = self.retry_queue.get(url, 0) + 1
                if retry_count <= self.max_retries:
                    self.retry_queue[url] = retry_count
                    self.queue.append(url)
                else:
                    self.failed_urls.add(url)
                    print(f"Failed after {retry_count} retries: {url}")

                return False
            finally:
                if url not in self.url_times:
                    self.url_times[url] = time.time() - url_start_time

    async def process_batch(self, batch_size: int) -> Tuple[int, float]:
        batch = []
        batch_count = 0
        current_queue_size = len(self.queue)

        actual_batch_size = min(batch_size * 2, current_queue_size) if current_queue_size > 100 else batch_size

        while len(batch) < actual_batch_size and self.queue and batch_count < actual_batch_size * 2:
            batch_count += 1

            if not self.queue:
                break

            url = self.queue.popleft()
            if (url not in self.visited and url not in self.pending and
                    url not in self.skipped and url not in self.failed_urls):
                batch.append(url)

        if not batch:
            return 0, 1.0

        tasks = [asyncio.create_task(self.process_url(url)) for url in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful = sum(1 for r in results if r is True)
        self.processed_count += len(batch)

        success_rate = successful / len(batch) if batch else 1.0

        return successful, success_rate

    async def crawl(self) -> List[Dict]:
        tcp_connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,
            ttl_dns_cache=3600,
            ssl=False,
            force_close=False,
            enable_cleanup_closed=True,
        )

        timeout = aiohttp.ClientTimeout(
            total=30,
            connect=10,
            sock_connect=10,
            sock_read=15
        )

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0"
        }

        self.session = aiohttp.ClientSession(
            connector=tcp_connector,
            timeout=timeout,
            headers=headers,
            cookie_jar=aiohttp.CookieJar(),
            auto_decompress=True
        )

        try:
            last_status_time = time.time()
            no_progress_iterations = 0
            last_batch_time = time.time()

            while (self.queue or self.pending) and (self.processed_count < self.max_pages or self.pending):
                batch_size = min(self.current_concurrency, 10 + (self.processed_count // 20))
                batch_start = time.time()
                processed, success_rate = await self.process_batch(batch_size)
                batch_time = time.time() - batch_start

                self.adjust_concurrency(success_rate)

                current_time = time.time()
                if current_time - last_status_time > 2:
                    elapsed = current_time - self.start_time
                    pages_per_second = self.success_count / elapsed if elapsed > 0 else 0
                    batch_speed = processed / batch_time if batch_time > 0 else 0

                    print(f"Processed {self.processed_count} pages, found {self.success_count} valid pages, "
                          f"{len(self.queue)} in queue, {len(self.pending)} pending, {self.actual_skipped_count} skipped, "
                          f"{len(self.failed_urls)} failed, {pages_per_second:.2f} pages/sec, "
                          f"batch: {batch_speed:.2f} p/s, concurrency: {self.current_concurrency}")
                    last_status_time = current_time

                if processed > 0:
                    last_batch_time = time.time()
                    no_progress_iterations = 0
                else:
                    no_progress_iterations += 1

                stall_time = time.time() - last_batch_time
                if no_progress_iterations > 20 and not self.pending and stall_time > 30:
                    print(f"No progress made for {stall_time:.1f} seconds and no pending URLs. Breaking.")
                    break

                if processed == 0 and self.pending:
                    await asyncio.sleep(0.1)

            if self.queue:
                print(f"Processing remaining {len(self.queue)} URLs with extra retries")
                self.max_retries = 10

                max_final_iterations = 200
                iterations = 0

                batch_size = min(self.current_concurrency * 2, len(self.queue))

                while self.queue and iterations < max_final_iterations and self.processed_count < self.max_pages * 1.1:
                    processed, _ = await self.process_batch(batch_size)
                    iterations += 1

                    if iterations % 10 == 0:
                        print(f"Final pass: {len(self.queue)} URLs remaining")

                    if processed == 0 and iterations > 50:
                        print("No more progress in final pass. Stopping.")
                        break

            elapsed = time.time() - self.start_time
            pages_per_second = self.success_count / elapsed if elapsed > 0 else 0

            print(
                f"\nCompleted crawling {self.base_url}: processed {self.processed_count} pages, found {len(self.markdown_files)} markdown files")
            print(f"Skipped {self.actual_skipped_count} non-HTML files, failed to process {len(self.failed_urls)} URLs")
            print(f"Average processing speed: {pages_per_second:.2f} pages/second")

            if self.url_times:
                sorted_times = sorted([(url, time_taken) for url, time_taken in self.url_times.items()],
                                      key=lambda x: x[1], reverse=True)
                print("\nSlowest URLs processed:")
                for url, time_taken in sorted_times[:5]:
                    print(f"  {url}: {time_taken:.2f} seconds")

            if self.failed_urls:
                print("\nThe following URLs could not be processed (reached max retries):")
                sorted_failed = sorted(list(self.failed_urls))
                for i, url in enumerate(sorted_failed):
                    print(f"{i + 1}. {url}")

            if self.skipped:
                extension_groups = {}
                for url in self.skipped:
                    _, ext = os.path.splitext(urlparse(url).path.lower())
                    if ext not in extension_groups:
                        extension_groups[ext] = []
                    extension_groups[ext].append(url)

                print("\nSkipped URLs by file type:")
                for ext, urls in sorted(extension_groups.items()):
                    print(f"\n{ext or 'No extension'} ({len(urls)} files):")
                    for i, url in enumerate(urls[:5]):
                        print(f"  {i + 1}. {url}")
                    if len(urls) > 5:
                        print(f"  ... and {len(urls) - 5} more")

            return self.markdown_files
        finally:
            await self.session.close()


async def download_static_website_async(base_url: str, max_pages: int = 9000) -> List[Dict]:
    import multiprocessing
    max_concurrent = min(200, multiprocessing.cpu_count() * 16)

    processor = URLProcessor(base_url, max_pages, max_concurrent)
    return await processor.crawl()


def download_static_website(base_url: str, max_pages: int = 9000):
    try:
        profiler = cProfile.Profile()
        profiler.enable()

        start_time = time.time()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        result = loop.run_until_complete(download_static_website_async(base_url, max_pages))

        loop.close()

        profiler.disable()

        end_time = time.time()
        print(f"Crawling completed in {end_time - start_time:.2f} seconds")

        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
        ps.print_stats(15)
        print("\nProfiling Results (Top 15 operations):")
        print(s.getvalue())

        return result
    except Exception as e:
        print(f"Error in download_static_website for {base_url}: {str(e)}")
        return []


def get_knowledge_source(urls_with_refresh: Union[List[str], Dict[str, str]], ks_name: str):
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

    made_openai_calls = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(5, len(urls_to_scrape))) as executor:
        future_to_url = {
            executor.submit(download_static_website, url, 9000): url
            for url in urls_to_scrape
        }

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                markdown_data = future.result()
                if markdown_data:
                    url_files = []
                    for i, md_data in enumerate(markdown_data):
                        filename = f"{md_data['path'].replace('/', '_')}.md" if md_data['path'] else f"content_{i}.md"

                        file_obj = BytesIO(md_data['content'].encode('utf-8'))
                        file_obj.name = filename
                        url_files.append(file_obj)

                    url_files_map[url] = url_files
                    refreshed_urls.append(url)
                    print(f"Processed {url}: found {len(url_files)} files")
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")

    if not url_files_map and not existing_id:
        print("No content was scraped and no existing vector store found. Aborting.")
        return None, False

    if existing_id and not url_files_map:
        print(f"No new content to add to existing vector store. Using ID: {existing_id}")
        return existing_id, False

    made_openai_calls = True

    if existing_id:
        update_existing_vector_store(
            vector_store_id=existing_id,
            url_files_map=url_files_map,
            refreshed_urls=refreshed_urls
        )
        print(f"Updated vector store with ID: {existing_id}")
        return existing_id, made_openai_calls
    else:
        vector_store_id = make_vector_store(
            url_files_map=url_files_map,
            vector_store_name=ks_name,
            source_refresh_times=refresh_times
        )
        print(f"Vector store created with ID: {vector_store_id}")
        return vector_store_id, made_openai_calls


if __name__ == "__main__":
    urls_to_test = [
        "https://www.rit.edu/imagine/"
    ]

    result, made_calls = get_knowledge_source(urls_to_test, "rit_departments")
    print(f"Vector store ID: {result}, Made OpenAI calls: {made_calls}")
