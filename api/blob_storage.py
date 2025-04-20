"""
blob_storage.py
Utility for interacting with Vercel Blob Storage to store vector store mappings
"""
import csv
import json
import requests
import time
import vercel_blob
from io import StringIO
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any
from dotenv import load_dotenv

load_dotenv()

VECTOR_STORE_CSV = "vector_stores/vector_store_data.csv"
CSV_FIELDNAMES = ['VectorStoreName', 'CreatedAt', 'VectorStoreID', 'Sources', 'RefreshTimes', 'LastRefreshed',
                  'FileMapping']

DATA_CACHE = {
}


def get_from_cache(blob_name):
    """Get data from cache if it exists and is not expired"""
    if blob_name in DATA_CACHE:
        cache_entry = DATA_CACHE[blob_name]
        if time.time() - cache_entry['timestamp'] < cache_entry['ttl']:
            return cache_entry['data']
    return None


def add_to_cache(blob_name, data, ttl=300):
    """Add data to cache with a specified TTL (default 5 minutes)"""
    DATA_CACHE[blob_name] = {
        'data': data,
        'timestamp': time.time(),
        'ttl': ttl
    }


def clear_cache(blob_name=None):
    """Clear cache for a specific blob or all blobs"""
    if blob_name:
        if blob_name in DATA_CACHE:
            del DATA_CACHE[blob_name]
    else:
        DATA_CACHE.clear()


async def save_csv_to_blob(data, blob_name=VECTOR_STORE_CSV):
    """Save a list of dictionaries to Vercel Blob storage as CSV"""
    if data != [] and not data:
        return None

    try:
        blobs = vercel_blob.list()['blobs']
        matching_blobs = [d for d in blobs if d.get("pathname") == blob_name]

        for existing_blob in matching_blobs:
            vercel_blob.delete(existing_blob['url'])
            print(f"Deleted existing blob: {existing_blob['url']}")
    except Exception as e:
        print(f"Error checking/deleting existing blobs: {str(e)}")

    output = StringIO()
    fieldnames = CSV_FIELDNAMES
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for row in data:
        writer.writerow(row)

    csv_bytes = output.getvalue().encode('utf-8')
    response = vercel_blob.put(blob_name, csv_bytes, {"contentType": "text/csv"})

    add_to_cache(blob_name, data)

    return response.get('url')


async def load_csv_from_blob(blob_name=VECTOR_STORE_CSV):
    """Load a CSV from Vercel Blob storage as list of dictionaries"""
    cached_data = get_from_cache(blob_name)
    if cached_data:
        print(f"Using cached data for {blob_name}")
        return cached_data

    try:
        blobs = vercel_blob.list()['blobs']
        blob_info = next((d for d in blobs if d.get("pathname") == blob_name), None)

        if not blob_info:
            print(f"Blob not found: {blob_name}")
            return []

        url = blob_info.get('url')
        if not url:
            print(f"URL not found for blob: {blob_name}")
            return []

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=10)
                if response.status_code != 200:
                    print(f"HTTP error {response.status_code} for {url}, attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        continue
                    return []

                if not response.text:
                    print(f"Empty response from {url}, attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        continue
                    return []

                csv_data = StringIO(response.text)
                reader = csv.DictReader(csv_data)
                result = [row for row in reader]

                if not result:
                    print(f"Empty CSV data from {url}, attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        continue

                print(f"Successfully loaded {len(result)} rows from {blob_name}")

                add_to_cache(blob_name, result)

                return result

            except Exception as e:
                print(f"Error loading CSV from {url}, attempt {attempt + 1}/{max_retries}: {str(e)}")
                if attempt < max_retries - 1:
                    continue

        return []
    except Exception as e:
        print(f"Unexpected error in load_csv_from_blob for {blob_name}: {str(e)}")
        return []


def read_csv() -> List[Dict[str, str]]:
    """Reads all data from the CSV file"""
    import asyncio

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        result = loop.run_until_complete(load_csv_from_blob(VECTOR_STORE_CSV))
        loop.close()

        return result
    except Exception as e:
        print(f"Error reading CSV: {str(e)}")
        return []


def write_csv(rows: List[Dict[str, str]], overwrite: bool = True) -> bool:
    """Writes data to the CSV file"""
    import asyncio

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        result = loop.run_until_complete(save_csv_to_blob(rows, VECTOR_STORE_CSV))
        loop.close()

        return True if result else False
    except Exception as e:
        print(f"Error writing CSV: {str(e)}")
        return False


def append_csv_row(row_data: List[Any]) -> bool:
    """Appends a single row to the CSV file"""
    try:
        row_dict = {}
        for i, field in enumerate(CSV_FIELDNAMES):
            if i < len(row_data):
                row_dict[field] = row_data[i]
            else:
                row_dict[field] = ""

        existing_data = read_csv()

        existing_data.append(row_dict)

        return write_csv(existing_data)
    except Exception as e:
        print(f"Error appending CSV row: {str(e)}")
        return False


async def get_or_create_vector_store_csv():
    """Get the vector store CSV or create it if it doesn't exist"""
    cached_data = get_from_cache(VECTOR_STORE_CSV)
    if cached_data:
        print(f"Using cached vector store CSV data")
        return cached_data

    try:
        blobs = vercel_blob.list()['blobs']
        blob_exists = any(d.get("pathname") == VECTOR_STORE_CSV for d in blobs)

        if blob_exists:
            print(f"Vector store CSV exists, loading it")
            vector_store_data = await load_csv_from_blob(VECTOR_STORE_CSV)
            return vector_store_data
        else:
            print(f"Vector store CSV doesn't exist, creating it")
            vector_store_data = []
            await save_csv_to_blob(vector_store_data, VECTOR_STORE_CSV)
            return vector_store_data
    except Exception as e:
        print(f"Error in get_or_create_vector_store_csv: {str(e)}")
        return []


def ensure_csv_exists() -> bool:
    """Creates vector store data CSV if needed"""
    import asyncio

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        result = loop.run_until_complete(get_or_create_vector_store_csv())
        loop.close()

    except Exception as e:
        print(f"Error ensuring CSV exists: {str(e)}")
        return False


def parse_refresh_data(row: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    """Helper function to parse refresh data from a CSV row"""
    refresh_data = {}
    sources = row['Sources'].split('|') if row['Sources'] else []
    refresh_times = row['RefreshTimes'].split('|') if row.get('RefreshTimes') else []
    last_refreshed = row['LastRefreshed'].split('|') if row.get('LastRefreshed') else []

    for i, source in enumerate(sources):
        refresh_time = refresh_times[i] if i < len(refresh_times) else "1 day"
        last_refresh = last_refreshed[i] if i < len(last_refreshed) else "2000-01-01 00:00:00"
        refresh_data[source] = {
            'refresh_time': refresh_time,
            'last_refreshed': last_refresh
        }

    return refresh_data


def parse_file_mapping(row: Dict[str, str]) -> Dict:
    """Helper function to parse file mapping from a CSV row"""
    file_mapping = {}
    if 'FileMapping' in row and row['FileMapping']:
        try:
            file_mapping = json.loads(row['FileMapping'])
        except json.JSONDecodeError:
            print(f"Error parsing file mapping JSON: {row['FileMapping'][:50]}...")
    return file_mapping


def find_vector_store_by_name(vector_store_name: str) -> Tuple[Optional[str], Dict, Dict]:
    """Finds vector store by name and returns ID, refresh data and file map"""
    rows = read_csv()

    for row in rows:
        if row['VectorStoreName'] == vector_store_name:
            vector_store_id = row['VectorStoreID']
            refresh_data = parse_refresh_data(row)
            file_mapping = parse_file_mapping(row)
            return vector_store_id, refresh_data, file_mapping

    return None, {}, {}


def find_vector_store_by_id(vector_store_id: str) -> Tuple[Optional[str], Dict, Dict]:
    """Finds vector store by ID and returns name, refresh data and file map"""
    rows = read_csv()

    for row in rows:
        if row['VectorStoreID'] == vector_store_id:
            vector_store_name = row['VectorStoreName']
            refresh_data = parse_refresh_data(row)
            file_mapping = parse_file_mapping(row)
            return vector_store_name, refresh_data, file_mapping

    return None, {}, {}


def find_vector_store_by_sources(sources: List[str]) -> Tuple[Optional[str], Dict, Dict]:
    """Finds vector store by source URLs and returns ID, refresh data and file map"""
    if not sources:
        return None, {}, {}

    rows = read_csv()
    sources_set = set(sources)

    for row in rows:
        row_sources = row.get('Sources', '')
        if row_sources:
            row_sources_set = set(row_sources.split('|'))
            if row_sources_set == sources_set:
                vector_store_id = row['VectorStoreID']
                refresh_data = parse_refresh_data(row)
                file_mapping = parse_file_mapping(row)
                return vector_store_id, refresh_data, file_mapping

    return None, {}, {}


def add_vector_store_mapping(vector_store_name: str, vector_store_id: str,
                             sources: List[str], refresh_data: Dict,
                             file_mapping: Dict = None,
                             source_refresh_times: Dict[str, str] = None,
                             urls_to_refresh: List[str] = None) -> None:
    """Adds a new name mapping for an existing vector store"""
    try:
        now = datetime.now()
        refresh_times_list = []
        last_refreshed_list = []

        for source in sources:
            if source in source_refresh_times:
                refresh_times_list.append(source_refresh_times[source])
            elif source in refresh_data:
                refresh_times_list.append(refresh_data[source]['refresh_time'])
            else:
                refresh_times_list.append("1 day")

            if urls_to_refresh and source in urls_to_refresh:
                last_refreshed_list.append(now.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                last_refreshed_list.append(
                    refresh_data.get(source, {}).get('last_refreshed', now.strftime('%Y-%m-%d %H:%M:%S')))

        refresh_times_str = "|".join(refresh_times_list)
        last_refreshed_str = "|".join(last_refreshed_list)
        sources_str = "|".join(sources)

        file_mapping_str = json.dumps(file_mapping) if file_mapping else ""

        row_data = [vector_store_name, now.strftime('%Y-%m-%d %H:%M:%S'),
                    vector_store_id, sources_str, refresh_times_str, last_refreshed_str, file_mapping_str]

        append_csv_row(row_data)
        print(f"Added new mapping for existing vector store: {vector_store_name} -> {vector_store_id}")
    except Exception as e:
        print(f"Error adding vector store mapping: {str(e)}")


def update_csv_with_file_mapping(vector_store_id: str, file_mapping: Dict) -> None:
    """Updates the CSV with new file mappings"""
    try:
        file_mapping_str = json.dumps(file_mapping)
        rows = read_csv()
        updated_rows = []

        for row in rows:
            if row['VectorStoreID'] == vector_store_id:
                row['FileMapping'] = file_mapping_str
            updated_rows.append(row)

        write_csv(updated_rows)
    except Exception as e:
        print(f"Error updating CSV with file mapping: {str(e)}")


def update_url_refresh_times(vector_store_id: str, refreshed_urls: List[str]) -> None:
    """Updates the last refresh timestamps for URLs"""
    if not refreshed_urls:
        return

    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows = read_csv()
        updated_rows = []

        for row in rows:
            for field in CSV_FIELDNAMES:
                if field not in row:
                    row[field] = ''

            if row['VectorStoreID'] == vector_store_id:
                sources = row['Sources'].split('|') if row['Sources'] else []
                refresh_times = row['RefreshTimes'].split('|') if row.get('RefreshTimes') else []
                last_refreshed = row['LastRefreshed'].split('|') if row.get('LastRefreshed') else []

                while len(refresh_times) < len(sources):
                    refresh_times.append("1 day")
                while len(last_refreshed) < len(sources):
                    last_refreshed.append("2000-01-01 00:00:00")

                for i, source in enumerate(sources):
                    if source in refreshed_urls:
                        last_refreshed[i] = now

                row['RefreshTimes'] = "|".join(refresh_times)
                row['LastRefreshed'] = "|".join(last_refreshed)

            updated_rows.append(row)

        write_csv(updated_rows)
    except Exception as e:
        print(f"Error updating URL refresh times: {str(e)}")


def add_new_vector_store(vector_store_name: str, vector_store_id: str,
                         sources: List[str], file_mapping: Dict = None,
                         source_refresh_times: Dict[str, str] = None) -> None:
    """Adds a new vector store entry to the CSV"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sources_str = "|".join(sources) if sources else ""

        if sources and source_refresh_times:
            refresh_times = []
            last_refreshed = []

            for source in sources:
                refresh_times.append(source_refresh_times.get(source, "1 day"))
                last_refreshed.append(now)

            refresh_times_str = "|".join(refresh_times)
            last_refreshed_str = "|".join(last_refreshed)
        else:
            refresh_times_str = "|".join(["1 day"] * len(sources)) if sources else ""
            last_refreshed_str = "|".join([now] * len(sources)) if sources else ""

        file_mapping_str = json.dumps(file_mapping) if file_mapping else ""

        row_data = [vector_store_name, now, vector_store_id, sources_str,
                    refresh_times_str, last_refreshed_str, file_mapping_str]

        append_csv_row(row_data)
        print(f"Added new vector store: {vector_store_name} -> {vector_store_id}")
    except Exception as e:
        print(f"Error adding new vector store: {str(e)}")


async def list_blobs(prefix=""):
    """List all blobs with an optional prefix filter"""
    try:
        blobs = vercel_blob.list()['blobs']
        if prefix:
            return [b for b in blobs if b.get("pathname", "").startswith(prefix)]
        return blobs
    except Exception as e:
        print(f"Error listing blobs: {str(e)}")
        return []


async def delete_blob(blob_name):
    """Delete a blob by name"""
    try:
        blobs = vercel_blob.list()['blobs']
        matching_blobs = [d for d in blobs if d.get("pathname") == blob_name]

        for existing_blob in matching_blobs:
            vercel_blob.delete(existing_blob['url'])
            print(f"Deleted blob: {existing_blob['pathname']}")
            clear_cache(blob_name)
            return True

        print(f"No blob found with name: {blob_name}")
        return False
    except Exception as e:
        print(f"Error deleting blob: {str(e)}")
        return False
