import os
from datetime import datetime, timedelta
from openai import OpenAI
import contextlib
import time
from typing import List, Union, BinaryIO, Tuple, Dict, Optional
from io import BytesIO
from dotenv import load_dotenv
import csv_util

load_dotenv()

CLIENT = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def parse_refresh_time(refresh_str):
    """
    Parse a refresh time string into a timedelta object.
    Examples: "1 hour", "2 days", "1 week", "30 minutes"
    """
    parts = refresh_str.strip().split()
    if len(parts) != 2:
        return timedelta(days=1)

    try:
        value = int(parts[0])
        unit = parts[1].lower()

        if unit.endswith('s'):
            unit = unit[:-1]

        if unit == 'minute':
            return timedelta(minutes=value)
        elif unit == 'hour':
            return timedelta(hours=value)
        elif unit == 'day':
            return timedelta(days=value)
        elif unit == 'week':
            return timedelta(weeks=value)
        elif unit == 'month':
            return timedelta(days=value * 30)
        elif unit == 'year':
            return timedelta(days=value * 365)
        else:
            return timedelta(days=1)
    except ValueError:
        return timedelta(days=1)


def determine_urls_to_refresh(sources: List[str], refresh_data: Dict) -> List[str]:
    """
    Determine which URLs need to be refreshed based on their refresh times.

    Args:
        sources: List of source URLs
        refresh_data: Dictionary with refresh information for each URL

    Returns:
        List of URLs that need to be refreshed
    """
    urls_to_refresh = []
    now = datetime.now()

    for source in sources:
        if source in refresh_data:
            refresh_time_str = refresh_data[source]['refresh_time']
            last_refreshed_str = refresh_data[source]['last_refreshed']

            try:
                last_refreshed = datetime.strptime(last_refreshed_str, '%Y-%m-%d %H:%M:%S')
                refresh_delta = parse_refresh_time(refresh_time_str)

                if now - last_refreshed > refresh_delta:
                    urls_to_refresh.append(source)
            except (ValueError, KeyError, TypeError) as e:
                print(f"Error parsing refresh data for {source}: {str(e)}")
                urls_to_refresh.append(source)
        else:
            urls_to_refresh.append(source)

    return urls_to_refresh


def delete_vector_store_files(vector_store_id: str, file_ids: List[str]) -> None:
    """
    Delete specific files from a vector store.

    Args:
        vector_store_id: ID of the vector store
        file_ids: List of file IDs to delete
    """
    try:
        for file_id in file_ids:
            try:
                CLIENT.beta.vector_stores.files.delete(
                    vector_store_id=vector_store_id,
                    file_id=file_id
                )
                CLIENT.files.delete(file_id=file_id)

                print(f"Deleted file {file_id} from vector store {vector_store_id}")
            except Exception as e:
                print(f"Error deleting file {file_id}: {str(e)}")
    except Exception as e:
        print(f"Error deleting vector store files: {str(e)}")


def add_files_to_vector_store(vector_store_id: str,
                              url_files_map: Dict[str, List[Union[str, BytesIO, BinaryIO]]]) -> Dict[str, List[str]]:
    """
    Add files to a vector store, organized by URL.

    Args:
        vector_store_id: ID of the vector store
        url_files_map: Dictionary mapping URLs to their respective files

    Returns:
        Dictionary mapping URLs to lists of their file IDs
    """
    file_mapping = {}

    try:
        for url, files in url_files_map.items():
            if not files:
                continue

            print(f"Uploading {len(files)} files for URL: {url}")
            url_file_ids = []

            batch_size = 20
            file_batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

            for batch in file_batches:
                with contextlib.ExitStack() as stack:
                    filestream = []
                    for file_item in batch:
                        if isinstance(file_item, str):
                            filestream.append(stack.enter_context(open(file_item, "rb")))
                        else:
                            filestream.append(file_item)

                    file_batch = CLIENT.beta.vector_stores.file_batches.upload_and_poll(
                        vector_store_id=vector_store_id,
                        files=filestream
                    )

                    vs_files = CLIENT.beta.vector_stores.file_batches.list_files(
                        batch_id=file_batch.id, vector_store_id=file_batch.vector_store_id)

                    for file in vs_files:
                        if hasattr(file, 'id'):
                            url_file_ids.append(file.id)

                    time.sleep(1)

            file_mapping[url] = url_file_ids
            print(f"Added {len(url_file_ids)} files for URL: {url}")

        return file_mapping

    except Exception as e:
        print(f"Error adding files to vector store: {str(e)}")
        return file_mapping


def update_vector_store_for_urls(vector_store_id: str, refreshed_urls: List[str],
                                 url_files_map: Dict[str, List[Union[str, BytesIO, BinaryIO]]],
                                 existing_file_mapping: Dict) -> Dict:
    """
    Update a vector store by removing files from URLs that need refreshing and adding new ones.
    Uses the file mapping to directly identify and delete files associated with each URL.

    Args:
        vector_store_id: ID of the vector store
        refreshed_urls: List of URLs that were refreshed
        url_files_map: Dictionary mapping URLs to their respective files
        existing_file_mapping: Current mapping of URLs to file IDs

    Returns:
        Updated file_mapping dictionary
    """
    if not refreshed_urls:
        return existing_file_mapping

    file_mapping = existing_file_mapping.copy() if existing_file_mapping else {}

    try:
        for url in refreshed_urls:
            file_ids_to_delete = file_mapping.get(url, [])

            if file_ids_to_delete:
                print(f"Found {len(file_ids_to_delete)} files to delete for URL: {url}")
                delete_vector_store_files(vector_store_id, file_ids_to_delete)
            else:
                print(f"No existing files found for URL: {url}")

            file_mapping[url] = []

        refreshed_url_files = {url: url_files_map.get(url, []) for url in refreshed_urls if url in url_files_map}

        updated_mapping = add_files_to_vector_store(vector_store_id, refreshed_url_files)

        for url, file_ids in updated_mapping.items():
            file_mapping[url] = file_ids

        return file_mapping

    except Exception as e:
        print(f"Error updating vector store: {str(e)}")
        return file_mapping


def fetch_existing_vector_store(vector_store_name: str, sources: List[str] = None,
                                source_refresh_times: Dict[str, str] = None) -> Tuple[Optional[str], bool, List[str]]:
    """
    Checks if a vector store exists with the given name or sources.
    Also determines which URLs need to be refreshed based on their refresh times.

    Returns:
        Tuple containing:
        - vector_store_id or None
        - boolean indicating if this is a new mapping for an existing vector store
        - list of sources (URLs) that need to be refreshed
    """
    if csv_util.ensure_csv_exists() and sources:
        return None, False, sources

    vector_store_id_by_name, \
        refresh_data_by_name, \
        file_mapping_by_name = csv_util.find_vector_store_by_name(vector_store_name)

    if vector_store_id_by_name and sources:
        urls_to_refresh = determine_urls_to_refresh(sources, refresh_data_by_name)
        return vector_store_id_by_name, False, urls_to_refresh

    if sources and not vector_store_id_by_name:
        vector_store_id_by_sources, \
            refresh_data_by_sources, \
            file_mapping_by_sources = csv_util.find_vector_store_by_sources(sources)

        if vector_store_id_by_sources:
            urls_to_refresh = determine_urls_to_refresh(sources, refresh_data_by_sources)

            csv_util.add_vector_store_mapping(
                vector_store_name,
                vector_store_id_by_sources,
                sources,
                refresh_data_by_sources,
                file_mapping_by_sources,
                source_refresh_times,
                urls_to_refresh
            )

            return vector_store_id_by_sources, True, urls_to_refresh

    return vector_store_id_by_name, False, sources if sources else []


def make_vector_store(url_files_map: Dict[str, List[Union[str, BytesIO, BinaryIO]]],
                      vector_store_name: str,
                      source_refresh_times: Dict[str, str] = None) -> Optional[str]:
    """
    Creates a new vector store and adds files to it
    
    Args:
        url_files_map: Dictionary mapping URLs to their respective files
        vector_store_name: Name of the vector store
        source_refresh_times: Dictionary mapping URLs to refresh time strings (e.g. "1 hour", "1 day")
    
    Returns:
        Vector store ID or None if creation failed
    """
    sources = list(url_files_map.keys())

    print("Creating new vector store")
    try:
        vector_store = CLIENT.beta.vector_stores.create(name=vector_store_name)
        vector_store_id = vector_store.id

        file_mapping = add_files_to_vector_store(vector_store_id, url_files_map)

        csv_util.add_new_vector_store(
            vector_store_name,
            vector_store_id,
            sources,
            file_mapping,
            source_refresh_times
        )

        return vector_store_id

    except Exception as e:
        print(f"Error creating vector store: {str(e)}")
        return None


def update_existing_vector_store(vector_store_id: str,
                                 url_files_map: Dict[str, List[Union[str, BytesIO, BinaryIO]]],
                                 refreshed_urls: List[str] = None) -> None:
    """
    Updates an existing vector store with new files for the refreshed URLs
    
    Args:
        vector_store_id: ID of the existing vector store
        url_files_map: Dictionary mapping URLs to their respective files
        refreshed_urls: List of URLs that have been refreshed
    """
    if not refreshed_urls:
        return

    _, _, file_mapping = csv_util.find_vector_store_by_id(vector_store_id)

    updated_file_mapping = update_vector_store_for_urls(
        vector_store_id,
        refreshed_urls,
        url_files_map,
        file_mapping
    )

    csv_util.update_csv_with_file_mapping(vector_store_id, updated_file_mapping)
    csv_util.update_url_refresh_times(vector_store_id, refreshed_urls)
