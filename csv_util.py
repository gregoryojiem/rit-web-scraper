import os
import csv
import json
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any, TextIO
from contextlib import contextmanager

CSV_FILE_PATH = "knowledge/vector_store_data.csv"
CSV_FIELDNAMES = ['VectorStoreName', 'CreatedAt', 'VectorStoreID', 'Sources', 'RefreshTimes', 'LastRefreshed',
                  'FileMapping']


@contextmanager
def csv_file_context(mode: str, operation_name: str = "CSV operation") -> TextIO:
    """Context manager for CSV file operations"""
    try:
        os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
        file = open(CSV_FILE_PATH, mode, encoding='utf-8', newline='')
        yield file
    except Exception as e:
        print(f"Error in {operation_name}: {str(e)}")
        raise
    finally:
        if 'file' in locals():
            file.close()


def read_csv() -> List[Dict[str, str]]:
    """Reads all data from the CSV file"""
    if not os.path.exists(CSV_FILE_PATH):
        return []

    try:
        with csv_file_context('r', "reading CSV") as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)
    except Exception as e:
        print(f"Error reading CSV: {str(e)}")
        return []


def write_csv(rows: List[Dict[str, str]], overwrite: bool = True) -> bool:
    """Writes data to the CSV file"""
    try:
        mode = 'w' if overwrite else 'a'
        with csv_file_context(mode, "writing CSV") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES, delimiter=',', quotechar='"')

            if overwrite or not os.path.exists(CSV_FILE_PATH) or os.path.getsize(CSV_FILE_PATH) == 0:
                writer.writeheader()

            for row in rows:
                writer.writerow(row)
        return True
    except Exception as e:
        print(f"Error writing CSV: {str(e)}")
        return False


def append_csv_row(row_data: List[Any]) -> bool:
    """Appends a single row to the CSV file"""
    try:
        with csv_file_context('a', "appending CSV") as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"')
            writer.writerow(row_data)
        return True
    except Exception as e:
        print(f"Error appending CSV row: {str(e)}")
        return False


def ensure_csv_exists() -> bool:
    """Creates vector store data CSV if needed"""
    if not os.path.exists(CSV_FILE_PATH):
        try:
            with csv_file_context('w', "creating CSV") as csvfile:
                writer = csv.writer(csvfile, delimiter=',', quotechar='"')
                writer.writerow(CSV_FIELDNAMES)
            return True
        except Exception as e:
            print(f"Error creating CSV: {str(e)}")
            return False
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
