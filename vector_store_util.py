import os
import csv
from datetime import datetime
from openai import OpenAI
import contextlib
import time

CLIENT = OpenAI()


def get_file_list(path):
    files = []
    try:
        for root, _, filenames in os.walk(path):
            for filename in filenames:
                if filename.endswith(".txt"):
                    files.append(os.path.join(root, filename))
        return files
    except FileNotFoundError:
        print(f"Error: Could not find '{path}'")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def get_or_make_vector_store(path: str, vector_store_name: str) -> str or None:
    vector_store_id = fetch_existing_vector_store(vector_store_name)
    if not vector_store_id:
        print("Creating new vector store")
        vector_store_id = vector_store_setup(path, vector_store_name)
    else:
        print("Creating new vector store")
    return vector_store_id


def vector_store_setup(path: str, vector_store_name: str) -> str or None:
    files = get_file_list(path)
    if not files:
        return None

    vector_store = CLIENT.beta.vector_stores.create(name=vector_store_name)
    vector_store_id = vector_store.id

    batch_size = 20
    file_batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

    for batch in file_batches:
        with contextlib.ExitStack() as stack:
            filestream = [stack.enter_context(open(path, "rb")) for path in batch]
            CLIENT.beta.vector_stores.file_batches.upload_and_poll(
                vector_store_id=vector_store_id, files=filestream
            )
            time.sleep(1)

    with open("knowledge/vector_store_data.csv", 'a', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"')
        writer.writerow([vector_store_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), vector_store_id])

    return vector_store_id


def fetch_existing_vector_store(vector_store_name: str) -> str or None:
    with open("knowledge/vector_store_data.csv", 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            if row['VectorStoreName'] == vector_store_name:
                return row['VectorStoreID']
    return None
