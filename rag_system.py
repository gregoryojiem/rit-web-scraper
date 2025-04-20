import os
from vectordb import Memory

FOLDER_LOCATION = "output"


def load_data(memory):
    for entry in os.listdir(FOLDER_LOCATION):
        subdir_path = os.path.join(FOLDER_LOCATION, entry)
        if os.path.isdir(subdir_path):
            for file in os.listdir(subdir_path):
                if file.endswith(".txt"):
                    file_path = os.path.join(subdir_path, file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        text = f.read()
                    metadata = {"document_path": file_path}
                    memory.save([text], [metadata])


def search_documents(memory, query, top_x):
    results = memory.search(query, top_n=top_x)
    seen_docs = set()
    unique_results = []

    for result in results:
        doc_path = result["metadata"]["document_path"]
        if doc_path not in seen_docs:
            seen_docs.add(doc_path)
            unique_results.append({
                "document": doc_path,
                "distance": result["distance"]
            })
        if len(unique_results) >= top_x:
            break

    return unique_results[:top_x]


def search_sections(memory, doc_path, query, top_x):
    all_results = memory.search(query, top_n=100)
    doc_sections = [
        r for r in all_results
        if r["metadata"]["document_path"] == doc_path
    ]
    return doc_sections[:top_x]


def main():
    memory = Memory(
        chunking_strategy={"mode": "paragraph"},
        embeddings="BAAI/bge-small-en-v1.5"
    )

    load_data(memory)

    # possible HR query
    query = "What is the policy on employee disputes?"

    # get top 5 documens
    print("Searching for top documents...")
    top_docs = search_documents(memory, query, 5)
    print("\nTop 5 documents:")
    for i, doc in enumerate(top_docs, 1):
        print(f"{i}. {doc['document']} (distance: {doc['distance']:.2f})")

    if top_docs:
        first_doc_path = top_docs[0]["document"]
        print(f"\nSearching for top sections in: {first_doc_path}")
        top_sections = search_sections(memory, first_doc_path, query, 5)

        print("\nTop 5 sections:")
        for i, section in enumerate(top_sections, 1):
            print(f"{i}. [Distance: {section['distance']:.2f}]")
            print(section["chunk"])
            print("---")


if __name__ == "__main__":
    main()