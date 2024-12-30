from datetime import datetime
from elasticsearch import Elasticsearch

def insert_data_elasticsearch(url, text, metadata):
    # Create a connection to Elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])  # Adjust host and port if needed

    # Define the index name
    index_name = 'website_data'

    # Prepare the document to be indexed
    document = {
        'url': url,
        'text': text,
        'title': metadata['title'],
        'description': metadata['description'],
        'keywords': metadata['keywords'],
        'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    # Index the document into Elasticsearch
    response = es.index(index=index_name, document=document)

    print(f"Data inserted into Elasticsearch for URL: {url}")
    print(f"Elasticsearch response: {response}")
