from transformers import GPT2LMHeadModel, GPT2Tokenizer
from elasticsearch import Elasticsearch, helpers
import torch
from text_to_doc import get_doc_chunks
from Lambda.web_crawler import get_data_from_website

# Initialize the GPT-2 model and tokenizer
model_name = "gpt2"  # Use the GPT-2 model
model = GPT2LMHeadModel.from_pretrained(model_name)
tokenizer = GPT2Tokenizer.from_pretrained(model_name)

# Elasticsearch client initialization
es = Elasticsearch("http://localhost:9200")  # Adjust the URL if needed

def get_es_index_name():
    """
    Returns the name of the Elasticsearch index.
    """
    return "website_data"

def create_es_index():
    """
    Creates an index in Elasticsearch for storing the vector data if it does not exist.
    """
    index_name = get_es_index_name()
    if not es.indices.exists(index=index_name):
        es.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "text": {"type": "text"},
                        "metadata": {"type": "object"},
                        "vector": {"type": "dense_vector", "dims": 768}  # Example vector size for GPT-2
                    }
                }
            }
        )

def store_docs(url):
    """
    Retrieves data from a website, processes it into document chunks, and stores them in Elasticsearch.

    Args:
        url (str): The URL of the website to retrieve data from.

    Returns:
        None
    """
    text, metadata = get_data_from_website(url)
    docs = get_doc_chunks(text, metadata)

    create_es_index()  # Ensure the Elasticsearch index exists

    actions = []
    
    for doc in docs:
        # Compute the embedding for the document (for example, using GPT-2 embeddings)
        inputs = tokenizer(doc.page_content, return_tensors="pt", padding=True, truncation=True)
        with torch.no_grad():
            outputs = model(**inputs, output_hidden_states=True)
        embeddings = outputs.hidden_states[-1][:, 0, :].numpy()  # Using the last hidden state (CLS token)

        # Prepare the document for Elasticsearch
        action = {
            "_op_type": "index",  # Define the operation (index in this case)
            "_index": get_es_index_name(),
            "_source": {
                "text": doc.page_content,
                "metadata": doc.metadata,
                "vector": embeddings[0].tolist()  # Convert to list for Elasticsearch
            }
        }
        actions.append(action)
    
    # Use bulk API for efficient document insertion
    try:
        helpers.bulk(es, actions)  # Bulk insert documents into Elasticsearch
        print(f"Successfully indexed {len(actions)} documents.")
    except Exception as e:
        print(f"Error occurred during bulk indexing: {e}")
