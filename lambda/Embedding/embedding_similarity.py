import numpy as np
import torch
from transformers import BertTokenizer, BertModel
from hbase import HBase

# Load BERT model and tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')

# Establish connection to HBase
def get_embedding_from_hbase(row_key):
    connection = HBase('localhost', 9090)  # Update with correct HBase connection settings
    table = connection.table('embeddings_table')

    try:
        # Retrieve row from HBase
        data = table.row(row_key)
        stored_embedding = data[b'info:embedding']

        # Convert stored bytes back to numpy array
        stored_embedding_array = np.frombuffer(stored_embedding, dtype=np.float32)
        return stored_embedding_array

    except Exception as e:
        print(f"Error retrieving embedding: {e}")
        return None

    finally:
        connection.close()

# Compare query with stored embedding using cosine similarity
def compare_embeddings(query, row_key="some_unique_id"):
    # Retrieve stored embedding from HBase
    stored_embedding_array = get_embedding_from_hbase(row_key)
    if stored_embedding_array is None:
        print("Failed to retrieve stored embedding.")
        return

    # Tokenize the query
    query_inputs = tokenizer(query, return_tensors='pt', truncation=True, padding=True, max_length=512)

    # Get query embeddings from BERT
    with torch.no_grad():
        query_outputs = model(**query_inputs)
        query_last_hidden_states = query_outputs.last_hidden_state

    # Compute the mean of the last hidden states to represent the query embedding
    query_embedding = query_last_hidden_states.mean(dim=1).squeeze().numpy()

    # Calculate cosine similarity between query embedding and stored embedding
    similarity = np.dot(query_embedding, stored_embedding_array) / (np.linalg.norm(query_embedding) * np.linalg.norm(stored_embedding_array))
    print("Similarity:", similarity)
    return similarity

# Example usage
if __name__ == "__main__":
    query = "New query to compare"
    compare_embeddings(query)
