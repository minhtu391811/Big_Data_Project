from transformers import BertTokenizer, BertModel
import torch
from hbase import HBase
import numpy as np

# Initialize BERT model and tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')

# Create a connection to HBase
connection = HBase('localhost', 9090)
table = connection.table('embeddings_table')

# Sample text data
text_data = "Example article text"

# Tokenize the text
inputs = tokenizer(text_data, return_tensors='pt', truncation=True, padding=True, max_length=512)

# Get the embeddings (using the last hidden state)
with torch.no_grad():
    outputs = model(**inputs)
    last_hidden_states = outputs.last_hidden_state

# Get the mean of the last hidden states to obtain a fixed-size embedding
embedding = last_hidden_states.mean(dim=1).squeeze().numpy()

# Convert embedding to bytes for storage in HBase
embedding_bytes = embedding.tobytes()

# Insert data with embedding into HBase
row_key = "some_unique_id"
table.put(row_key, {
    'info:text': text_data,
    'info:embedding': embedding_bytes
})

connection.close()

# Retrieve embedding from HBase and compare it with new query embeddings for similarity
row_key = "some_unique_id"
data = table.row(row_key)
stored_embedding = data[b'info:embedding']

# Convert stored bytes back to a numpy array
stored_embedding_array = np.frombuffer(stored_embedding, dtype=np.float32)

# Query to compare
query = "New query to compare"
query_inputs = tokenizer(query, return_tensors='pt', truncation=True, padding=True, max_length=512)

# Get query embeddings from BERT
with torch.no_grad():
    query_outputs = model(**query_inputs)
    query_last_hidden_states = query_outputs.last_hidden_state

# Get the mean of the last hidden states for the query embedding
query_embedding = query_last_hidden_states.mean(dim=1).squeeze().numpy()

# Calculate cosine similarity between query embedding and stored embedding
similarity = np.dot(query_embedding, stored_embedding_array) / (np.linalg.norm(query_embedding) * np.linalg.norm(stored_embedding_array))
print("Similarity:", similarity)