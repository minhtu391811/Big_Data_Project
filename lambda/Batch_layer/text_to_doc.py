import re
from hdfs import InsecureClient

# Data Cleaning functions
def merge_hyphenated_words(text):
    return re.sub(r"(\w)-\n(\w)", r"\1\2", text)


def fix_newlines(text):
    return re.sub(r"(?<!\n)\n(?!\n)", " ", text)


def remove_multiple_newlines(text):
    return re.sub(r"\n{2,}", "\n", text)


def clean_text(text):
    """
    Cleans the text by passing it through a list of cleaning functions.
    """
    cleaning_functions = [merge_hyphenated_words, fix_newlines, remove_multiple_newlines]
    for cleaning_function in cleaning_functions:
        text = cleaning_function(text)
    return text


def split_text_into_chunks(text, chunk_size=2048, chunk_overlap=128):
    """
    Splits text into smaller chunks with overlap.
    """
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunk = text[start:end]
        chunks.append(chunk)
        start = end - chunk_overlap  # overlap between chunks
    return chunks


def text_to_doc(text, metadata):
    """
    Converts input text to a list of Documents with metadata.
    """
    doc_chunks = []
    chunks = split_text_into_chunks(text)  # Split text into chunks
    for chunk in chunks:
        doc = {
            "page_content": chunk,
            "metadata": metadata
        }
        doc_chunks.append(doc)
    return doc_chunks


# Lấy dữ liệu từ HDFS
hdfs_client = InsecureClient('http://namenode_host:9870')  # Thay đổi tên host phù hợp

def read_file_from_hdfs(file_path):
    with hdfs_client.read(file_path, encoding='utf-8') as reader:
        return reader.read()


def get_doc_chunks(file_path, metadata):
    """
    Quy trình lấy dữ liệu từ HDFS, làm sạch và tạo document chunks.
    """
    raw_text = read_file_from_hdfs(file_path)
    cleaned_text = clean_text(raw_text)
    doc_chunks = text_to_doc(cleaned_text, metadata)
    return doc_chunks
