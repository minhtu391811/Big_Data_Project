from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ElasticsearchQuery")

# Khởi tạo kết nối đến Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],  # Địa chỉ của Elasticsearch
    # basic_auth=("username", "password"),  # Nếu có xác thực
)

# Kiểm tra kết nối
if not es.ping():
    raise ValueError("Không thể kết nối đến Elasticsearch")

# Định nghĩa chỉ số và tài liệu cần truy vấn
index_name = "web-crawl"
document_id = "your_document_id_here"  # Thay thế bằng ID thực tế

# Định nghĩa truy vấn
query = {
    "query": {
        "term": {
            "_id": document_id  # Sử dụng document_id để tìm kiếm
        }
    },
    "sort": [
        {"word_count": {"order": "asc"}}  # Sắp xếp tăng dần theo word_count
    ],
    "size": 10  # Giới hạn số kết quả trả về
}

try:
    # Thực hiện truy vấn
    logger.info(f"Thực hiện truy vấn trên chỉ số: {index_name}")
    response = es.search(index=index_name, body=query)

    # Kiểm tra và xử lý kết quả
    hits = response.get('hits', {}).get('hits', [])
    if not hits:
        logger.warning("Không tìm thấy kết quả phù hợp.")
    else:
        for hit in hits:
            print(f"ID: {hit['_id']}")
            print(f"URL: {hit['_source'].get('url', 'Không xác định')}")
            print("-" * 40)
except Exception as e:
    logger.error(f"Lỗi khi thực hiện truy vấn: {e}")
