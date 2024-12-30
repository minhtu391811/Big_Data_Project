import subprocess
import os
import json

def store_data_in_hdfs(url, text, title, description, keywords):
    """
    Lưu nội dung của mỗi website vào một file JSON và upload lên HDFS.
    """
    output_dir = "F:/ITTN_Project/Big_Data/Big-Data-Project/Lambda/Stream_data/"
    hdfs_dir = '/batch_layer/raw_data'

    # Đảm bảo thư mục output tồn tại
    os.makedirs(output_dir, exist_ok=True)

    # Tạo tên file từ URL (thay thế ký tự không hợp lệ)
    filename = url.replace("http://", "").replace("https://", "").replace("/", "_").replace(":", "_").replace(".", "_")
    filename = f"{filename}.json"

    # Đường dẫn đầy đủ của file JSON cục bộ
    local_file_path = os.path.join(output_dir, filename)

    # Chuẩn bị dữ liệu
    website_data = {
        'url': url,
        'text': text,
        'title': title,
        'description': description,
        'keywords': keywords  # List of keywords
    }

    # Lưu vào file JSON cục bộ
    try:
        with open(local_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(website_data, json_file, ensure_ascii=False, indent=4)
        print(f"Saved website data as JSON to: {local_file_path}")
    except Exception as e:
        print(f"Error saving website data for {url}: {e}")
        return

    # Upload file trực tiếp lên HDFS
    try:
        # Verify the file exists before attempting to upload
        if not os.path.exists(local_file_path):
            print(f"Error: Local file does not exist: {local_file_path}")
            return
        
        hdfs_put_command = [
            "hdfs", "dfs", "-put", "-f", local_file_path, hdfs_dir
        ]
        result = subprocess.run(hdfs_put_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            print(f"Successfully uploaded {local_file_path} to HDFS at {hdfs_dir}")
        else:
            print(f"Failed to upload file to HDFS. Error: {result.stderr}")

    except Exception as e:
        print(f"Error uploading file to HDFS: {e}")
