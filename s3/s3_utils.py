# s3_utils.py
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
from s3_manager import S3Manager   # import your class
from s3_manager import ProgressPercentage
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
#import threading

# Load environment variables
load_dotenv()

# Initialize S3Manager with env vars
s3 = S3Manager(
    bucket_name="aic-bucket-hcmus",
    region=os.environ.get("AWS_REGION"),
    aws_access_key=os.environ.get("AWS_ACCESS_KEY"),
    aws_secret_key=os.environ.get("AWS_SECRET_KEY")
)

# ---------- Utility Functions ----------

def get_public_url(object_name):
    """Get public URL for an S3 object"""
    return s3._public_url(object_name)

def upload_file(file_path, object_name=None, storage_class="STANDARD"):
    """Upload a single file to S3"""
    return s3.upload(file_path, object_name, storage_class)

def upload_many(file_mappings, storage_class="STANDARD", max_workers=5):
    """
    Upload multiple files to S3 in parallel with a global progress bar.
    """
    results = []

    total_size = sum(os.path.getsize(fp) for fp, _ in file_mappings)
    with tqdm(total=total_size, unit="B", unit_scale=True, desc="Total Progress") as global_pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {}
            for file_path, object_name in file_mappings:
                filesize = os.path.getsize(file_path)
                future = executor.submit(
                    s3.s3_client.upload_file,
                    file_path,
                    s3.bucket,
                    object_name or os.path.basename(file_path),
                    ExtraArgs={"StorageClass": storage_class},
                    Callback=ProgressPercentage(file_path, filesize, global_pbar)
                )
                future_to_file[future] = (file_path, object_name)

            for future in as_completed(future_to_file):
                file_path, object_name = future_to_file[future]
                try:
                    future.result()
                    results.append(s3._public_url(object_name or os.path.basename(file_path)))
                except Exception as e:
                    print(f"❌ Failed to upload {file_path}: {e}")
                    results.append(None)

    return results

def upload_folder(local_folder, s3_prefix="", storage_class="STANDARD", max_workers=5):
    """
    Upload a local folder (recursively) to S3 with corresponding prefix.

    Args:
        local_folder (str): Path to the local folder.
        s3_prefix (str): Prefix (folder) in S3 bucket. Default "" = root.
        storage_class (str): S3 storage class.
        max_workers (int): Number of parallel uploads.

    Returns:
        list: List of uploaded file URLs.
    """
    file_mappings = []

    # Walk through the folder recursively
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)

            # Preserve relative path for object name
            rel_path = os.path.relpath(local_path, local_folder)
            object_name = os.path.join(s3_prefix, rel_path).replace("\\", "/")  # S3 expects "/"

            file_mappings.append((local_path, object_name))

    if not file_mappings:
        print("⚠️ No files found in folder:", local_folder)
        return []

    print(f"📂 Found {len(file_mappings)} files to upload from {local_folder}")
    return upload_many(file_mappings, storage_class=storage_class, max_workers=max_workers)

def upload_large_file(file_path, object_name=None, part_size=50 * 1024 * 1024):
    """Multipart upload for large files"""
    return s3.upload_large(file_path, object_name, part_size)

def download_file(object_name, dest_path):
    """Download a file from S3"""
    return s3.download(object_name, dest_path)

def delete_file(object_name):
    """Delete a file from S3"""
    return s3.delete(object_name)

def get_list(prefix=""):
    """List files in the bucket (optionally by prefix/folder)"""
    return s3.list_files(prefix)

def get_presigned_url(object_name, expiry=3600):
    """Get a presigned URL for temporary access"""
    return s3.generate_presigned_url(object_name, expiry)

def list_files(prefix=""):
    """Liệt kê tất cả file dưới 1 prefix (folder) và trả về danh sách URL công khai."""
    file_urls = []
    paginator = s3.s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            file_urls.append(get_public_url(obj["Key"]))
    return file_urls


def download_folder(prefix: str, local_dir: str, max_workers=64):
    """
    Download toàn bộ object trong folder (prefix) về local_dir với tốc độ cao,
    thanh tiến trình và giữ nguyên cấu trúc thư mục con.
    """
    # Đảm bảo prefix luôn kết thúc bằng "/" để os.path.relpath hoạt động đúng
    if not prefix.endswith('/'):
        prefix += '/'

    os.makedirs(local_dir, exist_ok=True)

    # Step 1: Lấy danh sách file và tổng kích thước
    paginator = s3.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3.bucket, Prefix=prefix)

    files_to_download = []
    total_size = 0
    print("Đang lấy danh sách file và tính toán kích thước...")
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj.get("Size", 0)
            if size > 0 and key.endswith(".webp"):
                files_to_download.append({"key": key, "size": size})
                total_size += size
    
    if not files_to_download:
        print("⚠️ Không tìm thấy file .webp nào để tải trong prefix:", prefix)
        return

    print(f"📂 Tìm thấy {len(files_to_download)} files, tổng kích thước: {total_size / (1024*1024):.2f} MB")

    # Step 2: Tải song song với thanh tiến trình toàn cục
    with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {prefix}") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for file_info in files_to_download:
                key = file_info["key"]
                size = file_info["size"]
                
                # --- THAY ĐỔI CHÍNH Ở ĐÂY ---
                # Tạo đường dẫn tương đối để giữ cấu trúc thư mục
                relative_path = os.path.relpath(key, start=prefix)
                local_path = os.path.join(local_dir, relative_path)
                
                # Đảm bảo thư mục con tồn tại trước khi tải
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                # ---------------------------

                futures.append(
                    executor.submit(
                        s3.s3_client.download_file,
                        s3.bucket,
                        key,
                        local_path,
                        Callback=ProgressPercentage(key, size, pbar)
                    )
                )
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Lỗi khi tải file: {e}")