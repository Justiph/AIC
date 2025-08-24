import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from s3_utils import download_folder, upload_file, upload_many, upload_folder, get_list, download_file, delete_file, get_presigned_url,list_files
import requests
from PIL import Image
import time
# Public URL example
#print(get_public_url("test_upload/JUSTIPH.png"))



# Upload one file
# upload_file("static/images/phan tai.jpg", "test_upload/phan tai.jpg")



# Upload many files
# Upload with explicit object names
# files = [
#     ("static/images/kyyeu.jpg", "test_upload/kyyeu.jpg"),
#     ("static/images/tran thanh.jpg", "test_upload/tran thanh.jpg"),
# ]
# urls = upload_many(files, storage_class="STANDARD", max_workers=10)
# print("Uploaded URLs:", urls)



# Upload entire folder "static/images" into bucket prefix "project_images/"

# urls = upload_folder("static/images", s3_prefix="project_images", max_workers=10)
# print("Uploaded folder URLs:", urls)


# base_folder = "D:/Download/Keyframes_L30_a" 
# for i in range(1, 97): # từ 45 đến 96 
#     subfolder = f"L30_V{i:03d}" # ví dụ: L30_V045, L30_V046, ... 
#     folder_path = f"{base_folder}/{subfolder}" 
#     s3_prefix = f"Keyframes_L30_a/{subfolder}" 
#     urls = upload_folder(folder_path, s3_prefix=s3_prefix, max_workers=32) 
#     print(f"Uploaded folder {folder_path} URLs:", urls)




# List all files
list = list_files(prefix="Keyframes_L29_a")
print(len(list))
url = list[0]
print(url)


# Ghi lại thời điểm bắt đầu
start_time = time.time()

try:
    # Đoạn mã bạn muốn đo thời gian
    image = Image.open(requests.get(url, stream=True).raw)

    # Ghi lại thời điểm kết thúc
    end_time = time.time()

    # Tính thời gian đã trôi qua
    execution_time = end_time - start_time

    print(f"Đoạn mã đã chạy trong: {execution_time:.4f} giây")
    # image.show() # Bạn có thể hiển thị ảnh để kiểm tra

except Exception as e:
    print(f"Đã xảy ra lỗi: {e}")


download_folder(prefix="Keyframes_L29_a", local_dir="D:/Download/Keyframes_L29_a")
# Download file
#download_file("cat.jpg", "downloads/cat.jpg")



# Delete file
#delete_file("dog.jpg")



# Get presigned URL (valid 10 mins)
#get_presigned_url("JUSTIPH.png", expiry=600)
