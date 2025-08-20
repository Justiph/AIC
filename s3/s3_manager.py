import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from boto3.s3.transfer import TransferConfig
import os
#from dotenv import load_dotenv
from tqdm import tqdm
import threading



class ProgressPercentage:
    """Track upload progress and update tqdm progress bar."""
    def __init__(self, filename, filesize, pbar):
        self._filename = filename
        self._filesize = filesize
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._pbar = pbar

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            self._pbar.update(bytes_amount)
            
         
            

class S3Manager:
    def __init__(self, bucket_name, region="ap-southeast-2", aws_access_key=None, aws_secret_key=None):
        """
        Initialize the S3 client.
        If aws_access_key and aws_secret_key are not provided, boto3 will use credentials
        from environment variables or ~/.aws/credentials (aws configure).
        """
        if aws_access_key and aws_secret_key:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
        else:
            self.s3_client = boto3.client("s3", region_name=region)

        self.bucket = bucket_name
        self.region = region  # keep for building URLs

    def _public_url(self, object_name):
        """Build the public URL for an object (works if bucket policy allows)."""
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{object_name}"

    def upload(self, file_path, object_name=None, storage_class="STANDARD"):
        """Upload a file with progress bar."""
        try:
            if object_name is None:
                object_name = os.path.basename(file_path)

            filesize = os.path.getsize(file_path)
            with tqdm(total=filesize, unit="B", unit_scale=True, desc=f"Uploading {os.path.basename(file_path)}") as pbar:
                self.s3_client.upload_file(
                    file_path,
                    self.bucket,
                    object_name,
                    ExtraArgs={"StorageClass": storage_class},
                    Callback=ProgressPercentage(file_path, filesize, pbar)
                )

            url = self._public_url(object_name)
            print(f"\n✅ Uploaded {file_path} → {url}")
            return url
        except FileNotFoundError:
            print("❌ The file was not found.")
        except NoCredentialsError:
            print("❌ AWS credentials not available.")

    def upload_large(self, file_path, object_name=None, part_size=50 * 1024 * 1024):
        """Multipart upload for large files with progress bar."""
        try:
            if object_name is None:
                object_name = os.path.basename(file_path)

            config = TransferConfig(
                multipart_threshold=part_size,
                multipart_chunksize=part_size,
                max_concurrency=10,
                use_threads=True
            )

            filesize = os.path.getsize(file_path)
            with tqdm(total=filesize, unit="B", unit_scale=True, desc=f"Uploading {os.path.basename(file_path)}") as pbar:
                self.s3_client.upload_file(
                    file_path,
                    self.bucket,
                    object_name,
                    Config=config,
                    Callback=ProgressPercentage(file_path, filesize, pbar)
                )

            url = self._public_url(object_name)
            print(f"\n🚀 Large file uploaded {file_path} → {url}")
            return url
        except FileNotFoundError:
            print("❌ The file was not found.")
        except NoCredentialsError:
            print("❌ AWS credentials not available.")
    def download(self, object_name, dest_path):
        """Download a file from the S3 bucket."""
        try:
            self.s3_client.download_file(self.bucket, object_name, dest_path)
            print(f"✅ Downloaded s3://{self.bucket}/{object_name} → {dest_path}")
        except ClientError as e:
            print(f"❌ Error: {e}")

    def delete(self, object_name):
        """Delete a file from the S3 bucket."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=object_name)
            print(f"🗑️ Deleted s3://{self.bucket}/{object_name}")
        except ClientError as e:
            print(f"❌ Error: {e}")

    def list_files(self, prefix=""):
        """List all files in the S3 bucket (optionally filtered by prefix)."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if "Contents" in response:
                files = [obj["Key"] for obj in response["Contents"]]
                print("📂 Files in bucket:")
                for f in files:
                    print(" -", self._public_url(f))
                return files
            else:
                print("📂 Bucket is empty.")
                return []
        except ClientError as e:
            print(f"❌ Error: {e}")
            return []

    def generate_presigned_url(self, object_name, expiry=3600):
        """Generate a presigned URL (useful if bucket policy is private)."""
        try:
            url = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_name},
                ExpiresIn=expiry
            )
            print(f"🔗 Presigned URL (valid {expiry}s): {url}")
            return url
        except ClientError as e:
            print(f"❌ Error: {e}")
            return None

