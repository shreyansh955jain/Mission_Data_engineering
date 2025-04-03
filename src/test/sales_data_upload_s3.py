from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *

# Initialize S3 Client
s3_client_provider = S3ClientProvider(decrypt(config.aws_access_key), decrypt(config.aws_secret_key))
s3_client = s3_client_provider.get_client()

# Configuration Variables
local_file_source = "/home/shreyansh-jain/PycharmProjects/MIssion_DataEngineering/upload_to_s3"
s3_folder = config.s3_source_directory
s3_bucket_name = config.bucket_name


def upload_to_s3(directory, local_source, bucket):
    """
    Uploads files from a local directory to an S3 bucket.

    :param directory: Target S3 directory (prefix)
    :param local_source: Local directory containing files
    :param bucket: Target S3 bucket name
    """
    s3_prefix = f"{directory}/" if not directory.endswith("/") else directory
    # Ensure proper S3 prefix format

    try:
        for root, _, files in os.walk(local_source):
            for file in files:
                print(f"Uploading: {file}")
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, bucket, s3_key)
                print(f"Uploaded {file} to s3://{bucket}/{s3_key}")
    except Exception as err:
        print(f"Error uploading to S3: {err}")
        raise  # Re-raise exception for better debugging


# Call upload function
upload_to_s3(s3_folder, local_file_source, s3_bucket_name)
