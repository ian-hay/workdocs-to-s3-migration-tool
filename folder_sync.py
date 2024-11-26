import boto3
import os
import requests
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor
import logging
from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig
import time
import argparse

# Set up argparse to capture command-line arguments
parser = argparse.ArgumentParser(description='AWS credentials and folder info.')
parser.add_argument('--workdocs_folder_id', required=True, help='WorkDocs Folder ID')
parser.add_argument('--s3_bucket_name', required=True, help='S3 Bucket Name')
parser.add_argument('--region_name', required=True, help='AWS Region Name')
parser.add_argument('--max_pool_connections', type=int, default=28, help='Max pool connections for AWS SDK')
parser.add_argument('--max_workers', type=int, default=14, help='Max workers for ThreadPoolExecutor')

# Parse the arguments
args = parser.parse_args()

# Access the arguments
WORKDOCS_FOLDER_ID = args.workdocs_folder_id
S3_BUCKET_NAME = args.s3_bucket_name
REGION_NAME = args.region_name 
MAX_POOL_CONNECTIONS = args.max_pool_connections
MAX_WORKERS = args.max_workers

MAX_RETRIES = 5  # For API calls
TEMP_DIR = '/tmp'  # Temporary directory for processing files -- use for testing

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Set to DEBUG for more detailed output
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("script.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

# Custom Boto3 config
boto_config = Config(
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    max_pool_connections=MAX_POOL_CONNECTIONS  # Adjust pool size here
)

# AWS clients
workdocs_client = boto3.client('workdocs', region_name=REGION_NAME, config=boto_config)
s3_client = boto3.client('s3', config=boto_config)

def list_workdocs_folder_contents(folder_id, parent_path=''):
    """Recursively list all contents in the WorkDocs folder."""
    next_token = None
    while True:
        try:
            # Conditionally include the Marker parameter
            if next_token:
                response = workdocs_client.describe_folder_contents(
                    FolderId=folder_id,
                    Marker=next_token
                )
            else:
                response = workdocs_client.describe_folder_contents(
                    FolderId=folder_id
                )
        except Exception as e:
            logging.error(f"Error fetching folder contents for {folder_id}: {e}")
            raise

        for folder in response.get('Folders', []):
            folder_path = os.path.join(parent_path, folder['Name'])
            yield {
                'type': 'folder',
                'id': folder['Id'],
                'name': folder_path
            }
            yield from list_workdocs_folder_contents(folder['Id'], folder_path)

        for document in response.get('Documents', []):
            document_path = os.path.join(parent_path, document['LatestVersionMetadata']['Name'])
            yield {
                'type': 'file',
                'id': document['Id'],
                'name': document_path,
                'version_id': document['LatestVersionMetadata']['Id']
            }

        next_token = response.get('Marker')
        if not next_token:
            break


def get_file_from_workdocs(document_id, version_id):
    """Download a file from WorkDocs."""
    for attempt in range(MAX_RETRIES):
        try:
            response = workdocs_client.get_document_version(
                DocumentId=document_id,
                VersionId=version_id,
                Fields='SOURCE'
            )
            return response["Metadata"]["Source"]["ORIGINAL"]
        except Exception as e:
            logging.warning(f"Retry {attempt + 1}/{MAX_RETRIES} for document {document_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def download_file_from_workdocs(document_id, s3_path, version_id):
    """Stream a file from WorkDocs to S3."""
    try:
        # Regenerate URL before downloading
        file_url = get_file_from_workdocs(document_id, version_id)
        with requests.get(file_url, stream=True) as response:
            response.raise_for_status()
            s3_client.upload_fileobj(
                response.raw,
                S3_BUCKET_NAME,
                s3_path,
                ExtraArgs={'Metadata': {'workdocs-version-id': version_id}}
            )
        logging.info(f"Uploaded {s3_path} to S3 directly from WorkDocs.")
    except Exception as e:
        logging.error(f"Error streaming document {document_id} to S3: {e}")
        raise



def check_s3_file_version(s3_path, version_id):
    """Check if the file in S3 is up-to-date based on version ID."""
    try:
        response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_path)
        if response.get('Metadata', {}).get('workdocs-version-id') == version_id:
            logging.info(f"Skipping {s3_path}; already up-to-date.")
            return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logging.info(f"{s3_path} not found in S3; uploading.")
        else:
            logging.error(f"Error checking {s3_path} in S3: {e}")
    return False


def delete_removed_files_from_s3(workdocs_contents, s3_prefix=''):
    """Delete S3 objects that no longer exist in WorkDocs."""
    s3_objects = set()
    continuation_token = None

    # Paginate through S3 objects
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME, Prefix=s3_prefix, ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=s3_prefix)

        for item in response.get('Contents', []):
            s3_objects.add(item['Key'])

        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            break

    # Build the set of paths from WorkDocs
    workdocs_paths = {os.path.join(s3_prefix, item['name']) for item in workdocs_contents}
    for item in workdocs_contents:
        if item['type'] == 'folder':
            workdocs_paths.add(os.path.join(s3_prefix, item['name']) + '/')

    # Compare and delete stale S3 objects
    for s3_object in s3_objects - workdocs_paths:
        try:
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_object)
            logging.warning(f"Deleted stale S3 object: {s3_object}")
        except Exception as e:
            logging.error(f"Error deleting S3 object {s3_object}: {e}")


def sync_workdocs_to_s3(folder_id, s3_prefix=''):
    """Sync the contents of a WorkDocs folder to an S3 bucket."""
    transfer_config = TransferConfig(max_concurrency=MAX_WORKERS)
    s3_existing_objects = set()

    # Build a set of existing S3 objects to compare against
    continuation_token = None
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME, Prefix=s3_prefix, ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=s3_prefix)

        for item in response.get('Contents', []):
            s3_existing_objects.add(item['Key'])

        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            break

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        workdocs_paths = set()

        # Process WorkDocs items incrementally
        for item in list_workdocs_folder_contents(folder_id):
            if item['type'] == 'folder':
                folder_s3_path = os.path.join(s3_prefix, item['name']) + '/'
                logging.info(f"Ensuring folder {folder_s3_path} exists in S3.")
                s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=folder_s3_path)
                workdocs_paths.add(folder_s3_path)

            elif item['type'] == 'file':
                s3_file_path = os.path.join(s3_prefix, item['name'])
                workdocs_paths.add(s3_file_path)

                if not check_s3_file_version(s3_file_path, item['version_id']):
                    # Submit download tasks with URL regeneration
                    executor.submit(
                        download_file_from_workdocs,
                        item['id'], s3_file_path, item['version_id']
                    )

        # Delete stale objects incrementally
        stale_objects = s3_existing_objects - workdocs_paths
        for stale_object in stale_objects:
            try:
                s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=stale_object)
                logging.warning(f"Deleted stale S3 object: {stale_object}")
            except Exception as e:
                logging.error(f"Error deleting S3 object {stale_object}: {e}")


if __name__ == "__main__":
    try:
        os.makedirs(TEMP_DIR, exist_ok=True)
        sync_workdocs_to_s3(WORKDOCS_FOLDER_ID)
        logging.info("WorkDocs folder synced successfully.")
    except Exception as e:
        logging.error(f"Error during sync: {e}")