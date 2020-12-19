
# FTP home folder S3 bucket uploader

# Watchdog Imports
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

# Boto3 Imports
import boto3
from botocore.exceptions import ClientError

# Requests Imports
import requests

# Loguru Imports

from loguru import logger

# Utils
import mimetypes
import time
import os


url = os.getenv("EVENT_HANDLER_URL")

session = boto3.session.Session()
client = session.client(
    "s3",
    region_name=os.getenv("BUCKET_REGION_NAME"),
    endpoint_url=os.getenv("BUCKET_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("BUCKET_ACCESS_ID"),
    aws_secret_access_key=os.getenv("BUCKET_ACCESS_KEY"),
)

bucket_name = os.getenv("BUCKET_NAME")


def on_created_or_modified(event):
    file_path = event.src_path
    content_type = mimetypes.guess_type(file_path)[0] or "binary/octet-stream"
    try:
        client.upload_file(
            Bucket=bucket_name,
            Filename=event.src_path,
            Key=file_path,
            ExtraArgs={
                "ContentType": content_type,
                "ServerSideEncryption": "AES256",
            },
        )
        requests.post(
            url, json={f"fileName": "{event.src_path}", "bucketName": "{bucket_name}"}
        )
        logger.debug("Uploaded %s to %s successfully" % (file_path, bucket_name))
    except ClientError as error:
        logger.error(
            "Upload of %s to %s failed with: %s" % (file_path, bucket_name, error)
        )


if __name__ == "__main__":
    # create the event handler
    ignore_patterns = [""]
    ignore_directories = False
    case_sensitive = True
    bucket_event_handler = RegexMatchingEventHandler(
        ignore_regexes=ignore_patterns,
        ignore_directories=ignore_directories,
        case_sensitive=case_sensitive,
    )

    bucket_event_handler.on_created = on_created_or_modified
    bucket_event_handler.on_modified = on_created_or_modified

    # create an observer
    path = "/home/"
    go_recursively = True
    my_observer = Observer()
    my_observer.schedule(bucket_event_handler, path, recursive=go_recursively)

    my_observer.start()
    try:
        while True:
            time.sleep(5)
    except:
        my_observer.stop()
        logger.info("Observer Stopped")
    my_observer.join()
