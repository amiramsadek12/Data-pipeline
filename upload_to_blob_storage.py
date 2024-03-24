"""Script responsible of uploading file from local filesystem to GCP blob storage."""

import os
from google.cloud import storage
from pathlib import Path
from argparse import Namespace, ArgumentParser

BUCKET_NAME = "test-modeo-bucket"


def upload_blob(path_to_file: Path) -> None:
    """Uploads a file to the bucket."""

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "api.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(path_to_file.name)

    if blob.exists():
        generation_match_precondition = blob.generation
    else:
        generation_match_precondition = None

    blob.upload_from_filename(
        path_to_file, if_generation_match=generation_match_precondition
    )

    print(f"File {path_to_file} uploaded to {BUCKET_NAME}.")


def parse_args() -> Namespace:
    """Parse the arguments needed to run the script."""
    parser = ArgumentParser(description="Upload file to Google Cloud Storage bucket")
    parser.add_argument("--source_file_name", type=str, help="Path to the source file")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    path_to_source_file = Path(args.source_file_name)
    upload_blob(path_to_source_file)
