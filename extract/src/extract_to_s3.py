import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import gzip
import io
import logging
import os
import sys
import time
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_ENDPOINT = os.environ['S3_ENDPOINT']
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
REGION = os.environ['REGION']
SOURCE_URL_PREFIX = "https://datasets.imdbws.com/"
ROOT_BUCKET = "data"

# Expected tab-separated column headers per IMDB file
EXPECTED_HEADERS = {
    "name.basics.tsv.gz": ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"],
    "title.akas.tsv.gz": ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"],
    "title.basics.tsv.gz": ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"],
    "title.crew.tsv.gz": ["tconst", "directors", "writers"],
    "title.episode.tsv.gz": ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
    "title.principals.tsv.gz": ["tconst", "ordering", "nconst", "category", "job", "characters"],
    "title.ratings.tsv.gz": ["tconst", "averageRating", "numVotes"],
}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3


def _download_with_retry(url: str) -> requests.Response:
    """
    Download URL with exponential backoff retries (3 attempts, delay 2^n seconds).
    Returns a streaming Response object.
    """
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Downloading {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES - 1:
                delay = 2 ** attempt
                logger.warning(f"Download attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"All {MAX_RETRIES} download attempts failed for {url}: {e}")
    raise last_exc


def validate_schema(raw_file: str, file_obj) -> None:
    """
    Read the first line of the gzip stream and verify it contains the expected
    tab-separated column headers. Raises ValueError if headers don't match.
    """
    expected = EXPECTED_HEADERS.get(raw_file)
    if expected is None:
        logger.warning(f"No expected headers defined for {raw_file}, skipping schema validation")
        return

    # Read just enough bytes to get the first line
    header_bytes = b""
    for chunk in file_obj.iter_content(chunk_size=4096):
        header_bytes += chunk
        if b"\n" in header_bytes:
            break

    # Decompress the gzip prefix to get the TSV header line
    buf = io.BytesIO(header_bytes)
    with gzip.open(buf, "rt", encoding="utf-8") as gz:
        first_line = gz.readline().rstrip("\n")

    actual_columns = first_line.split("\t")
    if actual_columns != expected:
        raise ValueError(
            f"Schema mismatch for {raw_file}.\n"
            f"  Expected columns: {expected}\n"
            f"  Actual columns:   {actual_columns}"
        )
    logger.info(f"Schema validated for {raw_file}: {actual_columns}")


def download_raw_data_from_source(url: str, raw_file: str):
    """
    Download the file at url, validate its schema, and return a file-like
    streaming object ready to be uploaded to S3.
    """
    response = _download_with_retry(url)
    logger.debug(f"HTTP response headers for {raw_file}: {dict(response.headers)}")

    # Validate schema using the first chunk, then return the full stream
    validate_schema(raw_file, response)

    # Re-request to get a fresh stream for the actual upload (schema consumed the first response)
    logger.info(f"Re-fetching {url} for upload after schema validation")
    upload_response = _download_with_retry(url)
    return upload_response.raw


def upload_raw_data_to_s3(s3_client, file_obj, bucket: str, raw_file: str) -> str:
    """
    Upload a file-like object to S3/MinIO with date partitioning.
    Verifies the upload was successful via head_object.

    Returns the S3 object key of the uploaded file.
    """
    today = datetime.now(timezone.utc)
    year = today.year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"

    object_key = f"imdb/year={year}/month={month}/day={day}/{raw_file}"

    logger.info(f"Uploading {raw_file} to s3://{bucket}/{object_key}")
    s3_client.upload_fileobj(file_obj, bucket, object_key)

    # Verify upload
    try:
        head = s3_client.head_object(Bucket=bucket, Key=object_key)
        content_length = head.get("ContentLength", 0)
        if content_length <= 0:
            raise ValueError(f"Upload verification failed: ContentLength={content_length} for {object_key}")
        logger.debug(f"Upload verified: s3://{bucket}/{object_key} ({content_length} bytes)")
    except ClientError as e:
        raise RuntimeError(f"head_object failed for {object_key}: {e}") from e

    logger.info(f"Successfully uploaded {raw_file} to s3://{bucket}/{object_key}")
    return object_key


def extract(raw_file: str) -> None:
    """
    Full extract pipeline for a single IMDB file:
      1. Connect to S3/MinIO
      2. Ensure bucket exists
      3. Download file with retry and schema validation
      4. Upload to S3 with date partitioning
      5. Verify upload via head_object
    """
    logger.info(f"Starting extract for {raw_file}")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name=REGION,
    )

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=ROOT_BUCKET)
        logger.info(f"Bucket {ROOT_BUCKET} already exists")
    except Exception as e:
        logger.info(f"Bucket {ROOT_BUCKET} not found ({e}), creating it")
        s3.create_bucket(Bucket=ROOT_BUCKET)
        logger.info(f"Created bucket {ROOT_BUCKET}")

    url = f"{SOURCE_URL_PREFIX}{raw_file}"
    logger.info(f"Streaming {raw_file} from {url}")

    try:
        file_obj = download_raw_data_from_source(url, raw_file)
        upload_raw_data_to_s3(s3, file_obj, ROOT_BUCKET, raw_file)
    except Exception as e:
        logger.error(f"Extract failed for {raw_file}: {e}")
        raise

    logger.info(f"Extract complete for {raw_file}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python extract_to_s3.py <filename.tsv.gz>")
        sys.exit(1)
    extract(sys.argv[1])
