import boto3
from botocore.client import Config
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

# Expected tab-separated headers per IMDB file
EXPECTED_HEADERS = {
    "name.basics.tsv.gz": [
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession", "knownForTitles",
    ],
    "title.akas.tsv.gz": [
        "titleId", "ordering", "title", "region", "language",
        "types", "attributes", "isOriginalTitle",
    ],
    "title.basics.tsv.gz": [
        "tconst", "titleType", "primaryTitle", "originalTitle",
        "isAdult", "startYear", "endYear", "runtimeMinutes", "genres",
    ],
    "title.crew.tsv.gz": ["tconst", "directors", "writers"],
    "title.episode.tsv.gz": [
        "tconst", "parentTconst", "seasonNumber", "episodeNumber",
    ],
    "title.principals.tsv.gz": [
        "tconst", "ordering", "nconst", "category", "job", "characters",
    ],
    "title.ratings.tsv.gz": ["tconst", "averageRating", "numVotes"],
}

MAX_RETRIES = 3
REQUEST_TIMEOUT = 30


def download_raw_data_from_source(url: str) -> requests.Response:
    """
    Return a streaming Response from the URL, with retry/backoff.
    Raises on non-2xx or after exhausting retries.
    """
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            logger.info("Downloading %s (attempt %d/%d)", url, attempt + 1, MAX_RETRIES)
            response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            logger.debug("Download started; headers=%s", dict(response.headers))
            return response
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES - 1:
                delay = 2 ** attempt
                logger.warning(
                    "Request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1, MAX_RETRIES, e, delay,
                )
                time.sleep(delay)
            else:
                logger.error("All %d download attempts failed for %s: %s", MAX_RETRIES, url, e)
    raise last_exc


def validate_schema(raw_file: str, file_obj) -> None:
    """
    Read the first line of the gzip stream and verify it matches expected headers.
    Raises ValueError with a descriptive message on mismatch.
    file_obj must support .read() and will be partially consumed; callers should
    pass a fresh stream and re-open for the actual upload.
    """
    expected = EXPECTED_HEADERS.get(raw_file)
    if expected is None:
        logger.warning("No expected headers registered for %s — skipping schema validation", raw_file)
        return

    chunk = file_obj.read(4096)
    with gzip.open(io.BytesIO(chunk), "rb") as gz:
        first_line = gz.readline().decode("utf-8").rstrip("\n")

    actual_cols = first_line.split("\t")
    if actual_cols != expected:
        raise ValueError(
            f"Schema mismatch for {raw_file}.\n"
            f"  Expected : {expected}\n"
            f"  Got      : {actual_cols}"
        )
    logger.info("Schema validation passed for %s", raw_file)


def upload_raw_data_to_s3(s3_client, file_obj, bucket: str, raw_file: str) -> str:
    """
    Upload a file-like object to S3/MinIO with date partitioning.
    Returns the object key that was uploaded.
    """
    today = datetime.now(timezone.utc)
    year = today.year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"

    object_key = f"imdb/year={year}/month={month}/day={day}/{raw_file}"

    logger.info("Uploading %s → %s/%s", raw_file, bucket, object_key)
    s3_client.upload_fileobj(file_obj, bucket, object_key)
    logger.info("Upload complete: %s/%s", bucket, object_key)

    # Verify upload
    head = s3_client.head_object(Bucket=bucket, Key=object_key)
    content_length = head.get("ContentLength", 0)
    if content_length <= 0:
        raise RuntimeError(
            f"S3 upload verification failed for {object_key}: ContentLength={content_length}"
        )
    logger.debug("S3 verification OK — ContentLength=%d bytes", content_length)

    return object_key


def extract(raw_file: str) -> None:
    """
    Download raw_file from IMDB datasets, validate schema, and upload to S3.
    """
    logger.info("Starting extract for %s", raw_file)

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
        logger.info("Bucket %s already exists", ROOT_BUCKET)
    except Exception as e:
        logger.info("Bucket %s not found (%s) — creating it", ROOT_BUCKET, e)
        s3.create_bucket(Bucket=ROOT_BUCKET)
        logger.info("Created bucket %s", ROOT_BUCKET)

    url = f"{SOURCE_URL_PREFIX}{raw_file}"

    # --- Schema validation pass (download a fresh stream just for header check) ---
    expected = EXPECTED_HEADERS.get(raw_file)
    if expected is not None:
        logger.info("Validating schema for %s", raw_file)
        validation_response = download_raw_data_from_source(url)
        validate_schema(raw_file, validation_response.raw)

    # --- Actual upload pass ---
    logger.info("Streaming %s from %s", raw_file, url)
    upload_response = download_raw_data_from_source(url)
    upload_raw_data_to_s3(s3, upload_response.raw, ROOT_BUCKET, raw_file)

    logger.info("Extract finished for %s", raw_file)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <filename>  (e.g. name.basics.tsv.gz)", file=sys.stderr)
        print(f"Available files: {', '.join(sorted(EXPECTED_HEADERS))}", file=sys.stderr)
        sys.exit(1)

    raw_file = sys.argv[1]
    extract(raw_file)


if __name__ == "__main__":
    main()
