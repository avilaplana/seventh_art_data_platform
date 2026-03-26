import boto3
from botocore.exceptions import ClientError
from datetime import datetime


def get_object_path(snapshot_date: str = None) -> str:
    """Return the S3 prefix for a given snapshot_date (YYYY-MM-DD).

    Falls back to today's UTC date when snapshot_date is not provided.
    """
    if snapshot_date:
        date = datetime.strptime(snapshot_date, "%Y-%m-%d")
    else:
        date = datetime.utcnow()

    year = date.year
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"

    return f"data/imdb/year={year}/month={month}/day={day}"


def check_s3_file_exists(bucket: str, key: str, endpoint_url: str = "http://minio:9000") -> None:
    """Raise a clear error if the given S3 object does not exist.

    Args:
        bucket: S3 bucket name.
        key: Object key (path within the bucket).
        endpoint_url: S3-compatible endpoint URL.

    Raises:
        FileNotFoundError: If the object is not found.
        RuntimeError: On unexpected S3 errors.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="admin",
        aws_secret_access_key="password",
    )
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchKey"):
            raise FileNotFoundError(
                f"S3 file not found: s3://{bucket}/{key}"
            ) from e
        raise RuntimeError(
            f"Unexpected S3 error checking s3://{bucket}/{key}: {e}"
        ) from e


# ---------------------------------------------------------------------------
# Legacy alias — kept for backward compatibility during transition
# ---------------------------------------------------------------------------
today = datetime.utcnow()
year = today.year
month = f"{today.month:02d}"
day = f"{today.day:02d}"

object_path = f"data/imdb/year={year}/month={month}/day={day}"
