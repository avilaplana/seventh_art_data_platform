from datetime import datetime

today = datetime.utcnow()
year = today.year
month = f"{today.month:02d}"
day = f"{today.day:02d}"

# Build S3 object path with key with partition
object_path = f"data/imdb/year={year}/month={month}/day={day}"