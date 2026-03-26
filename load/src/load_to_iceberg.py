import argparse
import logging
from s3_utils import get_object_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructType, StructField, StringType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Explicit schemas for each IMDB table (all raw CSV columns are strings)
SCHEMAS = {
    "name_basics": StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", StringType(), True),
        StructField("deathYear", StringType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True),
    ]),
    "title_akas": StructType([
        StructField("titleId", StringType(), True),
        StructField("ordering", StringType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", StringType(), True),
    ]),
    "title_basics": StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", StringType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True),
    ]),
    "title_crew": StructType([
        StructField("tconst", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True),
    ]),
    "title_episode": StructType([
        StructField("tconst", StringType(), True),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", StringType(), True),
        StructField("episodeNumber", StringType(), True),
    ]),
    "title_principals": StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", StringType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True),
    ]),
    "title_ratings": StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", StringType(), True),
        StructField("numVotes", StringType(), True),
    ]),
}


def main(table: str, file: str, snapshot_date: str, ingested_at_timestamp: str, snapshot_try: int):
    if table not in SCHEMAS:
        raise ValueError(f"Unknown table '{table}'. Known tables: {list(SCHEMAS.keys())}")

    object_path = get_object_path(snapshot_date)
    iceberg_table = f"demo.stage_raw.{table}"
    s3_path = f"s3a://{object_path}/{file}"
    schema = SCHEMAS[table]

    logger.info("Starting load: table=%s, file=%s, snapshot_date=%s", table, file, snapshot_date)

    spark = SparkSession.builder \
        .appName(f"load-{table}-Iceberg-MinIO") \
        .getOrCreate()

    try:
        df = spark.read \
            .option("header", True) \
            .option("sep", "\t") \
            .schema(schema) \
            .csv(s3_path)

        source_count = df.count()
        logger.info("Source row count for %s: %d", table, source_count)

        df \
            .withColumn("snapshot_date", lit(snapshot_date)) \
            .withColumn("ingested_at_timestamp", lit(ingested_at_timestamp)) \
            .withColumn("snapshot_try", lit(int(snapshot_try) + 1)) \
            .writeTo(iceberg_table).createOrReplace()

        logger.info("Successfully loaded %d rows into %s for snapshot_date=%s", source_count, iceberg_table, snapshot_date)
    except Exception as e:
        logger.error(
            "Failed to load table=%s, snapshot_date=%s, file=%s: %s",
            table, snapshot_date, file, e
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load an IMDB table to Iceberg")
    parser.add_argument("--table", required=True, help="Iceberg target table name (e.g. name_basics)")
    parser.add_argument("--file", required=True, help="Source filename in S3 (e.g. name.basics.tsv.gz)")
    parser.add_argument("--snapshot_date", required=True, help="Snapshot date YYYY-MM-DD")
    parser.add_argument("--ingested_at_timestamp", required=True, help="Snapshot timestamp UTC YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--snapshot_try", required=True, help="Snapshot try")

    args = parser.parse_args()

    main(args.table, args.file, args.snapshot_date, args.ingested_at_timestamp, args.snapshot_try)
