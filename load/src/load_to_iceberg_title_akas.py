import argparse
from s3_utils import object_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def main(snapshot_date: str, ingested_at_timestamp: str, snapshot_try: int):

    spark = SparkSession.builder \
        .appName("load-title-akas-Iceberg-MinIO") \
        .getOrCreate()

    # Read from raw data from S3
    title_akas_df = spark.read \
        .option("header", True) \
        .option("sep", "\t") \
        .csv(f"s3a://{object_path}/title.akas.tsv.gz")

    # Load to Iceberg
    title_akas_df \
        .withColumn("snapshot_date", lit(snapshot_date)) \
        .withColumn("ingested_at_timestamp", lit(ingested_at_timestamp)) \
        .withColumn("snapshot_try", lit(int(snapshot_try) + 1)) \
        .writeTo("demo.stage_raw.title_akas").createOrReplace()

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load title_akas to Iceberg")
    parser.add_argument("--snapshot_date", required=True, help="Snapshot date YYYY-MM-DD")
    parser.add_argument("--ingested_at_timestamp", required=True, help="Snapshot timestamp UTC YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--snapshot_try", required=True, help="Snapshot try")
    
    args = parser.parse_args()

    # Call main function with the parsed arguments
    main(args.snapshot_date, args.ingested_at_timestamp, args.snapshot_try)
