
def build_spark_submit(job: str, snapshot_date: str, ingested_at_timestamp: str, snapshot_try: int, extra_args: str = ""):
    extra = f" {extra_args}" if extra_args else ""
    return f"""\
spark-submit \
--master spark://spark-master:7077 \
--deploy-mode client \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.defaultCatalog=demo \
--conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
--conf spark.sql.catalog.demo.uri=http://rest:8181 \
--conf spark.sql.catalog.demo.warehouse=s3://warehouse/ \
--conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.demo.s3.endpoint=http://minio:9000 \
--conf spark.sql.catalog.demo.s3.path-style-access=true \
--conf spark.sql.catalog.demo.s3.access-key-id=admin \
--conf spark.sql.catalog.demo.s3.secret-access-key=password \
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.access.key=admin \
--conf spark.hadoop.fs.s3a.secret.key=password \
--conf spark.driver.extraJavaOptions="-Daws.region=eu-west-2" \
--conf spark.executor.extraJavaOptions="-Daws.region=eu-west-2" \
--packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,\
org.apache.iceberg:iceberg-aws-bundle:1.8.1,\
org.apache.hadoop:hadoop-aws:3.3.4 \
{job} \
--snapshot_date {snapshot_date} \
--ingested_at_timestamp "{ingested_at_timestamp}" \
--snapshot_try {snapshot_try}{extra}
"""
