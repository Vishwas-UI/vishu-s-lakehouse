from pyspark.sql import SparkSession
from pathlib import Path

def init_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("hudi") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1") \
        .getOrCreate()

def write_hudi_table(spark: SparkSession, input_parquet_path: str, hudi_output_path: str, table_name: str):
    df = spark.read.parquet(input_parquet_path)

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.precombine.field': 'timestamp',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
        'hoodie.datasource.hive_sync.enable': 'false'
    }

    partition_cols = []
    expected_partition_fields = ["year","month"]

    df_cols = df.columns

    for p in expected_partition_fields:
        if p in df_cols:
            partition_cols.append(p)

    if partition_cols:
        hudi_options['hoodie.datasource.write.partitionpath.field'] = ",".join(partition_cols)
        hudi_options['hoodie.datasource.write.hive_style_partitioning'] = 'true'
    else:
        print(f"[WARN] No partition fields found for table {table_name}")

    df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save(hudi_output_path)

    print(f"Data written to Hudi table: {hudi_output_path}")