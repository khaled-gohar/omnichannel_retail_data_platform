from pyspark.sql.functions import (
    current_timestamp,
    lit
)
import yaml

with open("/Workspace/Users/kahledtrojan@gmail.com/omnichannel_retail_data_platform/databricks_scripts/config/config_AWS.yml") as f:
    config = yaml.safe_load(f)

for table_name, table_config in config["tables"].items():

    path = table_config["path"]


    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )

    df = (
        df
        .withColumn("_source_system", lit("AWS_S3"))
        .withColumn("_ingestion_timestamp", current_timestamp())
    )


    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            f"retail.bronze.aws_s3_{table_name}"
        )
    )
