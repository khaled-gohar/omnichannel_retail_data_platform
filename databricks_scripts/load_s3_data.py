from pyspark.sql.functions import (
    current_timestamp,
    lit
)
import yaml


# -----------------------------
# Read data from S3
# -----------------------------
def read_s3(path):

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    )

    return df


# -----------------------------
# Add ingestion metadata
# -----------------------------
def add_metadata(df, source_system):

    return (
        df
        .withColumn(
            "_source_system",
            lit(source_system)
        )
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
    )


# -----------------------------
# Write data to Bronze
# -----------------------------
def write_bronze(df, table_name):

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            f"retail.bronze.aws_s3_{table_name}"
        )
    )


# -----------------------------
# Main pipeline
# -----------------------------
def main():

    # Load configuration
    with open(
        "/Workspace/Users/kahledtrojan@gmail.com/omnichannel_retail_data_platform/databricks_scripts/config/config_AWS.yml"
    ) as f:

        config = yaml.safe_load(f)

    # Process all S3 tables
    for table_name, table_config in config["tables"].items():

        path = table_config["path"]

        # Read data
        df = read_s3(path)

        # Add metadata
        df = add_metadata(
            df,
            "AWS_S3"
        )

        # Full load into Bronze
        write_bronze(
            df,
            table_name
        )


if __name__ == "__main__":
    main()