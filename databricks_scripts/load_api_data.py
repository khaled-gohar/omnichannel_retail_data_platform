import requests
import yaml

from pyspark.sql.functions import (
    current_timestamp,
    lit
)


# -----------------------------
# Extract data from API
# -----------------------------
def extract_api(url):

    response = requests.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    return data["data"]


# -----------------------------
# Create Spark DataFrame
# -----------------------------
def create_dataframe(records):

    df = spark.createDataFrame(records)

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
            f"retail.bronze.api_{table_name}"
        )
    )


# -----------------------------
# Main pipeline
# -----------------------------
def main():

    # Load configuration
    with open(
        "/Workspace/Users/kahledtrojan@gmail.com/omnichannel_retail_data_platform/databricks_scripts/config/config_API.yml"
    ) as f:

        config = yaml.safe_load(f)

    # Process all API tables
    for table_name, table_config in config["tables"].items():

        url = table_config["URL"]

        # Extract
        records = extract_api(url)

        # Create Spark DataFrame
        df = create_dataframe(records)

        # Add metadata
        df = add_metadata(
            df,
            "json_ocean"
        )

        # Full load into Bronze
        write_bronze(
            df,
            table_name
        )


if __name__ == "__main__":
    main()