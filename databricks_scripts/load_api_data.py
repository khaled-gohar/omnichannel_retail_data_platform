%python
import requests

from pyspark.sql.functions import (
    current_timestamp,
    lit
)
import yaml


with open("/Workspace/Users/kahledtrojan@gmail.com/omnichannel_retail_data_platform/databricks_scripts/config/config_API.yml") as f:
    config = yaml.safe_load(f)

for table_name, table_config in config["tables"].items():

    url = table_config["URL"]


    # Extract
    response = requests.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()
    records = data["data"]

    # Create Spark DataFrame
    df = spark.createDataFrame(records)

    # Add metadata
    df = (
        df
        .withColumn("_source_system", lit("json_ocean"))
        .withColumn("_ingestion_timestamp", current_timestamp())
    )


    # Full load
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(F"retail.bronze.api_{table_name}")
    )