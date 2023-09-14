import sys
import configparser
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import max


def initialize_spark():
    findspark.init()
    return SparkSession.builder.appName('incrementaldataload').config("spark.jars", "C:\Installed_softwares\postgresql-42.6.0.jar").getOrCreate()

def main():
    if len(sys.argv) < 2:
        print("Usage: incrementaldataload.py <table_name>")
        sys.exit(1)

    table_name = sys.argv[1]
    spark = initialize_spark()

    config = configparser.ConfigParser()
    config_path = "C:/rohan/cgpoc3.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()

        config.read_string(content)

    properties = {
        "driver": config.get("db_details", "driver"),
        "user": config.get("db_details", "user"),
        "url": config.get("db_details", "url"),
        "password": config.get("db_details", "password"),
        "save_path": config.get("db_details", "base_path")

    }


    local_parquet_path = f'{properties["save_path"]}/{table_name}.parquet'
    df = spark.read.parquet(local_parquet_path)


    latest_date = df.agg(max(df["created_date"])).collect()[0][0]

    db_df = spark.read.jdbc(url=properties["url"], table=table_name, properties=properties)


    updated_records = db_df.filter(db_df.created_date > latest_date)


    updated_records.write.mode('append').parquet(properties['save_path'] + f'/{table_name}_updated_records')



if __name__ == "__main__":
    main()

