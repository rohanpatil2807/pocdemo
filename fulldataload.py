import sys
import configparser
import findspark
from pyspark.sql import SparkSession

findspark.init()
spark = SparkSession.builder.appName('de').config("spark.jars", "C:\Installed_softwares\postgresql-42.6.0.jar").getOrCreate()



def read_properties(properties_file_path):
    config = configparser.ConfigParser()
    config.read(properties_file_path)

    properties = {
        "driver": config.get("db_details", "driver"),
        "user": config.get("db_details", "user"),
        "url": config.get("db_details", "url"),
        "password": config.get("db_details", "password"),
        "save_path": config.get("db_details", "base_path")
    }
    return properties



def main(properties_file_path, table_name):
    properties = read_properties('C:/rohan/cgpoc3.properties')

    data = spark.read.jdbc(url=properties["url"], table=table_name, properties=properties)

    output_path = f"{properties['save_path']}/{table_name}.parquet"
    data.write.parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py properties_file_path table_name")
        sys.exit(1)

    properties_file_path = sys.argv[1]
    table_name = sys.argv[2]

    main(properties_file_path, table_name)

