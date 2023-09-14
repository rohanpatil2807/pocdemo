import sys
import configparser
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.functions import lit, current_date
from datetime import datetime


def initialize_spark():
    findspark.init()
    return SparkSession.builder.appName('reading_file1').config("spark.jars", "C:\Installed_softwares\postgresql-42.6.0.jar").getOrCreate()


def read_and_create_temp_views(spark, properties):
    saved_tables = ['customers', 'items', 'order_details', 'orders', 'salesperson', 'ship_to']

    for table_name in saved_tables:
        path = f"{properties['save_path']}/{table_name}.parquet"
        reading_file = spark.read.parquet(path)
        reading_file.createOrReplaceTempView(table_name)



def generate_report_1(spark):
    report_1 = spark.sql('''SELECT c.cust_name,date_format(o.order_date, 'YYYY-MM') AS month, COUNT(o.order_id) AS order_count
                            FROM orders o JOIN customers c 
                            ON o.cust_id = c.cust_id 
                            GROUP BY c.cust_name, month''')
    return report_1

def generate_report_3(spark):
    report_3 = spark.sql('''select i.item_description as item_name, count(o.item_quantity) as total_order_count from items i 
                            join order_details o
                            on i.item_id = o.item_id
                            group by item_name''')
    return report_3


def generate_report_4(spark):
    report_4 = spark.sql('''select i.category as category, count(o.order_id) as total_orders
                            from items i 
                            join order_details o
                            on i.item_id = o.item_id
                            group by category
                            order by total_orders desc''')
    return report_4

def generate_report_5(spark):
    report_5 = spark.sql('''select i.item_id as item , (o.item_quantity * o.detail_unit_price) as total_amount
                            from items i 
                            join order_details o
                            on i.item_id = o.item_id
                            group by item,total_amount
                            order by total_amount desc''')
    return report_5


def generate_report_6(spark):
    report_6 = spark.sql('''select i.category as category, (o.item_quantity * o.detail_unit_price) as total_amount
                            from items i 
                            join order_details o
                            on i.item_id = o.item_id
                            group by category,total_amount
                            order by total_amount desc''')
    return report_6



def main():
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

    read_and_create_temp_views(spark, properties)

    # report_1 = generate_report_1(spark)
    report_3 = generate_report_3(spark)
    report_4 = generate_report_4(spark)
    report_5 = generate_report_5(spark)
    report_6 = generate_report_6(spark)

    # Show or save reports as needed
    # report_1.show()
    # report_3.show()
    # report_4.show()
    # report_5.show()
    # report_6.show()


    Item_wise_Total_Order_count = report_3.withColumn('current_date', lit(current_date()))
    Item_name_category_wise_Total_Order_count_descending = report_4.withColumn('current_date', lit(current_date()))
    Item_wise_total_order_amount_in_descending = report_5.withColumn('current_date', lit(current_date()))
    Item_name_category_wise_total_order_amount_in_descending = report_6.withColumn('current_date', lit(current_date()))


    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Generate a timestamp
    output_path = f"{properties['save_path']}/Daily_Reports{timestamp}"

    Item_wise_Total_Order_count.write.jdbc(url=properties['url'], table="final_report3", mode="overwrite", properties=properties)
    Item_name_category_wise_Total_Order_count_descending.write.jdbc(url=properties['url'], table="final_report4", mode="overwrite", properties=properties)
    Item_wise_total_order_amount_in_descending.write.jdbc(url=properties['url'], table="final_report5", mode="overwrite", properties=properties)
    Item_name_category_wise_total_order_amount_in_descending.write.jdbc(url=properties['url'], table="final_report6", mode="overwrite", properties=properties)

    Item_wise_Total_Order_count.write.partitionBy('current_date').mode("append").parquet(output_path + '/Item_wise_Total_Order_count')
    Item_name_category_wise_Total_Order_count_descending.write.partitionBy('current_date').mode("append").parquet(output_path + '/Item_name_category_wise_Total_Order_count_descending')
    Item_wise_total_order_amount_in_descending.write.partitionBy('current_date').mode("append").parquet(output_path + '/Item_wise_total_order_amount_in_descending')
    Item_name_category_wise_total_order_amount_in_descending.write.partitionBy('current_date').mode("append").parquet(output_path + '/Item_name_category_wise_total_order_amount_in_descending')


if __name__ == "__main__":
    main()