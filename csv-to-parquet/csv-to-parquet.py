'''
Application: Csv to Parquet
Creator: Leandro da Silva
Date: 2021-02-12
Description:
  The Application has to load a csv file, from the input folder, to a Spark Dataframe.
  This CSV file contains duplicated records that must be removed, using the following business rule:
    * Keep only the most recent records based on the Id and Update_Date Columns.
  After the deduplication, the application has to use a config json file to execute a data type mapping.
  Finally, the application has to export a parquet file to the output folder. 
'''

# ------------------------ #
# Loading Python Libraries #
# ------------------------ #
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import json
import gc
from os import listdir

msg = "Python libraries loaded"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
# ------------------------------- #
# Defning the Spark Configuration #
# ------------------------------- #
config = SparkConf().setAll(
    [
        # Defining the number of cores to use on each executor.
        ("spark.executor.cores", "2"),
        # Defining the number of maximum amount of CPU cores to request for the application from across the cluster.
        ("spark.cores.max", "2"),
        # Defining the amount of memory to use per executor process.
        ("spark.executor.memory", "1g"),
        # Defining the compression codec used when writing Parquet files.
        ("spark.sql.parquet.compression.codec", "gzip"),
    ]
)

# -------------------------------------------------------- #
# Spark Session creation using the configuration defintion #
# -------------------------------------------------------- #
spark = SparkSession.builder \
        .master("local[1]") \
        .config(conf=config) \
        .appName('Csv-to-Parquet') \
        .getOrCreate()

msg = "Spark Session created using SparkConf"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)

# ------------------- #
# CSV Input File Path #
# ------------------- # 
csv_file = './data/input/users/load.csv'

# ----------------------------------------- #
# Loading the CSV file to a Spark DataFrame #
# ----------------------------------------- #
df = spark.read.option("header", True).csv(csv_file)

msg = "CSV Loaded"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)

msg = "Raw Data"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
df.show()

msg = "Original Schema"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
df.printSchema()

# -------------------- #
# Deduplicaton process #
# -------------------- #
# Clean Code - Windows function definition to do the PartitionBy clause
# Business Rule (Requirements.txt): 
#  1. Grouping by Id column.
#  2. Desc ordering by Update_date column.
wf_partitionBy = Window.partitionBy("id").orderBy(df["update_date"].desc())

# Running the deduplication
# Keeping only the most recent record based on Id and  Update_date Column (PartitionBy Definition above)
# Where the row_number = 1 will always be for the most recent record. 
df_dedup = df.select("*", F.row_number().over(wf_partitionBy).alias("row_number")) \
    .where('row_number = 1') \
    .drop('row_number') \
    .orderBy("id")

msg = "Deduplication executed"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
print ("")

msg = "Data after removing duplicates"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
df_dedup.show()
print ("")

msg = "Data schema after removing duplicates"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
df_dedup.printSchema()

# -------------------- #
# Type Mapping process #
# -------------------- #
# Function responsible to execute the cast of the dataframe data types
def df_cast_column_type(item):
    try:
        json_file = open("./config/types_mapping.json")
        json_object = json.load(json_file)

        for col in item.columns:
            for (key, value) in json_object.items():
                if (col.lower() == key.lower()):                
                    item = item.withColumn(
                        col, F.col(col).cast(value)
                    )
                    
        return item
    except Exception as error:
        print(error)
    finally:
        if json_file:
            json_file.close()
           
# Executing the type mappings using the function "df_cast_column_type"
df_cast = df_cast_column_type(df_dedup)


msg = " Data after data type mappings"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
df_cast.show()
print ("")

msg = " Data schema after data type mappinds"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
df_cast.printSchema()

# -------------------- #
# Exporting to Parquet #
# -------------------- #
output = "./data/output"
df_cast.repartition(1).write.mode("overwrite").parquet(output)

msg = "Parquet file exported to {}".format(output)
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
print ("")

# ---------- #
# List files #
# ---------- #
msg = "listing output folder"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)
print(listdir(output))

# -------------- #
# House cleaning #
# -------------- #
del df_cast
del df_dedup
del df
gc.collect()

msg =  "Dataframes have been deleleted and GC executed"
dash = "-"*len(msg)
print (dash)
print (msg)
print (dash)