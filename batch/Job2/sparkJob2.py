import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum
from datetime import datetime, timedelta
import os



spark = SparkSession\
    .builder\
    .master("local[4]")\
    .appName("sparkJob2")\
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

#add the path for the hive table on hdfs 


from pyspark.sql.functions import col, current_date, date_sub, to_date
tranformed_data = spark.read.orc("/user/hive/warehouse/transactionsdb.db/transactions_table/*")
tranformed_data.count()





# Convert the 'transaction_date' to date type (assuming it's in 'yyyy-MM-dd' format, adjust format as needed)
transformed_data = tranformed_data.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

# Define the date 1.5 years ago from today
one_and_half_years_ago = date_sub(current_date(), int(1.5 * 365)+1)  # 1.5 years in days

# Filter the data where transaction_date is within the last 1.5 years
filtered_data = tranformed_data.filter(col("transaction_date") == one_and_half_years_ago)

filtered_data.count()



# Show the filtered DataFrame
filtered_data.show(truncate=False)

from datetime import datetime, timedelta

# Function to get the previous day's date and the hour from 23 hours ago
def get_last_hour():
    now = datetime.now()
    last_day = now - timedelta(days=1)  # Subtract one day
    return last_day.strftime("%Y-%m-%d"), "23"  # Return previous day and hour 23

# Get the date and hour for the last hour
target_date, target_hour = get_last_hour()

# Base directory where your data is stored
base_dir = "/finalData/spark_project/sales_agents2"
dynamic_dir = f"date({target_date})"

# Full path to the directory
directory_path = os.path.join(base_dir, dynamic_dir)

# Print the directory path for debugging
print("Directory Path:", directory_path)

# Use glob to list all files in the directory
import glob

file_pattern = os.path.join(directory_path, '*')
all_files = glob.glob(file_pattern)




agents = spark.read.csv(directory_path, header=True)
agents.count()

agents = agents.dropDuplicates(["sales_person_id"])
agents.count()

# Perform join and aggregation
result_df = tranformed_data.join(agents, tranformed_data.sales_agent_id == agents.sales_person_id, "inner") \
                          .groupBy("name", "product_name") \
                          .agg(sum("units").alias("total_sold_units")) \
                          .select("name", "product_name", "total_sold_units")


result_df.show()
result_df.count()

df = result_df.coalesce(1)


# Write the DataFrame to a local CSV file with a header
df.write.option("header", "true").csv("file:///data/daily_dumb/daily_dumb.csv")


