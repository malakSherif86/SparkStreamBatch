
import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from datetime import datetime, timedelta
import os

spark = SparkSession\
    .builder\
    .master("local[4]")\
    .appName("sparkJob1")\
    .config("spark.eventLog.logBlockUpdates.enabled", True)\
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext


# Function to get the last hour's date and hour
def get_last_hour():
    now = datetime.now() 
    last_hour = now 
    return last_hour.strftime("%Y-%m-%d"), last_hour.strftime("%H")

# Get the date and hour for the last hour
target_date, target_hour = get_last_hour() 

# Base directory where your data is stored
base_dir = "/lastDEMO/spark_project/sales_transactions2"
dynamic_dir = f"date({target_date})"

# Full path to the directory
directory_path = os.path.join(base_dir, dynamic_dir)

# Print the directory path for debugging
print(f"Checking directory: {directory_path}")
directory_path1 = os.path.join(directory_path,f"sales_transactions_SS_raw_*_{target_hour}.csv" )
print(directory_path1)

#the right this is to but the varible insteed of the file path
data = spark.read.csv(directory_path1, header=True)

# Counting the number of rows
count = data.count()

print(count)



from pyspark.sql.functions import col, sum as spark_sum

# Calculate the null counts for each column
null_counts = data.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in data.columns])

# Convert the null counts to a list of tuples
null_counts_list = [(c, null_counts.collect()[0][c]) for c in data.columns]

# Create a new DataFrame for null counts
null_counts_df = spark.createDataFrame(null_counts_list, ["Column", "NullCount"])

# Show the null counts in tabular form
null_counts_df.show(40)

# List of columns to fill with their default values
fill_values = {
    "sales_agent_id": 0,
    "branch_id": 0,
    "shipping_address": "not available",
    "source": "not found",
    "logs": "not found"
}

# Get the current columns in the DataFrame
current_columns = data.columns

# Filter the fill_values dictionary to include only the columns that are present in the DataFrame
dynamic_fill_values = {col: fill_values[col] for col in fill_values if col in current_columns}

# Print the dynamic fill values dictionary for debugging
print(f"Dynamic fill values: {dynamic_fill_values}")

# Apply the fill operation with the dynamically constructed dictionary
data = data.na.fill(dynamic_fill_values)

# Show the first few rows to verify the changes (optional)
data.show()


#check for duplictes

duplicates = data.groupBy(data.columns).count().filter(col("count") > 1)

# Sum the counts of duplicate rows and subtract the number of unique duplicate groups
total_duplicate_count = duplicates.select(spark_sum(col("count") - 1).alias("total_duplicates")).collect()[0]["total_duplicates"]

print(f"Total number of duplicate rows: {total_duplicate_count}")

#drop duplictes
data = data.dropDuplicates()

#callculate discount 
from pyspark.sql.functions import when, col
data = data.withColumn(
    "discounts",
    when(col("offer_1") == True, 0.05)
    .when(col("offer_2") == True, 0.10)
    .when(col("offer_3") == True, 0.15)
    .when(col("offer_4") == True, 0.20)
    .when(col("offer_5") == True, 0.25)
    .otherwise(0.0)
)
data.show(3)

data.show(5)

from pyspark.sql.functions import coalesce, col, lit, when

columns = ['offer_1', 'offer_2', 'offer_3', 'offer_4','offer_5']



# Create the 'offer' column based on the condition
df = data.withColumn('offer',
                   when(col('offer_1') == 'True', 1)
                   .when(col('offer_2') == 'True', 2)
                   .when(col('offer_3') == 'True', 3)
                   .when(col('offer_4') == 'True', 4)
                   .when(col('offer_5') == 'True', 5)
                   .otherwise(None))

# Drop the original offer columns if needed
data = df.drop(*columns)

# Show the result
data.show()



from pyspark.sql.functions import split, col, when

# Split the shipping_address into components
df = data.withColumn('address_split', split(df['shipping_address'], '/'))

# Create new columns for city, state, and postal_code based on the condition
df = df.withColumn(
    'city',
    when(col('is_online') == 'yes', col('address_split').getItem(1))
    .otherwise("not found")
).withColumn(
    'state',
    when(col('is_online') == 'yes', col('address_split').getItem(2))
    .otherwise("not found")
).withColumn(
    'postal_code',
    when(col('is_online') == 'yes', col('address_split').getItem(3))
    .otherwise("not found")
)

# Drop the temporary address_split column if not needed
data= df.drop('address_split')

# Show the result
data.show()

#add total paid 
data = data.withColumn(
    "total_paid",
    (col("units") * col("unit_price")) - (col("units") * col("unit_price") * col("discounts"))
)



#handling email problems 
from pyspark.sql.functions import regexp_replace
data = data.withColumn("cusomter_email", regexp_replace("cusomter_email", r"\.com.*", ".com"))
display(data.head(1))



from pyspark.sql.functions import current_date, year, month
from pyspark.sql.functions import to_date
print(data.dtypes)
data = data.withColumn("transaction_date", to_date(data["transaction_date"], "yyyy-M-d"))
# Add the current extraction date
data = data.withColumn("extraction_date", current_date())

# Extract year and month from the extraction date
data = data.withColumn("Extraction_Year", year(data["extraction_date"])) \
           .withColumn("Extraction_Month", month(data["extraction_date"]))




#get info about the data 
distinct_years_count = data.select("Extraction_Year").distinct().count()
print(f"Distinct Years Count: {distinct_years_count}")

# Count distinct months
distinct_months_count = data.select("Extraction_Month").distinct().count()
print(f"Distinct Months Count: {distinct_months_count}")

# Count rows grouped by year and month
grouped_counts = data.groupBy( "Extraction_Month").count()

# Show the grouped counts
print("Row counts grouped by Year and Month:")
grouped_counts.show()

from pyspark.sql import functions as f

rdd = data.rdd
num_partitions = rdd.getNumPartitions()
print(num_partitions)
data.rdd.glom().map(lambda p: len(p)).collect()
data.rdd.glom().map(lambda p: set([i[-1] for i in p])).collect()
data.groupBy(f.spark_partition_id()).count().show()


data = data.coalesce(1)

spark.sql("CREATE DATABASE IF NOT EXISTS TransactionDataBase ")
databases = spark.sql("SHOW DATABASES")
databases.show()


try:
    # Save the DataFrame as a Hive table with multi-level partitioning and bucketing
    single_partition_df.write.format("orc") \
        .bucketBy(8, "product_name") \
        .sortBy("product_name") \
        .partitionBy("Extraction_Year", "Extraction_Month") \
        .mode("overwrite") \
        .saveAsTable("TransactionsDB.Transactions_Table")

    print("Table created successfully.")
except Exception as e:
    print(f"Error creating table: {e}")

from pyspark.sql.functions import concat, col, year, month, dayofmonth, quarter, date_format
customer_dim_df = data.select(
    "customer_id",
    "customer_fname",
    "cusomter_lname",  # Keep the typo if it exists in the data
    "cusomter_email"
).distinct()



product_dim_df = data.select(
    "product_id",
    "product_name",
    "product_category"
).distinct()


from pyspark.sql.functions import concat, lit

# Correctly using concat with lit
sales_data_with_location = data.select(
    concat(col("city"), lit("-"), col("state"), lit("-"), col("postal_code")).alias("location_id"),
    "city",
    "state",
    "postal_code"
).distinct()


from pyspark.sql.functions import dayofmonth, month, year, quarter, date_format

date_dim_df = data.select(
    "transaction_date",
    dayofmonth("transaction_date").alias("day"),
    month("transaction_date").alias("month"),
    year("transaction_date").alias("year"),
    quarter("transaction_date").alias("quarter"),
    date_format("transaction_date", "EEEE").alias("day_name")
).distinct()


from pyspark.sql import functions as F

fact_df = data.select(
    "transaction_id",
    "customer_id",
    "sales_agent_id",
    "branch_id",
    "product_id",
    F.concat_ws("-", "city", "state", "postal_code").alias("location_id"),
    "transaction_date",
    "offer",
    "is_online",
    F.col("units").cast("int").alias("units"),
    F.col("unit_price").cast("double").alias("unit_price"),
    "total_paid",
    "Extraction_Year",
    "Extraction_Month"
)

df = spark.read.orc("/user/hive/warehouse/transactionsdb.db/transactions_table/*")
df.show(20)

# If renaming the table is an option, use underscore instead of hyphen
customer_dim_df.write.format("orc") \
    .mode("append") \
    .saveAsTable("TransactionsDB.Customer1_Dim")


product_dim_df.write.format("orc") \
    .mode("append") \
    .saveAsTable("TransactionsDB.Product1_Dim")


sales_data_with_location.write.format("orc") \
    .mode("append") \
    .saveAsTable("TransactionsDB.Location1_Dim")


date_dim_df.write.format("orc") \
    .mode("append") \
    .saveAsTable("TransactionsDB.Date1_Dim")


fact_df.write.format("orc") \
    .partitionBy("Extraction_Year", "Extraction_Month") \
    .mode("append") \
    .saveAsTable("TransactionsDB.Transaction_FactTable")


table_description = spark.sql(f"DESCRIBE FORMATTED TransactionsDB.Transaction_FactTable")
table_description.show()

table_description = spark.sql(f"DESCRIBE FORMATTED TransactionsDB.Date1_Dim")
table_description.show(6)

table_description = spark.sql(f"DESCRIBE FORMATTED TransactionsDB.Location1_Dim")
table_description.show(4)

table_description = spark.sql(f"DESCRIBE FORMATTED TransactionsDB.Transaction_FactTable")
table_description.show()

table_description = spark.sql(f"DESCRIBE FORMATTED TransactionsDB.Product1_Dim")
table_description.show(3)

table_description = spark.sql(f"DESCRIBE FORMATTED TransactionsDB.Customer1_Dim")
table_description.show()

#total sold unites by product
most_selling_products_df = spark.sql("""
SELECT p.product_name, SUM(f.units) AS total_sold_units
FROM TransactionsDB.Transaction_FactTable f
JOIN TransactionsDB.Product1_Dim p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sold_units DESC
LIMIT 10
""")

most_selling_products_df.show()


#how many time did the cutomer use the offer , kol offer zahr kam mara 
spark.sql("""
    SELECT 
        CASE offer
            WHEN 1 THEN 'offer_1'
            WHEN 2 THEN 'offer_2'
            WHEN 3 THEN 'offer_3'
            WHEN 4 THEN 'offer_4'
            WHEN 5 THEN 'offer_5'
        END AS offer,
        COUNT(*) AS total_redeemed
    FROM TransactionsDB.Transaction_FactTable
    where offer is not null
    GROUP BY offer
    ORDER BY total_redeemed DESC
    LIMIT 5
""")


#a3la offer zahr per product, w zahr kam mara m3 l product 
spark.sql("""
    WITH offer_details AS (
        SELECT product_id,offer,COUNT(*) AS total_redeemed FROM TransactionsDB.Transaction_FactTable
        WHERE offer IS NOT NULL GROUP BY product_id, offer ),
    ranked_offers AS (
        SELECT  product_id,offer,total_redeemed,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY total_redeemed DESC) AS rank
        FROM offer_details)
    SELECT  p.product_name,
        CASE 
            WHEN ro.offer = 1 THEN 'offer_1'
            WHEN ro.offer = 2 THEN 'offer_2'
            WHEN ro.offer = 3 THEN 'offer_3'
            WHEN ro.offer = 4 THEN 'offer_4'
            WHEN ro.offer = 5 THEN 'offer_5'
            ELSE 'other_offer'
        END AS most_redeemed_offer,
        ro.total_redeemed
    FROM ranked_offers ro
    JOIN TransactionsDB.Product1_Dim p ON ro.product_id = p.product_id
    WHERE ro.rank = 1
    ORDER BY ro.total_redeemed DESC
""")


spark.conf.get("spark.sql.warehouse.dir")



lowest_online_sales_cities_df = spark.sql("""
SELECT l.city, round(SUM(f.total_paid),2) AS online_sales
FROM  TransactionsDB.Transaction_FactTable f
JOIN TransactionsDB.Location1_Dim l ON f.location_id = l.location_id
WHERE f.is_online = 'yes'
GROUP BY l.city
ORDER BY online_sales ASC
LIMIT 10
""")

lowest_online_sales_cities_df.show()


