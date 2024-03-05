# 1. Batch Layer (Delta Lake + Spark Job):
# This layer processes historical sales data in batches (e.g., daily).

from pyspark.sql.functions import col, current_timestamp, window
from delta.tables import DeltaTable

# Data Schema 

#| column      | type        | description  |
#|-------------|-------------|--------------|
#| product_id  | int         | Product ID   |
#| customer_id | int         | Customer ID  |
#| price       | double      | Price        |
#| timestamp   | timestamp   | Sale Time    |

def batch_layer(spark, data_path):
  # Read raw sales data from parquet files
  df = spark.read.parquet(data_path)

  # Perform transformations (example: filter recent data)
  filtered_df = df.where(col("timestamp") >= (current_timestamp() - F.expr("interval 1 day")))

  # Write transformed data to Delta table
  delta_table = DeltaTable.forPath(spark, "/delta/sales")
  delta_table.alias("bronze").merge(filtered_df.alias("silver"), condition="bronze.product_id = silver.product_id")\
             .whenMatchedUpdateAll()\
             .whenNotMatchedInsertAll()\
             .execute()

# Call the batch layer function with appropriate data path
batch_layer(spark, "/mnt/sales/raw/data")



#2. Speed Layer (Structured Streaming):
#This layer processes real-time sales data using structured streaming.

def speed_layer(spark, kafka_broker, topic):
  # Define streaming DataFrame from Kafka topic
  sales_stream = spark.readStream.format("kafka")\
                     .option("kafka.bootstrap.servers", kafka_broker)\
                     .option("subscribe", topic)\
                     .load()

  # Perform transformations (example: filter recent data)
  filtered_stream = sales_stream.select("*").where(col("timestamp") >= (current_timestamp() - F.expr("interval 1 hour")))

  # Write the real-time stream to a separate Delta table
  filtered_stream.writeStream\
                .format("delta")\
                .option("checkpointLocation", "/tmp/checkpoint")\
                .start("/delta/sales_stream")

# Call the speed layer function with Kafka details and topic name
speed_layer(spark, "localhost:9092", "sales_topic")





#3. Serving Layer (Delta Table + Interactive Query):
#This layer combines data from both Delta tables for querying.

def serving_layer(spark):
  # Read data from both Delta tables (bronze and silver)
  bronze_table = DeltaTable.forPath(spark, "/delta/sales")
  silver_table = DeltaTable.forPath(spark, "/delta/sales_stream")

  # Combine data using a view (might need optimization for larger datasets)
  spark.sql("""
    CREATE OR REPLACE TEMP VIEW all_sales
    USING DELTA LOCATION "/delta/sales"
  """)

  spark.sql("""
    CREATE OR REPLACE TEMP VIEW real_time_sales
    USING DELTA LOCATION "/delta/sales_stream"
  """)

  # Combine views for querying
  combined_df = spark.sql("SELECT * FROM all_sales UNION ALL SELECT * FROM real_time_sales")

  # Example query: Get total sales in the last hour (combines batch and streaming data)
  total_sales = combined_df.where(col("timestamp") >= (current_timestamp() - F.expr("interval 1 hour")))\
                             .agg(sum("price").alias("total_sales_last_hour"))
  total_sales.show()

# Call the serving layer function to execute queries
serving_layer(spark)
