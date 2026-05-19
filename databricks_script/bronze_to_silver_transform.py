# Databricks notebook source
storage_account_name = "frauddatastorage"
container_name = "fraud-data-lake"
access_key = "/Myaq2Ok6LJ1CEbi..iam-SOMESH-N-THIS_KEY-WAS_mine-bro-use-your-own🙂"

# COMMAND ----------

# key passing on Direct options 
df = spark.read.format("csv") \
    .option("header", "true") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/your_file.csv")

# COMMAND ----------


# Exact file full path
batch_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/batch/api/v1/datasets/download/mlg-ulb/creditcardfraud/creditcard.csv"

df_batch = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .load(batch_path)

display(df_batch)

# COMMAND ----------

from pyspark.sql.functions import col, when

# 1. Duplicates remove 
df_clean = df_batch.dropDuplicates()

# 2. Risk level seting (Amount ko double me cast karke filter lagaya hai taaki safer rahe)
df_clean = df_clean.withColumn(
    "risk_level",
    when(col("Amount").cast("double") > 2000, "High")
    .when(col("Amount").cast("double") > 500, "Medium")
    .otherwise("Low")
)

# 3. Final transformation output 
display(df_clean)

# COMMAND ----------

# 1. define  path of Silver layer 
silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/processed_batch/"

# 2. Key configuration ke sath Parquet format me data write kiya 
df_clean.write \
    .mode("overwrite") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .parquet(silver_path)

# COMMAND ----------

# 1. seting up path
stream_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/streaming/"

# 2. JSON data read with Key configuratio  
df_stream = spark.read.format("json") \
    .option("inferSchema", "true") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .load(stream_path)

# 3. Output display (Isme transaction_id, user_id wale columns milenge)
display(df_stream)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, when, col

# 1. Processing timestamp added
df_stream_clean = df_stream.withColumn(
    "processed_time",
    current_timestamp()
)

# 2. Transaction segmentation login add kiya
df_stream_clean = df_stream_clean.withColumn(
    "transaction_category",
    when(col("amount") > 30000, "High Value")
    .when(col("amount") > 10000, "Medium Value")
    .otherwise("Low Value")
)

# 3. for checking the Transformed data
display(df_stream_clean)

# COMMAND ----------

# 1. Silver stream data ka path set kiya 
stream_silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/processed_streaming/"

# 2. Authenticated write execute karo
df_stream_clean.write \
    .mode("overwrite") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .parquet(stream_silver_path)

# COMMAND ----------

# 1. Fraud Summary Analytics calculate karo
fraud_summary = df_clean.groupBy("risk_level").count()

# 2. Output check karo
display(fraud_summary)

# COMMAND ----------

from pyspark.sql.functions import col

# 1. High Risk Transactions filter karo
high_risk_txns = df_stream_clean.filter(
    col("transaction_category") == "High Value"
)

# 2. Output check karo
display(high_risk_txns)

# COMMAND ----------

# 1. Location Wise Analysis calculate karo
location_analysis = df_stream_clean.groupBy("location").count()

# 2. Output check karo
display(location_analysis)

# COMMAND ----------

# Paths define karo
gold_fraud_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold/fraud_summary/"
gold_location_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold/location_analysis/"
gold_highrisk_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold/high_risk_transactions/"

# A. Fraud Summary ko Gold layer me write karo
fraud_summary.write.mode("overwrite") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .parquet(gold_fraud_path)

# B. Location Analysis ko Gold layer me write karo
location_analysis.write.mode("overwrite") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .parquet(gold_location_path)

# C. High Risk Transactions ko Gold layer me write karo
high_risk_txns.write.mode("overwrite") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key) \
    .parquet(gold_highrisk_path)

# COMMAND ----------

