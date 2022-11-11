# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime


# COMMAND ----------

container = 'myConatiner'
storage_acct= 'myStroageAccount'

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_acct}.dfs.core.windows.net",
    "myStorageAccountKey")

# COMMAND ----------

base_date = f"{datetime.now().strftime('%Y')}/{datetime.now().strftime('%m')}/{datetime.now().strftime('%d')}"

# COMMAND ----------

# pull from alds into data frame
file_path = f"abfss://{container}@{storage_acct}.dfs.core.windows.net/raw/{base_date}/*/*/*.txt"
raw_df = (spark.read
     .json(file_path))
display(raw_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customer_messages (message String, recipient_id int, timestamp string, user_id string, value int);

# COMMAND ----------

raw_df.write.insertInto("customer_messages", overwrite = False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_messages
