# Databricks notebook source
# MAGIC %md
# MAGIC Todos:
# MAGIC - Set Spark config
# MAGIC - List files in the container
# MAGIC - Read file

# COMMAND ----------

# Set the config to access the Azure storage account
spark.conf.set(
    "fs.azure.account.key.formula1dlls.dfs.core.windows.net",
    dbutils.secrets.get('Formula1-Scope', 'formula1dl-account-key')
)

# COMMAND ----------

# List all files in the demo folder
dbutils.fs.ls("abfss://demo@formula1dlls.dfs.core.windows.net")


# COMMAND ----------

# Load circuits.csv from demo folder
display(spark.read.csv('abfss://demo@formula1dlls.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------


