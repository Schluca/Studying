# Databricks notebook source
# MAGIC %md
# MAGIC Todos:
# MAGIC - Set Spark config using sas token
# MAGIC - List files in the container
# MAGIC - Read file

# COMMAND ----------

# Get sas token
blob_sas_token = dbutils.secrets.get('Formula1-Scope', 'formula1dl-sas-token')

# COMMAND ----------

# Set config for accessing
spark.conf.set("fs.azure.account.auth.type.formula1dlls.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlls.dfs.core.windows.net", blob_sas_token)

# COMMAND ----------

# List files
dbutils.fs.ls("abfss://demo@formula1dlls.dfs.core.windows.net")

# COMMAND ----------

# Load file
display(spark.read.csv('abfss://demo@formula1dlls.dfs.core.windows.net/circuits.csv'))
