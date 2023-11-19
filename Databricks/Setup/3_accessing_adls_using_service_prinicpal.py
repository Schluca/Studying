# Databricks notebook source
# MAGIC %md
# MAGIC Accessing adls using a service principal
# MAGIC Todos:
# MAGIC - Get secrets from key vault
# MAGIC - Set spark config
# MAGIC - Assing role

# COMMAND ----------

# Get the secrets
client_id = dbutils.secrets.get('Formula1-Scope', 'formula1-dl-client-id')
tenant_id = dbutils.secrets.get('Formula1-Scope', 'formula1dl-tenant-id')
client_secret = dbutils.secrets.get('Formula1-Scope', 'formula1dl-client-secret')

# COMMAND ----------

# Set config for connecting
spark.conf.set("fs.azure.account.auth.type.formula1dlls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlls.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlls.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlls.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# List folders
dbutils.fs.ls("abfss://demo@formula1dlls.dfs.core.windows.net")

# COMMAND ----------

# Show file
display(spark.read.csv('abfss://demo@formula1dlls.dfs.core.windows.net/circuits.csv'))
