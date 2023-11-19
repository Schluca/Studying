# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('Formula1-Scope')

# COMMAND ----------

dbutils.secrets.get('Formula1-Scope', 'formula1dl-account-key')

# COMMAND ----------


