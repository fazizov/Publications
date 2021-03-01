# Databricks notebook source
# MAGIC %run ./MDA-Functions

# COMMAND ----------

createStorageMount()
InitializeMetadata()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from etl.TableNames order by schemaname,tableName

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from etl.TableFields order by schemaname,tableName,ColumnOrder