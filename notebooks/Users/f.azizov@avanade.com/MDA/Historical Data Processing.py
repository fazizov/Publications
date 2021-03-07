# Databricks notebook source
# MAGIC %run ./MDA-Functions

# COMMAND ----------

resetETLLogs()

# COMMAND ----------

ingestBronzeTables(True,None,True)

# COMMAND ----------

