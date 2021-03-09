# Databricks notebook source
dbutils.widgets.text("processingDate", "2021-03-07","")
processingDate=dbutils.widgets.get("processingDate")

# COMMAND ----------

# MAGIC %run ./MDA-Functions

# COMMAND ----------

ingestBronzeTables(False,processingDate,False)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from bronze.SalesLTSalesOrderHeader 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AccountNumber,BillToAddressID,Comment,CreditCardApprovalCode,CustomerID,DueDate,Freight,ModifiedDate,OnlineOrderFlag,OrderDate,PurchaseOrderNumber,RevisionNumber,rowguid,SalesOrderID,SalesOrderNumber,ShipDate,ShipMethod,ShipToAddressID,Status,SubTotal,TaxAmt,TotalDue,SourceFileName FROM TempSource --WHERE ModifiedDate >='2021-03-07'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table bronze.SalesLTSalesOrderHeader

# COMMAND ----------

cleanDWHTables("BRONZE")

# COMMAND ----------

resetETLLogs()

# COMMAND ----------

sourcePath="/mnt/mda/source/Regular/20210307/SalesLTSalesOrderHeader.csv"
df=spark.read.csv(sourcePath,header=True)
display(df)

# COMMAND ----------

sourcePath="/mnt/mda/source/Historical/SalesLTSalesOrderHeader.csv"
df=spark.read.csv(sourcePath,header=True)
display(df)

# COMMAND ----------

