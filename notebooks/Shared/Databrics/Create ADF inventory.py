# Databricks notebook source
# MAGIC %md #Create Azure Data Factory inventory using Data Bricks

# COMMAND ----------

# MAGIC %md ###Import required libraries

# COMMAND ----------

from pyspark.sql.functions import explode_outer,explode,col
from pyspark.sql.types import StringType,StructField,ArrayType

# COMMAND ----------

# MAGIC %md ####Read the arm template into the spark data frame and show its json schema

# COMMAND ----------

adfDoc=spark.read.option("multiline","true").json("/FileStore/tables/arm_template.json")
adfDoc.printSchema()

# COMMAND ----------

# MAGIC %md ####Explore different object types under the root resources node

# COMMAND ----------

rootObjects=adfDoc.select(explode("resources").alias("RootObjects"))
display(rootObjects.select("RootObjects.type").distinct())

# COMMAND ----------

# MAGIC %md #####Create & register name cleansing funtions

# COMMAND ----------

def cleanPipelineName(Name):
  if Name==None:   
    return None
  return (Name.split(",")[1].replace(")]","").replace("'",""))

cleanNameUDF=spark.udf.register("cleanNameSQL",cleanPipelineName,StringType())

# COMMAND ----------

def cleanseNamesArr(arr):
  cleansedArr=[]
  for Name in arr:
    if Name is not None:
      cleansedArr.append(Name.replace("concat(variables('factoryId')","").replace("[,","").replace(")]",""))
  return (cleansedArr)

cleanseNamesArrUDF=spark.udf.register("cleanseNamesArrSQL",cleanseNamesArr,ArrayType(StringType()))

# COMMAND ----------

# MAGIC %md ###1. Pipelines inventory 

# COMMAND ----------

# MAGIC %md #####Create pipelines data frame

# COMMAND ----------

dfPipelines=rootObjects.filter("RootObjects.type=='Microsoft.DataFactory/factories/pipelines'")\
.withColumnRenamed("RootObjects","Pipelines")
dfPipelines.printSchema()

# COMMAND ----------

# MAGIC %md #####Extract common pipelines attributes

# COMMAND ----------

dfPipelineMainDetails=dfPipelines.selectExpr("Pipelines.properties.folder.name as Folder",\
 "cleanNameSQL(Pipelines.name) as PipelineName",\
 "Pipelines.properties.description as Description",\
 "cast(cleanseNamesArrSQL(Pipelines.dependsOn) as string) as DependsOnObject",\
  "Pipelines.properties.parameters as Parameters","Pipelines")
dfPipelineMainDetails.printSchema()

# COMMAND ----------

# MAGIC %md #####Finalize pipelines inventory list

# COMMAND ----------

dfPipelineInventory=dfPipelineMainDetails\
.select("Folder","PipelineName","Description","DependsOnObject")\
.orderBy("Folder","PipelineName")
display(dfPipelineInventory)

# COMMAND ----------

# MAGIC %md ###2. Activities inventory 

# COMMAND ----------

# MAGIC %md #####Extract activities array

# COMMAND ----------

dfActivities=dfPipelineMainDetails.withColumn("ActivityDetails",explode("Pipelines.properties.activities"))\
.select("PipelineName","ActivityDetails")
dfActivities.printSchema()

# COMMAND ----------

# MAGIC %md #####Extract common activity attributes

# COMMAND ----------

dfActDetails=dfActivities.selectExpr("PipelineName","ActivityDetails.name as ActivityName",\
 "ActivityDetails.description as Description","ActivityDetails.type as ActivityType",\
 "ActivityDetails.typeProperties.storedProcedureName as StoredProcedureName",\
 "ActivityDetails.typeProperties.scriptPath as ScriptPath",\
 "ActivityDetails.linkedServiceName.referenceName as linkedServiceName",\
 "ActivityDetails.dependsOn as ActivityDependeniesRaw",\
 "ActivityDetails.typeProperties as ActivityTypePropertiesRaw")
dfActDetails.printSchema()

# COMMAND ----------

# MAGIC %md #####Add activity dependency details

# COMMAND ----------

dfActDetails_Dep=dfActDetails.withColumn("ActivityDependency",explode_outer("ActivityDependeniesRaw"))\
.selectExpr("*","ActivityDependency.activity as DependentOnActivity",\
"ActivityDependency.dependencyConditions as DependencyCondition")\
.withColumn("DependencyCondition",col("DependencyCondition")[0])\
.drop("ActivityDependency","ActivityDependeniesRaw")\
.orderBy("PipelineName","ActivityName","DependentOnActivity")
dfActDetails_Dep.printSchema()

# COMMAND ----------

# MAGIC %md #####Extract included activity details and get final activities inventory

# COMMAND ----------

dfActDetails_Inc=dfActDetails_Dep.withColumn("IncludedActivities",explode_outer("ActivityTypePropertiesRaw.activities"))\
.selectExpr("*","IncludedActivities.name as IncludedActName","IncludedActivities.type as IncludedActType",\
 "IncludedActivities.typeProperties.storedProcedureName as IncludedActivityStoredProcedureName",\
 "IncludedActivities.typeProperties.scriptPath as IncludedActivityScriptPath",\
 "IncludedActivities.linkedServiceName.referenceName as IncludedActivitylinkedServiceName")\
.drop("IncludedActivities","ActivityTypePropertiesRaw")\
.orderBy("PipelineName","ActivityName") 
display(dfActDetails_Inc)

# COMMAND ----------

# MAGIC %md ###3. Linked services inventory 

# COMMAND ----------

# MAGIC %md #####Extract linked service nodes

# COMMAND ----------

dfLinkedServices=rootObjects.filter("RootObjects.type=='Microsoft.DataFactory/factories/linkedServices'")\
.withColumnRenamed("RootObjects","LinkedServices")
dfLinkedServices.printSchema()

# COMMAND ----------

# MAGIC %md #####Get the linked service inventory

# COMMAND ----------

dfLinkedServicesDetails=dfLinkedServices.selectExpr("LinkedServices.properties.folder.name as Folder",\
"cleanNameSQL(LinkedServices.name) as LinkedServiceName","LinkedServices.properties.description as Description",\
"LinkedServices.properties.type as Type","cast(cleanseNamesArrSQL(LinkedServices.dependsOn) as string) as DependsOn")
display(dfLinkedServicesDetails)

# COMMAND ----------

# MAGIC %md ###4. Datasets inventory 

# COMMAND ----------

# MAGIC %md #####Extract dataset root nodes

# COMMAND ----------

dfDatasets=rootObjects.filter("RootObjects.type=='Microsoft.DataFactory/factories/datasets'").withColumnRenamed("RootObjects","Datasets")
dfDatasets.printSchema()

# COMMAND ----------

# MAGIC %md #####Get dataset inventory

# COMMAND ----------

dfDatasetDetails=dfDatasets.selectExpr("Datasets.properties.folder.name as Folder",\
"cleanNameSQL(Datasets.name) as DatasetName","Datasets.properties.description as Description",\
"Datasets.properties.type as Type","Datasets.properties.linkedServiceName.referenceName as LinkedServiceName",\
"cast(cleanseNamesArrSQL(Datasets.dependsOn) as string) as DependsOn")\
.orderBy("Folder","DatasetName")
display(dfDatasetDetails)

# COMMAND ----------

# MAGIC %md ###5. Triggers inventory 

# COMMAND ----------

# MAGIC %md #####Extract trigger root nodes

# COMMAND ----------

dfTriggers=rootObjects.filter("RootObjects.type=='Microsoft.DataFactory/factories/triggers'").withColumnRenamed("RootObjects","Triggers")
dfTriggers.printSchema()

# COMMAND ----------

# MAGIC %md #####Get triggers inventory

# COMMAND ----------

dfTriggerDetails=dfTriggers.selectExpr("cleanNameSQL(Triggers.name) as TriggerName",\
 "Triggers.properties.description as Description",\
 "Triggers.properties.type as Type","cast(cleanseNamesArrSQL(Triggers.dependsOn) as string) as DependsOn")\
.orderBy("TriggerName")
display(dfTriggerDetails)

# COMMAND ----------

dfPipelineDetails_Dep.write.format("csv").save("/FileStore/ADF/ADF_Pipelines.csv")