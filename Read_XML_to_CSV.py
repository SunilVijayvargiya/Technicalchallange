# Databricks notebook source
dbutils.widgets.text("Path","","")
dbutils.widgets.get("Path")
Path=getArgument("Path")
print(Path)

dbutils.widgets.text("FileName","","")
dbutils.widgets.get("FileName")
FileName=getArgument("FileName")
print(FileName)

# COMMAND ----------

file_location = Path+"/"+ FileName +".xml"
print(file_location)
#dbfs:/mnt/NSAA/TextFile/RSSdatafeeder.xml

# COMMAND ----------

df = spark.read.text(file_location)

# COMMAND ----------

#to check row header
df.display()

# COMMAND ----------

top_story_df = spark.read.format("xml").option("rowTag", "item").load(file_location)

# COMMAND ----------

top_story_df.display()

# COMMAND ----------

file_location_csv = Path+"/XMLtoCSV"
top_story_df.select("title","link","pubDate").write.mode("append").format("com.databricks.spark.csv").option("header","true").csv(file_location_csv)
