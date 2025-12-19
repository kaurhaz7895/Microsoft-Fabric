# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6f535fd5-bef8-4561-8749-0f305de9572d",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "fda9f6fb-9246-4947-8599-d182d5a7c5b3",
# META       "known_lakehouses": [
# META         {
# META           "id": "6f535fd5-bef8-4561-8749-0f305de9572d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH.sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/bronze/sales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze/sales.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.select("SalesOrderNumber","Quantity","UnitPrice");
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.select("SalesOrderNumber","Quantity");
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").saveAsTable("sales2");

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
