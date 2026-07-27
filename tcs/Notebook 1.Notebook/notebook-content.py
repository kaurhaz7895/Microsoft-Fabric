# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "274c7c1c-e010-4048-bf0f-082dfdff36e1",
# META       "default_lakehouse_name": "OrdersData",
# META       "default_lakehouse_workspace_id": "7bc2de5f-20a7-4104-9f29-337e431a585a",
# META       "known_lakehouses": [
# META         {
# META           "id": "274c7c1c-e010-4048-bf0f-082dfdff36e1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.read.format("csv").option("header","true").load("Files/orders/2019.csv")
# df now is a Spark DataFrame containing CSV data from "Files/orders/2019.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
