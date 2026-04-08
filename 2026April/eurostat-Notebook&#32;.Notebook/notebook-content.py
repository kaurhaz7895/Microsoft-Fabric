# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c0c8d9f4-8490-464d-a659-76f3f4bc64a8",
# META       "default_lakehouse_name": "Lab2LH",
# META       "default_lakehouse_workspace_id": "f55231f5-b7b3-41ff-b9ab-b3179866683e",
# META       "known_lakehouses": [
# META         {
# META           "id": "c0c8d9f4-8490-464d-a659-76f3f4bc64a8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

 %%code
    
 Download the following file from this URL:
    
 https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/proj_23np$defaultview/?format=TSV
     
 Then write the file to the default lakehouse into a folder named temp. Create the folder if it doesn't exist yet.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# WARNING: This code downloads data from the web and writes files to your Lakehouse. 
# It may overwrite existing data in the /lakehouse/default/Files/temp/ directory.

import os
import requests

# Define the download URL and Lakehouse temp folder path
url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/proj_23np$defaultview/?format=TSV"
lakehouse_temp_dir = "/lakehouse/default/Files/temp"
local_filename = "proj_23np.tsv"
local_filepath = os.path.join(lakehouse_temp_dir, local_filename)

# Create folder if it doesn't exist
os.makedirs(lakehouse_temp_dir, exist_ok=True)

# Download and write file
response = requests.get(url)
response.raise_for_status()  # Raise error if the download failed

with open(local_filepath, "wb") as f:
    f.write(response.content)

print(f"Downloaded file saved to {local_filepath}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Exploring data downloaded

# CELL ********************

 %%code
    
 Load the file 'Files/temp/proj_23np.tsv' into a spark dataframe.
    
 The fields are separated with a tab.
    
 Show the contents of the DataFrame using display method.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Read the TSV file into a Spark DataFrame and display its contents.
filePath = "Files/temp/"
fileName = "proj_23np.tsv"

spark_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .load(filePath + fileName)

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data: Split first field

# CELL ********************

 %%code
    
 From the currently loaded DataFrame, split the first field 'freq,projection,sex,age,unit,geo\TIME_PERIOD' using a comma into 6 separate fields. Then, remove the original column.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Split the giant first column into 6 separate fields, then drop the original column

from pyspark.sql.functions import split, col

# The column to split is named exactly as in the file header (verify this if needed)
original_col = "freq,projection,sex,age,unit,geo\\TIME_PERIOD"
new_cols = ["freq", "projection", "sex", "age", "unit", "geo"]

# Split column by comma into array, then extract elements
split_col = split(col(original_col), ",")

# Add new columns
for i, new_name in enumerate(new_cols):
    spark_df = spark_df.withColumn(new_name, split_col.getItem(i))

# Drop the original combined column
spark_df = spark_df.drop(original_col)

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data: remove few fields

# CELL ********************

 %%code
    
 From the currently loaded DataFrame, remove the fields 'freq', 'age', 'unit'.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Remove the fields 'freq', 'age', 'unit' from the DataFrame

fields_to_drop = ['freq', 'age', 'unit']
spark_df = spark_df.drop(*fields_to_drop)

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data: Reposition Fields

# CELL ********************

 %%code
    
 From the currently loaded DataFrame, the fields 'projection', 'sex', 'geo' should be positioned first.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Reorder the columns so 'projection', 'sex', and 'geo' are first

# Get current columns
current_cols = spark_df.columns

# Define the fields to be first 
priority_fields = ['projection', 'sex', 'geo']

# The rest of the columns, maintaining their original order and removing any duplicates
other_fields = [col for col in current_cols if col not in priority_fields]

# Concatenate the priority fields and the rest
new_order = priority_fields + other_fields

# Select columns in new order
spark_df = spark_df.select(new_order)

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data: Replace fields

# CELL ********************

 %%code
    
 The 'projection' field contains codes that should be replaced with the following values:
     _'BSL' -> 'Baseline projections'.
     _'LFRT' -> 'Sensitivity test: lower fertility'.
     _'LMRT' -> 'Sensitivity test: lower mortality'.
     _'HMIGR' -> 'Sensitivity test: higher migration'.
     _'LMIGR' -> 'Sensitivity test: lower migration'.
     _'NMIGR' -> 'Sensitivity test: no migration'.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

from pyspark.sql.functions import when

# Map the codes in 'projection' to their descriptive names.
spark_df = spark_df.withColumn(
    "projection",
    when(spark_df["projection"] == "BSL", "Baseline projections")
    .when(spark_df["projection"] == "LFRT", "Sensitivity test: lower fertility")
    .when(spark_df["projection"] == "LMRT", "Sensitivity test: lower mortality")
    .when(spark_df["projection"] == "HMIGR", "Sensitivity test: higher migration")
    .when(spark_df["projection"] == "LMIGR", "Sensitivity test: lower migration")
    .when(spark_df["projection"] == "NMIGR", "Sensitivity test: no migration")
    .otherwise(spark_df["projection"])
)

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform data: Filter records

# CELL ********************

 %%code
    
 Filter the 'geo' field and remove values 'EA20' and 'EU27_2020' (these are not countries).

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Filter out rows where 'geo' is 'EA20' or 'EU27_2020'

spark_df = spark_df.filter(~spark_df["geo"].isin(["EA20", "EU27_2020"]))

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
