# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9775fcf3-62c8-459a-886a-3dd89072bb7d",
# META       "default_lakehouse_name": "eurostatLH",
# META       "default_lakehouse_workspace_id": "ebe99ab4-7bb0-46c6-979a-9fff8c840ba6",
# META       "known_lakehouses": [
# META         {
# META           "id": "9775fcf3-62c8-459a-886a-3dd89072bb7d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%code
# MAGIC     
# MAGIC Download the following file from this URL:
# MAGIC     
# MAGIC  https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/proj_23np$defaultview/?format=TSV
# MAGIC      
# MAGIC  Then write the file to the default lakehouse into a folder named temp. Create the folder if it doesn't exist yet.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 #### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
    
 import requests
 import os
    
 # Define the URL and the local path
 url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/proj_23np$defaultview/?format=TSV"
 local_path = "/lakehouse/default/Files/temp/"
 file_name = "proj_23np.tsv"
 file_path = os.path.join(local_path, file_name)
    
 # Create the temporary directory if it doesn't exist
 if not os.path.exists(local_path):
     os.makedirs(local_path)
    
 # Download the file
 response = requests.get(url)
 response.raise_for_status()  # Check that the request was successful
    
 # Write the content to the file
 with open(file_path, "wb") as file:
     file.write(response.content)
    
 print(f"File downloaded and saved to {file_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Load the TSV file into a Spark DataFrame
file_path = "Files/temp/proj_23np.tsv"
spark_df = spark.read.format("csv").option("header", "true").option("sep", "\t").load(file_path)

display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

from pyspark.sql.functions import split, col

# The column name as expected per the file's structure
source_col = 'freq,projection,sex,age,unit,geo\\TIME_PERIOD'

# Split the column into 6 new columns and remove the original column
spark_df_split = spark_df.withColumn("freq", split(col(source_col), ",").getItem(0)) \
    .withColumn("projection", split(col(source_col), ",").getItem(1)) \
    .withColumn("sex", split(col(source_col), ",").getItem(2)) \
    .withColumn("age", split(col(source_col), ",").getItem(3)) \
    .withColumn("unit", split(col(source_col), ",").getItem(4)) \
    .withColumn("geo_TIME_PERIOD", split(col(source_col), ",").getItem(5)) \
    .drop(source_col)

display(spark_df_split)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Remove the columns 'freq', 'age', 'unit' from spark_df_split
spark_df_selected = spark_df_split.drop('freq', 'age', 'unit')

display(spark_df_selected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Reorder columns so that projection, sex, geo are first
# We'll assume the cleaned geo name is 'geo_TIME_PERIOD' as in previous steps

desired_first = ["projection", "sex", "geo_TIME_PERIOD"]
remaining_cols = [col for col in spark_df_selected.columns if col not in desired_first]
final_col_order = desired_first + remaining_cols

spark_df_reordered = spark_df_selected.select(final_col_order)

display(spark_df_reordered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Mapping codes to their descriptions
spark_df_replaced = spark_df_reordered.withColumn(
    "projection", 
    when(spark_df_reordered["projection"] == "BSL", "Baseline projections")
    .when(spark_df_reordered["projection"] == "LFRT", "Sensitivity test: lower fertility")
    .when(spark_df_reordered["projection"] == "LMRT", "Sensitivity test: lower mortality")
    .when(spark_df_reordered["projection"] == "HMIGR", "Sensitivity test: higher migration")
    .when(spark_df_reordered["projection"] == "LMIGR", "Sensitivity test: lower migration")
    .when(spark_df_reordered["projection"] == "NMIGR", "Sensitivity test: no migration")
    .otherwise(spark_df_reordered["projection"])
)

display(spark_df_replaced)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# Remove rows where 'geo_TIME_PERIOD' (assumed to be the geo field) is 'EA20' or 'EU27_2020'
filtered_df = spark_df_replaced.filter(~spark_df_replaced["geo_TIME_PERIOD"].isin(["EA20", "EU27_2020"]))

display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%code
    
 Filter the 'sex' field and remove 'T' (these are totals).

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Remove rows where the 'sex' field = 'T'
filtered_no_total = filtered_df.filter(filtered_df["sex"] != "T")

display(filtered_no_total)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%code
    
 Strip spaces from all field names in the dataframe.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Strip spaces from all column names in the dataframe
stripped_cols = [col.strip() for col in filtered_no_total.columns]
filtered_no_total_stripped = filtered_no_total.toDF(*stripped_cols)

display(filtered_no_total_stripped)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%code
    
 Convert the data type of all the year fields to integer.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 #### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
    
 from pyspark.sql.functions import col
    
 # Convert the data type of all the year fields to integer
 year_columns = [col(column).cast("int") for column in spark_df.columns if column.strip().isdigit()]
 spark_df = spark_df.select(*spark_df.columns[:3], *year_columns)
    
 # Display the updated DataFrame
 display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%code
    
 Save the dataframe as a new table named 'Population' in the default lakehouse.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Save the current DataFrame as a new table named 'Population' in the default lakehouse
converted_df.write.format("delta").saveAsTable("Population")
# If you want to overwrite the table in the future, use:
# converted_df.write.mode("overwrite").format("delta").saveAsTable("Population")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%code
# MAGIC  What are the projected population trends for geo BE  from 2020 to 2050 as a line chart visualization. Make sure to sum up male and female numbers. Use only existing columns from the population table. Perform the query using SQL.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Retrieve and sum up population for BE by year (summing F and M), then plot as a line chart

# SQL: We'll use Spark SQL to unpivot (melt) the wide-format table and aggregate
population_sql = """
SELECT
  CAST(year AS INT) AS year,
  SUM(value) AS population
FROM (
  SELECT
    sex,
    UNPIVOT (
      value FOR year IN (`2022`, `2023`, `2025`, `2030`, `2035`, `2040`, `2045`, `2050`)
    ) 
    AS u
  FROM population
  WHERE geo_TIME_PERIOD = 'BE'
    AND projection = 'Baseline projections'
    AND sex IN ('F', 'M')
) as base
GROUP BY year
ORDER BY year
"""

# Since Spark SQL does not directly support UNPIVOT, use selectExpr with stack:
years = ["2022", "2023", "2025", "2030", "2035", "2040", "2045", "2050"]
expr = "stack({0}, {1}) as (year, value)".format(
    len(years),
    ",".join(["'{0}', `{0}`".format(y) for y in years])
)

pop_df = spark.read.table("population").filter(
    (col("geo_TIME_PERIOD") == "BE") &
    (col("projection") == "Baseline projections") &
    (col("sex").isin("F", "M"))
)
melted = pop_df.selectExpr("sex", expr)
result = melted.groupBy("year").sum("value").orderBy("year").withColumnRenamed("sum(value)", "population")

# Plot result as a line chart using matplotlib
import matplotlib.pyplot as plt

pandas_df = result.toPandas()
plt.figure(figsize=(10,6))
plt.plot(pandas_df["year"], pandas_df["population"], marker="o")
plt.title("Projected Population Trends for Belgium (BE) 2022-2050")
plt.xlabel("Year")
plt.ylabel("Population")
plt.grid(True)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
