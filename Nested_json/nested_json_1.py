# Databricks notebook source
# read Json File
df = spark.read.format("json").option("multiline",True).load("dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/sample_nested.json")
display(df)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

# define Schema

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

country_schema1 = StructType([
    StructField("company",StringType(),True),
    StructField("id",IntegerType(),True)])

ele_schema1 = StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True)])

brewing_schema = StructType([
    StructField("country",StructType(country_schema1),True),
    StructField("region",ArrayType(ele_schema1),True)
])

country_schema2 = StructType([
    StructField("company",StringType(),True),
    StructField("id",IntegerType(),True)])

ele_schema2 = StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True)])

coffee_schema = StructType([
    StructField("country",StructType(country_schema2),True),
    StructField("region",ArrayType(ele_schema2),True)
])

# COMMAND ----------

# root_schema
root_schema = StructType([
    StructField("brewing",StructType(brewing_schema),True),
    StructField("coffee",StructType(coffee_schema),True)
])

# COMMAND ----------

# read json file with root schema

df_json = spark.read.schema(root_schema).option("multiline",True).json("dbfs:/FileStore/shared_uploads/harisumanjali328@outlook.com/sample_nested.json")
display(df_json)
df_json.show(truncate=False)
df_json.printSchema()

# COMMAND ----------

# flatten the data of StructType data

df1 = df_json.select("brewing.country.*","coffee.country.*")
display(df1)

# COMMAND ----------

# flatten data of ArrayType 

from pyspark.sql.functions import explode,col

df2 = df_json.withColumn("brew_region",explode(col("brewing.region"))).\
withColumn("coff_region",explode(col("coffee.region"))).withColumn("b_region_id",explode(col("brewing.region.id"))).\
withColumn("c_region_id",explode(col("coffee.region.id"))).withColumn("b_region_name",explode(col("brewing.region.name"))).\
withColumn("c_region_name",explode(col("coffee.region.name"))).drop("brewing","coffee")

display(df2)

# COMMAND ----------

