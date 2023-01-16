# Databricks notebook source
# Load the Data

dbutils.fs.put("/Nested_json/nested_json.json","""[{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}]""",True)

# COMMAND ----------

# read the data
df = spark.read.option("multiline",True).json('/Nested_json/nested_json.json')
display(df)
df.printSchema()

# COMMAND ----------

# define Schema 
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,DoubleType

ele_schema = StructType([
    StructField("id",StringType(),True),
    StructField("type",StringType(),True)])

batter_schema = StructType([
    StructField("batter",ArrayType(ele_schema),True)])

t_ele_schema = StructType([
     StructField("id",StringType(),True),
    StructField("type",StringType(),True)])


root_schema = StructType([
    StructField("batters",StructType(batter_schema),True),
    StructField("id",StringType(),True),
    StructField("name",StringType(),True),
    StructField("ppu",DoubleType(),True),
    StructField("topping",ArrayType(t_ele_schema),True),
    StructField("type",StringType(),True)])

# COMMAND ----------

df_json = spark.read.option("multiline",True).schema(root_schema).json("/Nested_json/nested_json.json")
display(df_json)

# COMMAND ----------

# flatten the data 
from pyspark.sql.functions import explode,col


df1 = df_json.withColumn("batter_id",explode(col("batters.batter.id"))).withColumn("batter_type",explode(col("batters.batter.type"))).\
withColumn("topping_id",explode(col("topping.id"))).withColumn("topping_type",explode(col("topping.type"))).\
withColumn("id",col("id")).withColumn("name",col("name")).withColumn("ppu",col("ppu")).drop("batters","topping")
display(df1)




# COMMAND ----------

