from pyspark.sql import *


spark = SparkSession.builder.master('local[1]').getOrCreate()
file_parq = spark.read.parquet('userdata1.parquet')

file_parq.show()

#create a tempview sql 

parquesql = file_parq.createOrReplaceTempView('parquesql')

#will show only first and last name columns from parquet file that we loaded
first_last_name = spark.sql('SELECT first_name, last_name from parquesql')

first_last_name.show()

first_last_name.write.parquet("firstlast_name")

first_last_name.show()

#will return all values in the parquesql

all_info = spark.sql('SELECT * from parquesql')

all_info.show()




#################################3
data = [("king ", "queen"),
       ("dog ", "bite"),
       ("listen ", "hear"),
       ("kung ", "fu"),
       ("hustle ", "bustle")]

columns =  ["first_name", "last_name"]
df = spark.createDataFrame(data, columns)

df.show()
#it will write the file and save type and schema


df.write.parquet('small')



#spark.read.parquet('first_last.parquet')