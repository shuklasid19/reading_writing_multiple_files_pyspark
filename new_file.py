from pyspark.sql import *

#spark sesson
spark = SparkSession.builder.master('local[1]').getOrCreate()

#it will load file 
first_last = spark.read.parquet(r'C:\Users\sid\Downloads\code pil\Data_parquet\firstlast_name')

#it will show file
first_last.show()
first_last.printSchema()

small = spark.read.parquet(r'C:\Users\sid\Downloads\code pil\Data_parquet\small')
small.show()
small.printSchema()

small.write.mode('append').parquet(r'C:\Users\sid\Downloads\code pil\Data_parquet\firstlast_name')
small.show()

#it will overwrite whatever was in the file before
#small.write.mode('overwrite').parquet(r'C:\Users\sid\Downloads\code pil\Data_parquet\firstlast_name')
#small.show