from pyspark import SparkContext
import pyspark.sql 
from pyspark.sql import SQLContext

spark = SparkContext(appName="PythonStreamingQueueStream")    

sqlContext = SQLContext(spark)

df = sqlContext.read.format("csv")\
		.option("header", "true")\
		.load("/Users/danielgarcia/Downloads/airports.csv")

df\
.filter(df.longitude < -120)\
.selectExpr(
    "concat(min(longitude), ' €') as `min-long`",
    "max(longitude) as `max-long`",
    "min(latitude) as `min-lat`",
    "max(latitude) as `max-lat`"
)\
.show()