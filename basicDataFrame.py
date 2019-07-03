from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


print("*" * 100)
df = spark.read.csv("/Users/danielgarcia/Downloads/online-retail-dataset.csv");
print (df.take(10)[1]["_c2"]);


print("*" * 100)

spark-submit --master yarn --deploy-mode cluster --num-executors 1 --driver-memory 2g --executor-memory 1g --executor-cores 1 