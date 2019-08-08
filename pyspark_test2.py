#%%
from pyspark.sql import SparkSession
import os
os.environ['HADOOP_HOME'] = "C:\\hadoop-2.7.1"
print(os.environ['HADOOP_HOME'])
spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///E:/temp").appName("rdd_test").getOrCreate()
spark.conf.set("spark.executor.memory", "14g")
sc = spark.sparkContext
#%%
dfjs=spark.read.json("file:///E:/source_data/total.json")
# text_file = sc.textFile("file:///E:/source_data/total.json")
# text_file = sc.textFile("file:///C:/Users/qbt/Desktop/2015_2017_gd_raw.total.csv")
#%%
# text_file.take(2)
#%%
# text_file.count()
