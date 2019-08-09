# %%
import json
from pyspark.sql import SparkSession
import os
os.environ['HADOOP_HOME'] = "C:\\hadoop-2.7.1"
print(os.environ['HADOOP_HOME'])
spark = SparkSession.builder.config(
    "spark.sql.warehouse.dir",
    "file:///E:/temp").appName("rdd_test").getOrCreate()
spark.conf.set("spark.executor.memory", "14g")
sc = spark.sparkContext
# %%
raw_data = sc.textFile("file:///E:/source_data/ol_cdump.json")
# %%
# %%
dataset = raw_data.map(json.loads)
# %%
dataset.persist()
# %%
dataset.count()
