# %%
from pyspark import SparkContext, SparkConf
import os
os.environ['HADOOP_HOME'] = "C:\\hadoop-2.7.1"
print(os.environ['HADOOP_HOME'])
conf = SparkConf().setMaster("local").setAppName("wordcount").set(
    "spark.executor.memory", "13g").set("spark.executor.cores", "5")
sc = SparkContext(conf=conf)
# %%
text_file = sc.textFile(
    "file:///C:/Users/qbt/Desktop/2015_2017_gd_raw.total.csv", 5)
# %%
wordcount = text_file.flatMap(
    lambda line: line.split(",")).map(
        lambda word: (
            word,
            1)).reduceByKey(
                lambda a,
    b: a + b)
wordcount.cache()
# %%
wordcount.count()
