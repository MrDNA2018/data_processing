#%%
from pyspark.sql import SparkSession
import os
os.environ['HADOOP_HOME'] = "C:\\hadoop-2.7.1"
print(os.environ['HADOOP_HOME'])
spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///E:/temp").appName("rdd_test").getOrCreate()
spark.conf.set("spark.executor.memory", "12g")
sc = spark.sparkContext
#%%
df = spark.read.option("header", True).csv("file:///F:/total2.csv")

#%%
df.printSchema()

#%%
df.persist

#%%
# def delete_empty
# df.map(lambda x:)
# 重命名
for col in df.columns:
    print("{}---->{}".format(col,str(col).replace('.', '')))
    df = df.withColumnRenamed(col,str(col).replace('.', ''))

#%%
def delete_empty(test):
    if test is None:
        return None
    for col in df.columns:
        test[col] = test[col].strip()
    return test
#%%
df = df.foreach(lambda x:delete_empty(x))
#%%
# 设置为一个partition, 这样可以把输出文件合并成一个文件
df.coalesce(1) .write.mode("overwrite").format("csv").save("file:///F:/fun_test.csv")
#
