#%%
# Requires pymongo 3.6.0+
from pymongo import MongoClient

client = MongoClient("mongodb://192.168.123.64:27017/")
database = client["SNA_0806"]
collection = database["c_out"]

# Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

query = {"registno" : "RDZA201544520000030555"}

cursor = collection.find(query).limit(1)
try:
    for doc in cursor:
        aa = ''
        for a in doc.keys():
            aa = aa + a + ','
        print(aa)
finally:
    client.close()
