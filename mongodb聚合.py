# %%
from pymongo import MongoClient

client = MongoClient("mongodb://192.168.123.64:27017/")
database = client["gd_raw_data"]
collection = database["prplregist"]
prplregistex = client["gd_raw_data"]['prplregistex']
pipeline = [
    {
        u"$lookup": {
            u"from": u"prplregistex",
            u"localField": u"registno",
            u"foreignField": u"registno",
            u"as": u"prplregistex"
        }
    },
    {
        u"$out": u"C_b"
    }
]

cursor = collection.aggregate(
    pipeline,
    allowDiskUse=False
)

# %%
db.getCollection("main").aggregate(
    [
        {
            "$lookup": {
                "from": "registex",
                "localField": "registno",
                "foreignField": "registno",
                "as": "registex"
            }
        },
        {
            "$lookup": {
                "from": "regist",
                "localField": "registno",
                "foreignField": "registno",
                "as": "regist"
            }
        },
        {"$out" 	: "C_b"}
    ],
    {
        "allowDiskUse": true
    }
)
