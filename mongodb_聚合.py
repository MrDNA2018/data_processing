# %%
from pymongo import MongoClient

client = MongoClient("mongodb://192.168.123.64:27017/")
database = client["gd_raw_data"]
cases = database["cases"]
drivers = database["drivers"]
injureds = database["injureds"]
insureds = database["insureds"]

pipeline = [
    {
        u"$lookup": {
            u"from": u"drivers",
            u"localField": u"registno",
            u"foreignField": u"registno",
            u"as": u"drivers"
        }
    },
    {
        "$lookup": {
            "from": "injureds",
            "localField": "registno",
            "foreignField": "registno",
            "as": "injureds"
        }
    },
    {
        "$lookup": {
            "from": "insureds",
            "localField": "registno",
            "foreignField": "registno",
            "as": "insureds"
        }
    },
    {
        u"$out": u"C_b"
    }
]

cursor = cases.aggregate(
    pipeline,
    allowDiskUse=False
)
