# %%
from pymongo import MongoClient

client = MongoClient("mongodb://192.168.123.64:27017/")
temp = client["gd_raw_data"]["temp"]
prplregistex = client["gd_raw_data"]["prplregistex"]
repairfee = client["gd_raw_data"]["repairfee"]
prplcitemcar = client["gd_raw_data"]["prplcitemcar"]
lossthirdparty_lossmain = client["gd_raw_data"]["lossthirdparty_lossmain"]
lossthirdparty = client["gd_raw_data"]["lossthirdparty"]
lossmain = client["gd_raw_data"]["lossmain"]
citemkind = client["gd_raw_data"]["citemkind"]
check = client["gd_raw_data"]["check"]

query = {}
cursor = temp.find(query, no_cursor_timeout=True)
try:
    i = 0
    for doc in cursor:
        registno = doc['registno']
        print("报案号：{}".format(registno))
        prplregistex_info = prplregistex.find_one(
            {"registno": registno}, no_cursor_timeout=True)
        repairfee_info = repairfee.find_one(
            {"registno": registno}, no_cursor_timeout=True)
        prplcitemcar_info = prplcitemcar.find_one(
            {"registno": registno}, no_cursor_timeout=True)
        lossthirdparty_lossmain_info = lossthirdparty_lossmain.find_one(
            {"registno": registno}, no_cursor_timeout=True)
        lossthirdparty_info = lossthirdparty.find_one(
            {"registno": registno}, no_cursor_timeout=True)
        lossmain_info = lossmain.find_one(
            {"registno": registno}, no_cursor_timeout=True)
        citemkind_info = citemkind.find_one(
            {"registno": registno}, no_cursor_timeout=True)
        check_info = check.find_one(
            {"registno": registno}, no_cursor_timeout=True)

        newvalues = {
            "$set": {
                "prplregistex_info": prplregistex_info,
                "repairfee_info": repairfee_info,
                "prplcitemcar_info": prplcitemcar_info,
                "lossthirdparty_lossmain_info": lossthirdparty_lossmain_info,
                "lossthirdparty_info": lossthirdparty_info,
                "lossmain_info": lossmain_info,
                "citemkind_info": citemkind_info,
                "check_info": check_info}}
        temp.update_one({"registno": registno}, newvalues)


finally:
    client.close()
