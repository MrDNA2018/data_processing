#%%
from pyspark.sql import SparkSession
import os
os.environ['HADOOP_HOME'] = "C:\\hadoop-2.7.1"
print(os.environ['HADOOP_HOME'])
spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///E:/temp").appName("rdd_test").getOrCreate()
spark.conf.set("spark.executor.memory", "14g")
sc = spark.sparkContext
#%%
df = spark.read.json("file:///F:/employees_singleLine.json")
#%%
# raw_data = sc.textFile("file:///F:/total1.json")
raw_data = sc.textFile("file:///F:/employees_singleLine.json")
#%%
import json
def json_loads_and_extraction(line):
    line = json.loads(line)
    new_line = {}
    new_line['registno'] = line['registno']
    new_line['reportdate'] = line['reportdate']
    new_line['reporthour'] = line['reporthour']
    new_line['damagename'] = line['damagename']
    new_line['damagecode'] = line['damagecode']
    new_line['damageaddress'] = line['damageaddress']
    new_line['damageaddresscode'] = line['damageaddresscode']
    new_line['remark'] = line['remark']
    new_line['reportorname'] = line['reportorname']
    new_line['reportornumber'] = line['reportornumber']
    new_line['reportormobile'] = line['reportormobile']
    new_line['linkername'] = line['linkername']
    new_line['linkermobile'] = line['linkermobile']
    new_line['relationship'] = line['relationship']

    new_line['prplregistex'] = [{'driversname': i['driversname'],'driverssex': i['driverssex'],
                                 'drivingno': i['drivingno'],'drivertype': i['drivertype'],
                                 'drivercareer': i['drivercareer'],
                                 'driverlevel': i['driverlevel'],
                                 'years': i['years'],'driverphoneno': i['driverphoneno'],
                                 'driverrelationship': i['driverrelationship']} for i in line['prplregistex']]

    new_line['repairfee'] = [{'repairfactoryname': i['repairfactoryname'],'repairfactorycode': i['repairfactorycode'],
                                 'sumveriloss': i['sumveriloss'],'kindcode': i['kindcode'],
                                 'compname': i['compname'],'compcode': i['compcode'],
                                 'complevelname': i['complevelname'],'complevelcode': i['complevelcode']} for i in line['repairfee']]

    new_line['check'] = [{'damagename': i['damagename'],'damagecode': i['damagecode'],
                                 'damagetypename': i['damagetypename'],'damagetypecode': i['damagetypecode'],
                                 'damagestartdate': i['damagestartdate'],'damagestarthour': i['damagestarthour'],
                                 'damageenddate': i['damageenddate'],'damageendhour': i['damageendhour'],
                                 'damageaddress': i['damageaddress'],'reportorname': i['reportorname'],
                                 'reportorphone': i['reportorphone'] } for i in line['check']]

    new_line['lossmain'] = [{'lossfeetype': i['lossfeetype'],'riskcode': i['riskcode'],
                                 'insuredname': i['insuredname'],'losslevel': i['losslevel'],
                                 'repairfactorycode': i['repairfactorycode'],'repairfactoryname': i['repairfactoryname'],
                                 'repairfactorytype': i['repairfactorytype'],'repairbrandcode': i['repairbrandcode'],
                                 'repairbrandname': i['repairbrandname'],'repairbrandmanhour': i['repairbrandmanhour'],
                                 'repairorgcode': i['repairorgcode'],'comcode': i['comcode'],
                                 'handlercode': i['handlercode'],'deflossdate': i['deflossdate'],
                                 'sumrepairfee': i['sumrepairfee'],'sumlossfee': i['sumlossfee']} for i in line['lossmain']]

    new_line['lossthirdparty'] = [{'prplthirdpartyid': i['prplthirdpartyid'],'startdate': i['startdate'],
                                 'starthour': i['starthour'],'enddate': i['enddate'],
                                 'endhour': i['endhour'],'lossitemtype': i['lossitemtype'],
                                 'clausetype': i['clausetype'],'licensetype': i['licensetype'],
                                 'licenseno': i['licenseno'],'licensecolorcode': i['licensecolorcode'],
                                 'carkindcode': i['carkindcode'],'frameno': i['frameno'],
                                 'losscarkindcode': i['losscarkindcode'],'losscarkindname': i['losscarkindname'],
                                 'brandname': i['brandname'],'useyears': i['useyears'],
                                 'carowner': i['carowner'],'insuredflag': i['insuredflag'],
                                 'insurecomcode': i['insurecomcode'],'insurecomname': i['insurecomname'],
                                 'vinno': i['vinno'],'ciindemduty': i['ciindemduty'],
                                 'dutypercent': i['dutypercent'],'purchaseprice': i['purchaseprice'],
                                 'actualvalue': i['actualvalue'],'thirdcarlinker': i['thirdcarlinker'],
                                 'thirdcarlinkernumber': i['thirdcarlinkernumber']} for i in line['lossthirdparty']]

    new_line['prplcitemcar'] = [{'riskcode': i['riskcode'],'insuredtypecode': i['insuredtypecode'],
                                 'carinsuredrelation': i['carinsuredrelation'],'carowner': i['carowner'],
                                 'clausetype': i['clausetype'],'licenseno': i['licenseno'],
                                 'licensetype': i['licensetype'],'licensecolorcode': i['licensecolorcode'],
                                 'carkindcode': i['carkindcode'],'engineno': i['engineno'],
                                 'vinno': i['vinno'],'frameno': i['frameno'],
                                 'runareacode': i['runareacode'],'runareaname': i['runareaname'],
                                 'runmiles': i['runmiles'],'useyears': i['useyears'],
                                 'modelcode': i['modelcode'],'brandname': i['brandname'],
                                 'owneraddress': i['owneraddress'],'colorcode': i['colorcode'],
                                 'purchaseprice': i['purchaseprice'],'actualvalue': i['actualvalue'],
                                 'insurercode': i['insurercode'],'brandid': i['brandid'],
                                 'brandname1': i['brandname1']} for i in line['prplcitemcar']]

    new_line['citemkind'] = [{'kindname': i['kindname'],'kindcode': i['kindcode'],
                                 'itemdetailname': i['itemdetailname'],'itemcode': i['itemcode'],
                                 'startdate': i['startdate'],'starthour': i['starthour'],
                                 'enddate': i['enddate'],'endhour': i['endhour'],
                                 'value': i['value'],'amount': i['amount'],
                                 'discount': i['discount']} for i in line['citemkind']]

    return new_line

#%%

# dataset = raw_data.map(lambda line: json.loads(line))

# dataset = raw_data.map(lambda x: x.encode('utf-8','ignore').strip(u",\r\n[]\ufeff"))
dataset = raw_data.map(lambda line: json_loads_and_extraction(line))
#%%
df = spark.read.json(dataset)
#%%
# dataset.persist()
#%%
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

schema = StructType([StructField(str(i), StringType(), True) for i in range(21)])
#%%
df = spark.createDataFrame(dataset, schema)