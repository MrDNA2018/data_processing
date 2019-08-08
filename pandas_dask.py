
import dask
import dask.dataframe as dd
from dask.distributed import Client
client = Client(processes=False, threads_per_worker=4, n_workers=4, memory_limit='12GB')
#%%
df = dd.read_csv("F://total2.csv", blocksize=25e6,encoding='utf-8',dtype='object')
#%%
for i in df.columns:
    print("{}".format(df.head(1)[i]))

#%%
logs =  'Mismatched dtypes found in `pd.read_csv`/`pd.read_table`.\n\n+----------------------------------+--------+----------+\n| Column                           | Found  | Expected |\n+----------------------------------+--------+----------+\n| check.0.reportorphone            | object | float64  |\n| damagetypecode                   | object | float64  |\n| lossmain.0.handlercode           | object | float64  |\n| lossmain.0.repairbrandcode       | object | float64  |\n| lossmain.0.repairbrandname       | object | float64  |\n| lossmain.0.repairfactorycode     | object | float64  |\n| lossmain.0.repairfactoryname     | object | float64  |\n| lossthirdparty.0.insurecomcode   | object | float64  |\n| lossthirdparty.0.losscarkindname | object | float64  |\n| lossthirdparty.0.thirdcarlinker  | object | float64  |\n| lossthirdparty.0.vinno           | object | float64  |\n| phonenumber                      | object | int64    |\n| prplcitemcar.0.brandid           | object | float64  |\n| prplcitemcar.0.brandname1  '
print(logs)

#%%
# 去除空格
for i in df.columns:
    df[i] = df[i].apply(lambda x: str(x).strip(), meta=(i, 'f8'))

#%%
df.compute()