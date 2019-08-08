# -*- coding:utf-8 -*-
import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
#%%
cases_fraud = pd.read_csv('E://source_data/0709_data/cases_fraud_3.csv',encoding='utf-8',dtype='object',engine='c', error_bad_lines=False)

#%%
# 处理同一行内的数据缺失补全

#%%
# cases_fraud.to_csv('E://source_data/0709_data/cases_fraud_3.csv',header=True,index=False)
insureds = cases_fraud[['insured','insuredphone','insuredidentifynumber']]
insureds.rename(columns={'insured':'name','insuredphone':'phone','insuredidentifynumber':'identifynumber'},inplace=True)
drivers = cases_fraud[['driver','driverphone']]
drivers.rename(columns={'driver':'name','driverphone':'phone'},inplace=True)
reportors = cases_fraud[['reportor','reportorphone']]
reportors.rename(columns={'reportor':'name','reportorphone':'phone'},inplace=True)
linkers = cases_fraud[['linker','linkerphone']]
linkers.rename(columns={'linker':'name','linkerphone':'phone'},inplace=True)
thirdpartylinkers = cases_fraud[['thirdpartylinker','thirdpartylinkerphone']]
thirdpartylinkers.rename(columns={'thirdpartylinker':'name','thirdpartylinkerphone':'phone'},inplace=True)
carowners = cases_fraud[['carowner','carownerphone']]
carowners.rename(columns={'carowner':'name','carownerphone':'phone'},inplace=True)

#%%
total_guys = pd.concat([insureds,drivers,reportors,linkers,thirdpartylinkers,carowners],ignore_index=True,sort=False)
#%%
total_guys =total_guys.dropna(how = 'all')
#%%
total_guys.drop_duplicates(keep='first',inplace=True)
#%%
total_guys.drop_duplicates(['name','phone'],keep='first',inplace=True)

#%%
total_guys.to_csv('E://source_data/0709_data/total_guys_3.csv',header=True,index=False)
#%%
total_guys = pd.read_csv('E://source_data/0709_data/total_guys_3.csv',encoding='utf-8',dtype='object',engine='c', error_bad_lines=False)
#%%
firstcars = cases_fraud[['carvinno','carno','carowner','carownerphone']]
thirdcars = cases_fraud[['thirdpartycarno']]
thirdcars.rename(columns={'thirdpartycarno':'carvinno'},inplace=True)
#%%
total_cars = pd.concat([firstcars,thirdcars],ignore_index=True,sort=False)
#%%
total_cars =total_cars.dropna(how = 'all')
#%%
total_cars.drop_duplicates(keep='first',inplace=True)
#%%
total_cars.drop_duplicates(['carvinno','carno'],keep='first',inplace=True)


#%%
total_cars.to_csv('E://source_data/0709_data/total_cars_1.csv',header=True,index=False)
#%%
total_cars = pd.read_csv('E://source_data/0709_data/total_cars_1.csv',encoding='utf-8',dtype='object',engine='c', error_bad_lines=False)


