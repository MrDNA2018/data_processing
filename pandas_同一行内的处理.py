# -*- coding:utf-8 -*-
import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
# %%
last = pd.read_csv(
    'E://source_data/last/last_formate_15.csv',
    encoding='utf-8',
    dtype='object',
    engine='c',
    error_bad_lines=False)
# %%
# 处理同一行内的数据缺失补全


def testfunc_reportnumber(line):
    # if pd.isna(line['reportornumber']):
    #     if line['reportorname'] == line['linkername']:
    #         line['reportornumber'] = line['phonenumber']
    # if pd.isna(line['reportorname']):
    #     if line['reportornumber'] == line['phonenumber']:
    #         line['reportorname'] = line['linkername']
    # if pd.isna(line['linkername']):
    #     if line['phonenumber'] == line['reportornumber']:
    #         line['linkername'] = line['reportorname']
    # if pd.isna(line['phonenumber']):
    #     if line['linkername'] == line['reportorname']:
    #         line['phonenumber'] = line['reportornumber']
    # if pd.isna(line['driverphoneno']):
    #     if line['driversname'] == line['reportorname']:
    #         line['driverphoneno'] = line['reportornumber']
    #     if line['driversname'] == line['linkername']:
    #         line['driverphoneno'] = line['phonenumber']
    # if pd.isna(line['driversname']):
    #     if line['driverphoneno'] == line['reportornumber']:
    #         line['driversname'] = line['reportorname']
    #     if line['driverphoneno'] == line['phonenumber']:
    #         line['driversname'] = line['linkername']
    # if line['drivers.0.drivername'] == line['driversname']:
    #     line['drivers.0.drivername'] = 'firstdriver'
    # if line['drivers.1.drivername'] == line['driversname']:
    #     line['drivers.1.drivername'] = 'firstdriver'
    # if pd.isna(line['ll_licenseno']):
    #     if line['drivers.0.drivername'] == 'firstdriver':
    #         line['ll_licenseno'] = line['drivers.0.licenseno']
    #     if line['drivers.1.drivername'] == 'firstdriver':
    #         line['ll_licenseno'] = line['drivers.1.licenseno']
    # if line['thirdcardrivername'] == 'firstdriver':
    #     line['thirdcardrivername'] = ''
    #     line['thirdcarlicenseno'] = ''
    # if line['drivers.1.drivername'] == 'firstdriver':
    #     line['drivers.1.drivername'] = ''
    #     line['drivers.1.licenseno'] = ''
    # if line['thirdcarlicenseno'] == line['ll_licenseno']:
    #     line['thirdcarlicenseno'] = ''
    # if line['drivers.1.licenseno'] == line['ll_licenseno']:
    #     line['drivers.1.licenseno'] = ''
    # if not pd.isna(line['drivers.1.licenseno']):
    #     line['thirdcarlicenseno'] = line['drivers.1.licenseno']
    # if not pd.isna(line['drivers.1.drivername']):
    #     line['thirdcardrivername'] = line['drivers.1.drivername']
    # if not pd.isna(line['thirdcardrivername']):
    #     if line['thirdcardrivername'] == line['ll_thirdcarlinker']:
    #         line['thirdcardriverphone'] = line['ll_thirdcarlinkernumber']
    # if pd.isna(line['carownerphone']):
        # if line['carowner'] == line['reportorname']:
        #     line['carownerphone'] = line['reportornumber']
        # if line['carowner'] == line['linkername']:
        #     line['carownerphone'] = line['phonenumber']
        # if line['carowner'] == line['driversname']:
        #     line['carownerphone'] = line['driverphoneno']
    # if pd.isna(line['ll_vinno']):
    #     line['ll_vinno'] = line['ll_frameno']
    # if pd.isna(line['insureds.1.identifynumber']):
    #     if line['insureds.1.identifynumber'] == line['insureds.0.identifynumber']:
    #         line['insureds.1.identifynumber'] = ''
    #         line['insureds.1.insuredname '] = ''
    #
    #         if pd.isna(line['insureds.0.mobile']):
    #             line['insureds.0.mobile'] = line['insureds.1.mobile']
    #             line['insureds.1.mobile'] = ''
    # if not pd.isna(line['insureds.1.insuredname']):
    #     if line['insureds.1.insuredname'] == line['insureds.0.insuredname']:
    #         line['insureds.1.identifynumber'] = ''
    #         line['insureds.1.insuredname'] = ''
    #
    #         if pd.isna(line['insureds.0.mobile']):
    #             line['insureds.0.mobile'] = line['insureds.1.mobile']
    #             line['insureds.1.mobile'] = ''
    # if not pd.isna(line['insureds.1.mobile']):
    #     if line['insureds.1.mobile'] == line['insureds.0.mobile']:
    #         line['insureds.1.mobile'] = ''
    # if not pd.isna(line['insuredphoneno']):
    #     if line['insuredphoneno'] == line['insureds.0.mobile'] or line['insuredphoneno'] == line['insureds.1.mobile']:
    #         line['insuredphoneno'] = ''
    # if not pd.isna(line['insuredphoneno']):
    #     if pd.isna(line['insureds.0.mobile']):
    #         line['insureds.0.mobile'] = line['insuredphoneno']
    #         line['insuredphoneno'] = ''
    # if not pd.isna(line['insuredphoneno']):
    #     if pd.isna(line['insureds.1.mobile']):
    #         line['insureds.1.mobile'] = line['insuredphoneno']
    #         line['insuredphoneno'] = ''
    temp = set()
    if not pd.isna(line['insuredphoneno']):
        temp.add(line['insuredphoneno'])
    if not pd.isna(line['insureds.0.mobile']):
        temp.add(line['insureds.0.mobile'])
    if not pd.isna(line['insureds.1.mobile']):
        temp.add(line['insureds.1.mobile'])
    line['insuredphonelist'] = temp

    return line


# %%
last = last.apply(testfunc_reportnumber, axis=1)

# %%
# 更改列名
last.rename(columns={'drivers.0.licenseno': 'thirdcarlicenseno'}, inplace=True)
last.rename(
    columns={
        'drivers.0.drivername': 'thirdcardrivername'},
    inplace=True)
# %%
last.rename(columns={'ll_insuredname': 'insuredphonelist'}, inplace=True)
last['insuredphonelist'] = ''
# %%
last = last.drop(
    columns=[
        'insureds.1.insuredname',
        'insureds.1.identifynumber'])
# %%
# 增加一列
remark = last['phonenumber']
last.insert(11, 'thirdcardriverphone', remark)
# %%
# 查看非空元素
last[['insuredphoneno', 'insureds.0.mobile', 'insureds.1.mobile']
     ][last['insuredphoneno'].notna()]

# %%
last['thirdcardriverphone'] = ''
# %%
# 获取所有的列名
column_name_list = []
for i in last.columns:
    column_name_list.append(i)

# %%
last.to_csv(
    'E://source_data/last/last_formate_16.csv',
    header=True,
    index=False)
