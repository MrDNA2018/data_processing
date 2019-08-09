# -*- coding:utf-8 -*-
import re
import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
# %%
last = pd.read_csv(
    'E://source_data/last/last_formate.csv',
    encoding='utf-8',
    dtype='object',
    engine='c',
    error_bad_lines=False)
# %%


def ReTel(tn):
    reg1 = r"1[3|4|5|7|8][0-9]{9}"
    reg2 = r"\(?0\d{2,3}[)-]?\d{7,8}"
    result = re.findall(reg1, tn) or re.findall(reg2, tn)
    if len(result) == 0:
        return ''
    return result[0].replace(
        '-',
        '').replace(
        '(',
        '').replace(
            ')',
            '').replace(
                '（',
                '').replace(
                    '）',
        '')


# %%
def ReChinese(tn):
    reg1 = r"[\u4e00-\u9fa5]+"
    result = re.findall(reg1, tn)
    if len(result) == 0:
        return ''
    return result[0]

# %%


def ReID(tn):
    if tn == '111111111111111111':
        return ''
    reg1 = r"^[1-6]\d{5}[12]\d{3}(0[1-9]|1[12])(0[1-9]|1[0-9]|2[0-9]|3[01])\d{3}(\d|X|x)$"
    result = re.match(reg1, tn)
    if result is None:
        return ''
    return result[0]
# %%


def ReCarno(tn):
    reg1 = r"^[\u4e00-\u9fa5]{1}[A-Z]{1}[A-Z_0-9]{5}$"
    result = re.findall(reg1, tn)
    if len(result) == 0:
        return ''
    return result[0]
# %%


# %%
# %%
# 电话号码处理
number_check_list = [
    'reportornumber',
    'phonenumber',
    'll_thirdcarlinkernumber',
    'driverphoneno',
    'insuredphoneno',
    'insureds.0.mobile',
    'insureds.1.mobile']

for column_name in number_check_list:
    last[column_name] = last[column_name].apply(lambda i: ReTel(str(i)))

# %%
# 处理非汉字的名字
name_check_list = [
    'reportorname',
    'linkername',
    'll_thirdcarlinker',
    'driversname',
    'drivers.0.drivername',
    'drivers.1.drivername',
    'carowner',
    'll_carowner',
    'll_insuredname',
    'insureds.0.insuredname',
    'insureds.1.insuredname',
    'injureds.0.personname',
    'injureds.1.personname']

for column_name in name_check_list:
    last[column_name] = last[column_name].apply(lambda i: ReChinese(str(i)))

# %%
# 处理身份证
identifynumber_check_list = [
    'insureds.0.identifynumber',
    'insureds.1.identifynumber',
    'injureds.0.identifynumber',
    'injureds.1.identifynumber',
    'drivers.0.drivinglicenseno',
    'drivers.1.drivinglicenseno']

for column_name in identifynumber_check_list:
    last[column_name] = last[column_name].apply(lambda i: ReID(str(i)))

# %%
# 处理车牌号
carno_check_list = [
    'drivers.0.licenseno',
    'drivers.1.licenseno',
    'll_licenseno']

for column_name in carno_check_list:
    last[column_name] = last[column_name].apply(lambda i: ReCarno(str(i)))

# %%
last.to_csv('E://source_data/last/last_formate.csv', header=True, index=False)
