import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
# %%
last = pd.read_csv(
    'E://source_data/insured.csv',
    encoding='utf-8',
    dtype='object',
    engine='c',
    error_bad_lines=False)

# %%
# 获取所有的列名
column_name_list = []
for i in last.columns:
    column_name_list.append(i)
# %%
# 去除待补充
for column_name in column_name_list:
    last[column_name] = last[column_name].replace(
        '待补充',
        '').replace(
        '粤A12345',
        '').replace(
            '待补充',
            '').replace(
                '未知',
                '').replace(
                    '00000000000',
        '')
# %%
last.to_csv(
    'E://source_data/last/last_formate_2.csv',
    header=True,
    index=False)
