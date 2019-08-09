import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
# %%
df = pd.read_csv(
    'E://source_data/new_demo.csv',
    encoding='utf-8',
    engine='python',
    error_bad_lines=False)

# %%
# %%
file = open(
    'E://source_data/insured.csv',
    'r',
    encoding='utf-8',
    errors='ignore')
# %%
df_chunk = pd.read_csv(
    file,
    encoding="utf-8",
    dtype='object',
    error_bad_lines=False,
    engine='c')

# %%
# 获取所有的列名
column_name_list = []
for i in df.columns:
    column_name_list.append(i)
# %%
# 去除空格,换行,不详
for column_name in column_name_list:
    df[column_name] = df[column_name].apply(
        lambda i: str(i).strip().replace(
            '\n',
            '').replace(
            ' ',
            '').replace(
                '不详',
                '').replace(
                    'NaN',
                    '').replace(
                        'nan',
            ''))

# %%
df = df.drop(columns=['_id', 'll_id'])
# %%
# 保存备份数据
df.to_csv(
    'E://source_data/new_demo_delete_empty_lines.csv',
    header=True,
    index=False)

# %%
df = pd.read_csv(
    'E://source_data/new_demo_delete_empty_lines.csv',
    encoding='utf-8',
    engine='python',
    error_bad_lines=False)
# %%
# 探索数据
df.describe()
# %%
df_isna_sum = df.isna().sum()
# %%
# 获取空值大于阈值的字段
na_gt_limit_list = []
for i in df_isna_sum.index:
    if df_isna_sum[i] > len(df) * 0.8:
        na_gt_limit_list.append(i)
# %%
# 打印一下
for i in na_gt_limit_list:
    print("{} : {}".format(i, df_isna_sum[i]))

# %%
# 删除空值大于阈值的字段
df = df.drop(columns=na_gt_limit_list)
# %%
# 保存备份数据
df.to_csv(
    'E://source_data/new_demo_delete_empty_lines_na-gt-limit.csv',
    header=True,
    index=False)
# %%
df = pd.read_csv(
    'E://source_data/new_demo_delete_empty_lines_na-gt-limit.csv',
    encoding='utf-8',
    engine='python',
    error_bad_lines=False)
# %%
# 移动数据到指定位置
remark = df.pop('remark')
df.insert(len(df.columns), 'remark', remark)
# %%
# 补全字段
