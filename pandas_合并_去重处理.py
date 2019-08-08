import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
#%%
new_demo = pd.read_csv('E://source_data/after_delete_empty_nulls/new_demo_delete_empty_lines_na-gt-limit_drop_duplicates.csv',encoding='utf-8',dtype='object',engine='c', error_bad_lines=False)
#%%
registnos = list(new_demo['registno'])
#%%
drivers = pd.read_csv('E://source_data/after_delete_empty_nulls/insured_utf-8_delete_empty_nulls.csv',encoding='utf-8', dtype='object',engine='c', error_bad_lines=False)
#%%
new_demo_drivers = pd.merge(new_demo, drivers, on='registno', how='left')

#%%
df_isna_sum = drivers.isna().sum()
# 获取空值大于阈值的字段
na_gt_limit_list = []
for i in df_isna_sum.index:
    if df_isna_sum[i] > len(drivers)*0.8:
        na_gt_limit_list.append(i)
#%%
# 打印一下
for i in na_gt_limit_list:
    print("{} : {}".format(i,df_isna_sum[i]))

#%%
# 删除空值大于阈值的字段
drivers = drivers.drop(columns=na_gt_limit_list)

#%%
# 保存备份数据
path = 'E://source_data/after_delete_empty_nulls/drivers_utf-8_delete_empty_nulls_na-gt-limit.csv'
drivers = pd.read_csv(path, encoding='utf-8', dtype='object',engine='c', error_bad_lines=False)
#%%
drivers = drivers.drop_duplicates()
#%%
drivers.to_csv('E://source_data/after_delete_empty_nulls/drivers_utf-8_delete_empty_nulls_na-gt-limit_drop_duplicates.csv',header=True,index=False)