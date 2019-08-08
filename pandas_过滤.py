import pandas as pd
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
#%%
new_demo = pd.read_csv('E://source_data/after_delete_empty_nulls/new_demo_delete_empty_lines_na-gt-limit_drop_duplicates.csv',encoding='utf-8',dtype='object',engine='c', error_bad_lines=False)
#%%
registnos = list(new_demo['registno'])
#%%
drivers = pd.read_csv('E://source_data/after_delete_empty_nulls/drivers_utf-8_delete_empty_nulls_na-gt-limit_drop_duplicates.csv',encoding='utf-8', dtype='object',engine='c', error_bad_lines=False)
# #%%
# drivers = drivers.drop_duplicates(['registno','mobile','insuredname'])
#
# #%%
# drivers = drivers.drop(columns=['f'])
# #%%
# # test.to_csv('E://source_data/after_delete_empty_nulls/insured_utf-8_delete_empty_nulls_na-gt-limit_drop_duplicates.csv',header=True,index=False)
# #%%
# drivers = pd.concat([drivers, pd.DataFrame(columns=list('f'))],sort=True)
#
# #%%
# def filter_registno_in_registnos(line):
#     print(line)
#     if line['registno'] in registnos:
#         line['f'] = 1
#     else:
#         line['f'] = 0
#     print(line)
#     return line


#%%
test = drivers[drivers.registno.isin(registnos)]
#%%
test.to_csv('E://source_data/after_delete_empty_nulls/drivers_utf-8_delete_empty_nulls_na-gt-limit_drop_duplicates_filter.csv',header=True,index=False)
