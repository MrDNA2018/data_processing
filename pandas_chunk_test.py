import pandas as pd
df_chunk = pd.read_csv('F://total2.csv', iterator=True,encoding='utf-8')
df=df_chunk.get_chunk(10)
#%%
df_chunk = pd.read_csv('F://total2.csv', chunksize=500000,encoding='utf-8')
chunk_list = []  # append each chunk df here
i =1
#%%
# Each chunk is in df format
for chunk in df_chunk:
    # perform data filtering
    # chunk_filter = chunk_preprocessing(chunk)

    # Once the data filtering is done, append the chunk to list
    # chunk_list.append(chunk_filter)
    chunk_list.append(chunk)
    print("当前chunnk:{}".format(i))
    i += 1

# concat the list into dataframe
df_concat = pd.concat(chunk_list)