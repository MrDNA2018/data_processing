import pandas as pd
chunk_list = []  # append each chunk df here
i = 1
reader = pd.read_csv( 'E://source_data/insured.csv', iterator=True,encoding='utf-8')
#%%
loop = True
chunkSize = 100000
chunks = []
while loop:
    try:
        chunk = reader.get_chunk(chunkSize)
        chunks.append(chunk)
        print("当前处理数量：{}万".format(i*10))
        if i % 10 == 0:
            pd.concat(chunks, ignore_index=True).to_csv('E://source_data/insured'+ str(i) + '.csv', header=True, index=False)
            print("saved：{}万".format(i*10))
            chunks = []

        i +=1
    except StopIteration:
        loop = False
        print("Iteration is stopped.")
