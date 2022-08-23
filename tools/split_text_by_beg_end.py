import pandas as pd
import re
from tqdm import tqdm
from pathlib import Path
import multiprocessing


volume_path = "/home/basti/data/buddhanexus-pedurma/wylie/merged.tsv"

volume_df = pd.read_csv(volume_path,sep="\t",names=["segmentnr","orig","stemmed"],dtype="str")

def read_file(path):
    cfile = open(path,'r')
    global qlines
    qlines = cfile.readlines()
    if len(qlines) <= 2:
        return [0,0,path]
    beg_result = volume_df[volume_df['orig'].str.match(re.escape(qlines[0].strip()), na=False)]
    found_flag = False
    beg = 0
    end = 0 
    for cindex in beg_result.index:
        if cindex+2 < len(volume_df):
            if qlines[1].strip() in volume_df.iloc[cindex+1]['orig']:
                beg = cindex
                found_flag = True
                break
    if not found_flag:
        print("DIDN't find BEG for: ",path)
        print("QLINE",qlines[0])
    found_flag = False
    end_result = volume_df[volume_df['orig'].str.match(re.escape(qlines[-2].strip()), na=False)]
    for cindex in end_result.index:
        if cindex+1 < len(volume_df):
            if qlines[-1].strip() in volume_df.iloc[cindex+1]['orig']:
                end = cindex
                found_flag = True
                break
    if not found_flag:
        print("DIDN't find END for: ",path)
        print("QLINE",qlines[-2])
    return [beg,end, path] 



volume_df['flag'] = ""
volume_df['filename'] = ""
#volume_df = read_file("/home/basti/data/buddhanexus-pedurma/extracted/K01D0001-2_H0001-2_pedurma.txt", volume_df)
query_path = "/home/basti/data/buddhanexus-pedurma/extracted/"


filelist = []
for file in tqdm(os.listdir(query_path)):
    filename = os.fsdecode(file)
    if ".txt" in filename:
        short_path = Path(filename).stem
        filelist.append(query_path + filename)

pool = multiprocessing.Pool(processes=12)
results = pool.map(read_file,filelist)
pool.close()


for result in results:
    beg,end,fn = result
    short_filename = Path(fn).stem
    if not volume_df.iloc[beg]['flag'] == "END":
        volume_df.iloc[beg]['flag'] = "BEG"
    volume_df.iloc[beg]['filename'] = short_filename
    volume_df.iloc[end]['flag'] = "END"
    volume_df.iloc[end]['filename'] = short_filename

volume_df.to_csv("../wylie/merged_marked.csv",sep="\t")

def write_df_split(df):
    outpath = "../extracted2/"
    c = 0
    c_last = 0
    cunknown = 0 
    for index, row in tqdm(df.iterrows()):
        if row['flag'] == "BEG":
            print("GOT HERE")
            current_df = df[c_last:c]
            current_df.to_csv(outpath + str(cunknown) + ".txt", sep ='\t', index=False)
            cunknown += 1
            c_last = c+1
        if row['flag'] == "END":
            fn = row['filename'].replace("_pedurma.txt","")
            current_df = df[c_last:]
            current_df.to_csv(outpath + fn + ".txt", sep ='\t',index=False)
            c_last = c+1
            
        c += 1


write_df_split(volume_df)
