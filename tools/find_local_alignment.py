import re
import sys
import pandas as pd 
import numpy as np 
import faiss
import os
import difflib
from pathlib import Path
from tqdm import tqdm as tqdm
from gensim.models import KeyedVectors
from collections import OrderedDict

from Bio import pairwise2

#volume_path = sys.argv[1]
#volume_path = "/home/basti/data/buddhanexus-pedurma/wylie/I1PD96860.txt.w.tsv"
volume_path = "../pedurma-volumes/"
query_path = "../acip/"

vectorlength = 200
gapsize = 200 


def read_file(path):
    df = pd.read_csv(path,sep="\t",names=["segmentnr","orig","stemmed"],dtype="str", on_bad_lines="skip")
    df['stemmed'] = df['stemmed'].apply(lambda line: line.split(" "))
    df = df.explode('stemmed')
    df['stemmed'] = df['stemmed'].apply(lambda stem: stem.split("_")[0])
    stems = df['stemmed'].tolist()
    return df,stems

def read_files(path):
    all_volumes_df = []
    all_stems = []
    for file in os.listdir(path):
        filename = os.fsdecode(file)
        print("NOW LOADING", filename)
        current_volume_df, current_stems = read_file(path+filename)
        all_volumes_df.append(current_volume_df)
        all_stems.extend(current_stems)
    return pd.concat(all_volumes_df), all_stems



volume_file_df, volume_stems = read_files(volume_path)
vector_model = KeyedVectors.load_word2vec_format("tib.vec", binary=False)    

def get_alignment(query_stems, volume_stems):
     alignments =  pairwise2.align.localms(query_stems,volume_stems,5,-4,-5,-5, gap_char=["-"],one_alignment_only=1)
     if alignments:
         return alignments[0].start, alignments[0].end
     else:
         return 0,0
     

def get_vector(stem,vector_model):
    if stem in vector_model.vocab:
        return vector_model.get_vector(stem)
    else:
        # when OOV, return vector of zeros
        return np.zeros(vector_model.vector_size)

def get_sumvectors(vector_list, windowsize,skip_gap):
    sumvectors = []    
    for i in tqdm(range(0,len(vector_list),skip_gap)):
        k = i + windowsize
        sumvectors.append(np.mean(vector_list[i:k], axis=0))
    return sumvectors

def create_index(stems):
    vectors = []
    
    for stem in stems:
        vectors.append(get_vector(stem,vector_model))
    sumvectors = np.asarray(get_sumvectors(vectors,vectorlength,gapsize)).astype("float32")
    index = faiss.IndexHNSWFlat(100, 32)
    index.verbose=True
    faiss.normalize_L2(sumvectors)
    index.add(sumvectors)
    return index



def get_beg_end(query_file_df,query_stems, volume_stems):
    beg_vectors = []
    for stem in query_stems[:vectorlength]:
        beg_vectors.append(get_vector(stem,vector_model))
    end_vectors = []
    for stem in query_stems[-vectorlength:]:
        end_vectors.append(get_vector(stem,vector_model))
    beg_vector = np.mean(beg_vectors,axis=0)
    end_vector = np.mean(end_vectors,axis=0)
    global search_result
    search_result = index.search(np.array([beg_vector, end_vector]).astype("float32"), 1)
    result_position_beg = search_result[1][0][0] * gapsize
    result_position_end = search_result[1][1][0] * gapsize
    global volume_tokens_end
    volume_tokens_beg = volume_stems[result_position_beg-vectorlength:result_position_beg+vectorlength*2]
    volume_tokens_end = volume_stems[result_position_end-vectorlength:result_position_end+vectorlength*2]
    alignment_beg, alignment_end = get_alignment(query_stems[:vectorlength],volume_tokens_beg)
    beg = result_position_beg-vectorlength+alignment_beg

    alignment_beg, alignment_end = get_alignment(query_stems[-100:],volume_tokens_end)
    end = result_position_end-vectorlength+alignment_end
    print("BEG",volume_file_df[beg:beg+1])
    print("END",volume_file_df[end:end+1])
    return beg,end
    
def write_pedurma_file(beg, end, volume_file_df,query_path):
    global sentences
    cslice = volume_file_df[beg-10:end+100]
    global sentences_volume
    sentences_volume = list(OrderedDict.fromkeys((cslice['orig'].tolist())))
    c = 0 
    for sentence in sentences_volume:
        sentences_volume[c] = str(sentence)
        c += 1
        
    volume_string = "\n".join(sentences_volume)
    short_path = Path(query_path).stem
    with open("../extracted/" + short_path + "_pedurma.txt", "w") as outfile:
        outfile.write(volume_string) 
    
def process_query_file(query_path):
    global query_stems
    query_file_df, query_stems = read_file(query_path)
    beg, end = get_beg_end(query_file_df,query_stems, volume_stems)
    qlen = len(query_stems)
    rlen = end-beg    
    len_deviation = abs(qlen-rlen) / qlen
    if len_deviation < 0.2:
        result_stems = volume_stems[beg:end]
        sm = difflib.SequenceMatcher(None,query_stems[:1000],result_stems[:1000])
        sm_similarity = sm.ratio()
        if sm_similarity > 0.3:
            print("SEQUENCE DEVIATION", sm_similarity)
            write_pedurma_file(beg, end, volume_file_df,query_path)



index = create_index(volume_stems)

for file in tqdm(os.listdir(query_path)):
    filename = os.fsdecode(file)
    short_path = Path(filename).stem
    if ".tsv" in filename:
        if not os.path.isfile("../extracted/" + short_path + "_pedurma.txt"):
            process_query_file(query_path + filename)

