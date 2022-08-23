import plotly.graph_objects as go
from sklearn.metrics.pairwise import cosine_similarity
from gensim.models import KeyedVectors
import numpy as np
#import faiss
import sys
import os
import multiprocessing
import pandas as pd 
import re
import natsort
from sklearn.manifold import TSNE
from sklearn import cluster
from sklearn import metrics 
from itertools import groupby
import random 
import matplotlib.pyplot as plt
from tqdm import tqdm
import networkx as nx


threshold = 0.3 #0.73 war ein guter erster versuch
#threshold = 0.8
vector_path = "pedurma.vec"

SEG_MIN_LENGTH = 1 # min length of a segment to be extracted

path = sys.argv[1] #"../wylie/I1PD95922.txt.w.tsv"
#path = "../wylie/I1PD96842.txt.w.tsv"

vectors = KeyedVectors.load_word2vec_format(vector_path, binary=False)

stopwords = [] # do we really need stopwords for this task? let's see...
    
def dedup(k):
    k = sorted(k)
    return list(k for k, _ in groupby(k))


def get_sumvector(stemmed):
    cvectors = []
    for stem in stemmed.split():
        if stem in vectors.vocab and not stem in stopwords:
            cvector = vectors.get_vector(stem)                
            cvectors.append(cvector)
    if len(cvectors) > 0:
        return  np.average(cvectors,axis=0)
    else:
        return np.zeros(100)

def doc2vec(path):
    doc_vectors = {}
    print("NOW PROCESSING",path)
    cfile = pd.read_csv(path,sep='\t',names=["segmentnr","orig","stemmed"])
    #cfile['sumvectors'] = cfile['orig'].apply(get_sumvector)
    return cfile 


def test_sentences(sent_a, sent_b):
    if re.search("[»«《》]",str(sent_a)) and re.search("[»«《》]", str(sent_b)):
        return True
    if not re.search("[»«《》]",str(sent_a)) and not re.search("[»«《》]", str(sent_b)):
        return True 
        


def create_graph(sent_data):
    global vectors, distances,g,cliques
    #vectors = np.asarray(sent_data['sumvectors'].tolist())
    orig_sentences = sent_data['orig'].tolist()
    #similarities = cosine_similarity(vectors,vectors)
    graph_data = []
    g = nx.MultiGraph()
    for c1 in range(len(orig_sentences)):
        sentence_a = orig_sentences[c1]
        for c2 in range(len(orig_sentences)):
            sentence_b = orig_sentences[c2]
            if c1 != c2 and test_sentences(sentence_a, sentence_b):
                #if similarities[c1][c2] > threshold:
                g.add_node(c1)
                g.add_node(c2)
                g.add_edge(c1,c2)
    return g                

def merge_cliques(cliques):
    old_cliques = []
    merged_cliques = []
    while old_cliques != cliques:
        old_cliques = cliques.copy()
        merged_cliques = []
        for c in range(len(cliques)-1):
            current_segment = cliques[c]
            next_segment = cliques[c+1]
            if len(current_segment) > 0 and len(next_segment) > 0:
                if not set(current_segment).isdisjoint(next_segment):
                    merged_cliques.append(dedup(current_segment + next_segment))            
                    cliques[c+1] = []
                # elif current_segment[-1] + 1 == next_segment[0]:
                #     merged_cliques.append(current_segment + next_segment)
                #     cliques[c+1] = []
                else:
                    merged_cliques.append(current_segment)
        merged_cliques.append(cliques[-1])
        merged_cliques = [i for i in merged_cliques if len(i) > 0]
        cliques = dedup(merged_cliques)
    return cliques


def fill_segment_gaps(segments,total_length):
    start = 0
    result = []
    for c2 in range(0,segments[0][0]):
        result.append([c2])
    for c in range(len(segments)-1):
        current_segment = segments[c]
        next_segment = segments[c+1]
        # enter here if we have a segment that only consist of one line as next:
        result.append(current_segment)            
        if abs(current_segment[-1]-next_segment[-1]) > 1:
            new_segment = []
            for c2 in range(0,abs(current_segment[-1]-next_segment[0]-1)):
                new_segment.append([current_segment[-1]+1+c2])
            result.extend(new_segment)
    result.append(segments[-1])
    for c in range(segments[-1][-1],total_length):
        result.append([c])
    return result

def cliques_to_dict(cliques):
    result = {}
    for clique in cliques:
        for index in clique:
            if index not in result:
                result[index] = [clique]
            else:
                result[index].append(clique)
    return result

def merge_by_common_clique(segments,cliques_dict):
    old_segments = []
    global merged_segments
    while old_segments != segments:
        old_segments = segments.copy()
        merged_segments = []
        for c in range(len(segments)-1):
            current_segment = segments[c]
            next_segment = segments[c+1]
            found_flag = False
            global current_cliques
            current_cliques = []
            for index in current_segment:
                if index in cliques_dict:
                    current_cliques += cliques_dict[index]
            for clique in current_cliques:                
                if not set(current_segment).isdisjoint(clique):
                    if not set(next_segment).isdisjoint(clique):
                        merged_segments.append(current_segment+next_segment)
                        segments[c+1] = []
                        found_flag = True 
                        break
            if found_flag == False:
                merged_segments.append(current_segment)
        merged_segments.append(segments[-1])
        merged_segments = [i for i in merged_segments if len(i) > 0]
        segments = dedup(merged_segments)
    return segments

def get_continuous_cliques(max_cliques):
    continuous_cliques = []
    cliques = []
    for clique in max_cliques:
        cliques.append(clique)
        if len(clique) > 1:
            clique.sort()
            gb = groupby(enumerate(clique), key=lambda x: x[0] - x[1])
            all_groups = ([i[1] for i in g] for _, g in gb)
            continuous_cliques.extend(list(filter(lambda x: len(x) > 1, all_groups)))
    return continuous_cliques, cliques

def remove_short_segments(segments):
    
    for c in range(len(segments)):
        new_segments = []
        for segment in segments:
            if len(segment) > SEG_MIN_LENGTH:
                new_segments.append(segment)
        last_segment = [0]
        new_segments2 = []
        for c in range(len(new_segments)-1):
            current_segment = new_segments[c]
            next_segment = new_segments[c+1]
            if abs(current_segment[-1]-next_segment[0]) > 1:
                new_segments2.append(current_segment+next_segment)
                c += 1
            else:
                new_segments2.append(current_segment)
        return new_segments2
                        


def get_segments_from_graph(G,total_length):
    # get sorted subgraphs
    global continuous_cliques, cliques, merged_cliques 
    max_cliques = nx.find_cliques(G)    
    # 1. get continuous sentences within each clique
    continuous_cliques, cliques = get_continuous_cliques(max_cliques)
    cliques_dict = cliques_to_dict(cliques)
    continuous_cliques =  dedup(continuous_cliques)
    merged_cliques = merge_cliques(continuous_cliques)
    if len(merged_cliques) > 0:
        merged_cliques = fill_segment_gaps(merged_cliques,total_length)
        merged_cliques = merge_by_common_clique(merged_cliques,cliques_dict)
    merged_cliques = remove_short_segments(merged_cliques)
    return merged_cliques
    #return continuous_cliques

            
        
    
def print_segments(sent_data, segments, path):    
    beg = 0
    count = 0 
    for segment in segments:
        if len(segment) > SEG_MIN_LENGTH:
            end = segment[-1]
            segment_data = sent_data['orig'].loc[beg:end+2]
            beg = end 
            result_string = "\n".join(segment_data)
            if "018_5 @#/_" in result_string:
                print(result_string)
            with open(path.replace(".tsv", "_split_") + str(count) + ".tsv", "w") as outfile:
                outfile.write(result_string)
        count += 1


def process_file(path):
    segments = []
    global sent_data
    sent_data = doc2vec(path)
    sent_data['orig'] = sent_data['orig'].astype(str)
    for c in tqdm(range(0,len(sent_data),100)):
    #for c in tqdm(range(0,1000,100)):
        global g
        if len(sent_data[c:c+100]) > 0:
            g = create_graph(sent_data[c:c+100])
            current_segments = get_segments_from_graph(g,len(sent_data[c:c+100]))
            # merge segments that are overlapping
            if len(segments) > 0 and len(current_segments) > 0:
                # concat two segments from two different batches if they are longer than 3 
                if len(segments[-1]) > 3 and len(current_segments[0]) > 3:
                    if test_sentences(sent_data['orig'].loc[segments[-1][-2]], sent_data['orig'].loc[c+current_segments[0][1]]):
                        segments[-1] = segments[-1] + [x+c for x in current_segments[0]] 
                    else:
                        segments.append([x+c for x in current_segments[0]])
                else:
                    segments.append([x+c for x in current_segments[0]])

            else:
                if len(current_segments) > 0:
                    segments.append([x+c for x in current_segments[0]])
            for segment in current_segments[1:]:
                new_segment = []
                for index in segment:
                    new_segment.append(index+c)
                segments.append(new_segment)        
    print_segments(sent_data, segments, path)


process_file(path)
