from gensim.models import KeyedVectors
import numpy as np
import faiss
import os
import multiprocessing

chn_vector_path = "/mnt/code/calculate-quotes/data/chinese.vec"

tib_vector_path = "pedurma.vec"
ifolder = "/home/basti/data/buddhanexus-pedurma/exp/"
qfolder = "/home/basti/data/tibetan/tsv/"
#ifolder = "/home/basti/data/buddhanexus-pedurma/extracted2/fragments/"

vectors = KeyedVectors.load_word2vec_format(tib_vector_path, binary=False)

def doc2vec(path):
    doc_vectors = []
    cfile = open(path,'r')
    for line in cfile:
        splits = line.split('\t')
        if len(splits) > 2:
            words = splits[1]
            for word in words.split():
                if word in vectors.vocab:
                    cvector = vectors.get_vector(word)
                    doc_vectors.append(cvector)
        elif len(splits) < 2:
            words = splits[0]
            for word in words.split():
                if word in vectors.vocab:
                    cvector = vectors.get_vector(word)
                    doc_vectors.append(cvector)
                    
    sum_vector = np.zeros(100)
    if len(doc_vectors) > 1:
        sum_vector = np.average(doc_vectors,axis=0)
    return [path,sum_vector]

def test_files(path1,path2):
    size1 = os.path.getsize(path1)
    size2 = os.path.getsize(path2)
    variation = 10
    if size1 > 0:
        variation = abs(size1-size2) / size1
    return variation


def get_vectors_from_path(path):
    filelist = []
    for filename in os.listdir(path):
        filename = os.fsdecode(filename)
        if not "N" in filename:
            filelist.append(path+filename)            
    pool = multiprocessing.Pool(processes=16)    
    results = pool.map(doc2vec, filelist)
    pool.close()
    return results

def process_folder(qpath,ipath):
    global idata
    qdata = get_vectors_from_path(qpath)
    idata = get_vectors_from_path(ipath)
    
    ifilenames = []
    ivectors = []
    for entry in idata:
        ifilenames.append(entry[0])
        ivectors.append(entry[1])    
    ivectors = np.asarray(ivectors).astype("float32")
    index = faiss.IndexFlat(100)
    #index.hnsw.efConstruction = 100 # 40 is default, higher = more accuracy 
    index.verbose = True
    faiss.normalize_L2(ivectors)
    index.add(ivectors)
    final_results = []
    
    for qfile in qdata:
        qfilename = qfile[0]
        print("NOW PROCESSING",qfilename)
        qvector = qfile[1]
        query_result = index.search(np.asarray([qvector]).astype("float32"), 3)
        for score_list,result_list in zip(query_result[0],query_result[1]):
            for score,result in zip(score_list,result_list):
                if score < 0.2:
                    result_filename = ifilenames[result]
                    variation = test_files(qfilename,result_filename)
                    if variation < 1:
                        final_results.append([os.path.getsize(qfilename), qfilename, result_filename, str(variation), str(score)])
                        break
    final_results.sort()
    result_string = ""
    for result in final_results:
        result_string += str(result[0]) + "\t" + '\t'.join(result[1:]) + "\n"        
    with open("similarity_results.txt",'w') as outfile:
        outfile.write(result_string)


process_folder(qfolder,ifolder)


