from gensim.models import KeyedVectors
import pandas as pd
import faiss

vector_model = KeyedVectors.load_word2vec_format("/home/basti/code/dvarapandita/code/ref/tib.vec", binary=False)    

file1 = sys.argv[1]
file2 = sys.argv[2]

file1_df = pd.read_csv(file1)
file2_df = pd.read_csv(file2)

def get_vector(stem,vector_model):
    if stem in vector_model.vocab:
        return vector_model.get_vector(stem)
    else:
        # when OOV, return vector of zeros
        return np.zeros(vector_model.vector_size)

def vectorize_file(df):
    vectors = []
    sentences = df['stemmed'].tolist()
    for sentence in sentences:
        for stem in sentence.split():
            stem = vector.split("_")[0]
            vectors.append(get_vector(stem))
    
