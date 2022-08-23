import re
import sys
import os 
import pandas as pd 

filepath = sys.argv[1]

#cfile = open(filename,'r')


def test_if_footnote(line):
    #onlychar = re.sub("[^a-zA-Z+/']+","",line)
    #if len(onlychar) < 40 or len(onlychar) > 90:
    # if "rgya gar skad du" in line:
    #     return 0
    if re.search("[»«《》]",line):# or abs(len(line)-len(onlychar)) > (len(line)/3):
        return 1    
    else:
        return 0
if os.path.getsize(filepath) < 1000:
    df = pd.read_csv(filepath,sep="\t",names=["orig"])

    footnote_count = 0 
    lines = df['orig'].tolist()

    for line in lines:
        footnote_count += test_if_footnote(line)

    if footnote_count / (len(lines)-2) > 0.4:    
        os.remove(filepath)
