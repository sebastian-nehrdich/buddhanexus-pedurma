import re
import sys

right_file = "../classifier/training-data/right.txt"
wrong_file = "../classifier/training-data/wrong.txt"

rfile = open(right_file,'r')
wfile = open(wrong_file,'r')


lens = []

def test_if_junk(line):
    onlychar = re.sub("[^a-z]+","",line)
    #if len(onlychar) < 40 or len(onlychar) > 90:
    if re.search("[»«《》]",line):
        return True
    elif len(onlychar) < 40:
        return True

    # elif len(onlychar) > 80:
    #     return True

    elif abs(len(onlychar)-len(line)) > len(line)/2:
        return True
    
    else:
        return False

scores = []
for line in rfile:
    score = test_if_junk(line)
    scores.append(score)

    
print("FALSE POSITIVE ON RIGHT FILE:",sum(scores))

scores = []
for line in wfile:
    score = test_if_junk(line)
    scores.append(score)
    # if not score:
    #     print(line)


print("TRUE NEGATIVES ON WRONG FILE:",sum(scores))
print("FALSE POSITIVES ON WRONG FILE:",len(scores) - sum(scores))
    
