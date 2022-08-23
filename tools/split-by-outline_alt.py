import xml.etree.cElementTree as et
import re
import os
from fuzzysearch import levenshtein_ngram
from tqdm import tqdm
import multiprocessing



outline_path = "../outline/bdrc-pedurma.xml"
filepath = "../wylie/merged.txt"
#filepath = "../wylie/I1PD95846.txt.wylie"

def test_matches(large_string, query_string, threshold):
    words = large_string.split()
    for word in words:
        s = difflib.SequenceMatcher(None, word, query_string)
        match = ''.join(word[i:i+n] for i, j, n in s.get_matching_blocks() if n)
        if len(match) / float(len(query_string)) >= threshold:
            yield match

def read_outline_entries(path):
    tree = et.parse(path)
    root = tree.getroot()
    result = []
    for entry in root.iter('{http://www.tbrc.org/models/outline#}node'):
        if entry.attrib["type"] == "text":
            wtitle = ""
            titles = entry.findall('{http://www.tbrc.org/models/outline#}title')
            for title in titles:
                if 'encoding' in title.attrib:
                    if title.attrib['encoding'] == "extendedWylie":
                        wtitle = title.text.strip()
            if wtitle != "":
                colophon = ""
                location = ""
                first_page = ""
                last_page = ""
                descriptions = entry.findall('{http://www.tbrc.org/models/outline#}description')
                for description in descriptions:
                    if 'type' in description.attrib:
                        if description.attrib['type'] == "colophon":
                            if description.text:
                                colophon = description.text.strip()
                                colophon = colophon.replace("\n"," ")
                                colophon = re.sub(" +"," ",colophon)
                        if description.attrib['type'] == "location":
                            if description.text:
                                text = description.text.strip().replace("pp. ","")
                                if len(text) < 20:
                                    if "-" in text:
                                        text = re.sub("--+","-",text)
                                        first_page,last_page = text.split("-")
                                    else:
                                        first_page = re.sub("[^0-9]+","",first_page)
                                        last_page = first_page
                location = entry.findall('{http://www.tbrc.org/models/outline#}location')[0]
                vol = ""
                if "vol" in location.attrib:
                    vol = location.attrib['vol']
                else:
                    print("NO VOL")
                result.append({
                    "title": wtitle.strip(),
                    "colophon": colophon,
                    "first_page": first_page,
                    "last_page": last_page,
                    "vol": vol
                    })
    return result


outline_entries = read_outline_entries(outline_path)

def clean_col(col):
    col = re.sub("[/]"," ",col)
    col = re.sub("_","",col)
    col = re.sub("[0-9\[\]]","",col)
    col = re.sub(" +"," ",col)
    return col

cfile = open(filepath)
lines = cfile.readlines()
lines = [line.rstrip() for line in lines]



def mark_beginning(lines):
    beginning_string = "rgya gar skad du"
    end_string = "bod skad du"
    for c in tqdm(range(len(lines)-10)):
        current_window = ' '.join(lines[c:c+10])
        if beginning_string in current_window and end_string in current_window and not "TEXT_BEG" in current_window:
            current_window = clean_col(current_window)
            if not re.search("[»«《》]",current_window):
                found_beg = False 
                for d in range(0,10):
                    if "rgya gar" in lines[c+d] and not "TEXT_BEG" in lines[c+d]:
                        lines[c+d] = lines[c+d].replace("rgya gar","TEXT_BEG rgya gar")
                        found_beg = True
                        break
                if not found_beg:
                    for d in range(0,10):
                        if re.search("rgya$",lines[c+d]) and not "TEXT_BEG" in lines[c+d]:
                            lines[c+d] = lines[c+d][:-4] + "/TEXT_BEG rgya"
                            found_beg = True
                            break
                    if not found_beg:
                        print("DIDN'T find BEG",c)
                        print("CURRENT_WINDOW",current_window)
                        print("LINES",lines[c:c+10])
                else:
                    print("FOUND BEG")

    return lines
        
                
            
#lines = mark_beginning(lines)        

def get_longest(col):
    tokens = col.split()
    cleaned_tokens = []
    for token in tokens:
        if token != "rdzogs" and token != "mdzad":
            cleaned_tokens.append(token)
    if len(tokens) > 0:
        return max(cleaned_tokens, key=len)
    return ""

        
def mark_ending(outline_entry,file_lines):
    title = outline_entry['title']
    colophon = clean_col(outline_entry['colophon'])
    longest_token = get_longest(colophon)
    found_match = False
    failed = ""
    if len(colophon) > 5:
        allowed_distance = int(len(colophon) / 10)
        for c in range(len(lines)-10):
            current_window = " ".join(lines[c:c+10])
            if ("mdzad" in current_window or "rdzogs" in current_window) and longest_token in current_window and not "TEXT" in current_window:                
                current_window = clean_col(current_window)
                match = list(levenshtein_ngram.find_near_matches_levenshtein_ngrams(colophon,current_window, max_l_dist=allowed_distance))
                if match:
                    rough_window = "".join(lines[c:c+10])
                    match2 = list(levenshtein_ngram.find_near_matches_levenshtein_ngrams(colophon,rough_window, max_l_dist=allowed_distance * 3))
                    if match2:
                        print("MATCH2",match2[0])
                        end_pos = match2[0].end
                        true_end_pos = 0 
                        len_acc = 0
                        for d in range(0,5):
                            clen = len(file_lines[c+d])
                            if len_acc  + clen > end_pos:
                                true_end_pos = end_pos - len_acc
                                file_lines[c+d] = file_lines[c+d][:true_end_pos] + " TEXT_END # " + title + " # " +  file_lines[c+d][true_end_pos:]
                                print("FILE LINES WITH END",file_lines[c+d])
                                break
                            else:
                                len_acc += clen
                    
                    else:
                        print("FOUND COLOPHON, DIDN'T FIND ENDING")
                        failed = title 

                                               
                    print("COLOPHON",colophon)
                    print(match[0])
                    found_match = True
                    print("CWINDOW",current_window)
                    print("LINES",lines[c:c+5])
                    print("C",c)
                    break
    if not found_match:
        print("DIDN'T find match for",outline_entry)
        failed = title
    return [file_lines,failed]  

def mark_endings(data):
    outline_entries,lines = data
    failed_result = ""
    
    for entry in outline_entries[:10]:
        lines,failed = mark_ending(entry,lines)
        failed_result += failed + "\n"
    return [lines,failed_result]


# chunksize = int(len(outline_entries)/12)
# chunks = []
# for x in range(0,len(lines),chunksize):
#     chunks.append([outline_entries[x:x+chunksize], lines])
# pool = multiprocessing.Pool(processes=12)
# results = pool.map(mark_endings, chunks)
# pool.close()

# failed_string = ""
# merged_string = ""
# merged_lines = []
# for result in results:
#     lines,failed = result
#     failed_string += failed
#     merged_lines.append(lines)


# for c in range(len(lines)):
#     found = False
#     for d in range(12):
#         if "TEXT" in merged_lines[d][c]:
#             merged_string += merged_lines[d][c]
#             found = True
#             break
#     if found == False:
#         merged_string += merged_lines[0][c]

col_only = ""

for entry in outline_entries:
    col_only += entry['colophon'].replace("\n"," ") + "\n"

with open("col_only.txt",'w') as outfile:
    outfile.write(col_only)

# with open("merged_lines_processed.txt",'w') as outfile:
#     outfile.write(merged_string)
    

# for entry in outline_entries[3:10]:
#     mark_ending(entry,lines)
            
# for entry in outline_entries:
#     print(entry['colophon'][-10:])

# for line in lines:
#     if "BEG" in line:
#         print(line)
