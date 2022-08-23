import xml.etree.cElementTree as et
import re
import os
from fuzzysearch import levenshtein_ngram
from tqdm import tqdm
import multiprocessing



outline_path = "../outline/bdrc-pedurma.xml"
filepath = "../wylie/merged.txt"
filepath = "../wylie/I1PD95846.txt.wylie"

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
    no_toh = 5000
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
                toh = ""
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
                        if description.attrib['type'] == "toh":
                            if description.text:
                                toh = description.text.strip()
                                toh = toh.replace("/","_")
                                toh = toh.replace(" ","")
                if toh == "":
                    toh = str(no_toh)
                    no_toh += 1 
                                        
                location = entry.findall('{http://www.tbrc.org/models/outline#}location')[0]
                vol = ""
                if "vol" in location.attrib:
                    vol = int(location.attrib['vol'])

                result.append({
                    "title": wtitle.strip(),
                    "colophon": colophon,
                    "first_page": first_page,
                    "last_page": last_page,
                    "vol": vol,
                    "toh": toh
                    })
    return result


outline_entries = read_outline_entries(outline_path)

def clean_col(col):
    col = re.sub("[/]"," ",col)
    col = re.sub("_","",col)
    col = re.sub("[0-9\[\]]","",col)
    col = re.sub(" +"," ",col)
    return col

def load_lines(path):
    cfile = open(path)
    lines = cfile.readlines()
    lines = [line.rstrip() for line in lines]
    return lines

def load_tengyur():
    tengyur_path = "tengyur_vols.txt"
    tengyur_file = open(tengyur_path,'r')
    tengyur_dic = {}
    for line in tengyur_file:
        vol, path = line.strip().split('\t')
        path = "../wylie/" + path
        tengyur_dic[vol] = load_lines(path)

    return tengyur_dic

tengyur = load_tengyur() 





def extract_text(lines,first_page,last_page,current_vol):
    text_flag = False
    found_text = False
    return_lines = []
    for line in lines:
        if re.search("^\[[^a-zA-Z]+\]$",line):
            if re.search("^\[[0-9]",line):
                page_line = re.sub("[^0-9]","",line)
                page_without_vol = re.sub("^" + str(current_vol),"",page_line)
                if re.search("[0-9]",page_without_vol):
                    page = int(page_without_vol)
                    if page == first_page:
                        text_flag = True
                    if text_flag == True and (page == last_page+1 or page == last_page+2):
                        text_flag = False
                        found_text = True
        if text_flag:
            return_lines.append(line)

    if found_text:
        return return_lines
    else:
        return []

def process_page_numbers(outline_entries,tengyur):
    last_vol = ""
    lines = []
    for entry in outline_entries:
        if len(entry['first_page']) > 0 and len(entry['last_page']) > 0:
            current_vol = entry['vol']        
            first_page = int(re.sub("[^0-9]","",entry['first_page']))
            last_page = int(re.sub("[^0-9]","",entry['last_page']))

            if current_vol != last_vol:
                print("VOL",current_vol)
                if str(current_vol) in tengyur:
                    lines = tengyur[str(current_vol)]
                last_vol = current_vol
            return_lines = extract_text(lines,first_page,last_page,current_vol)
            if return_lines == []:
                return_lines = extract_text(lines,first_page-1,last_page,current_vol)
            result = "\n".join(return_lines)
            with open("../output/" + entry['toh'] + ".txt",'w') as outfile:
                outfile.write(result)
            



            
process_page_numbers(outline_entries, tengyur)
    








