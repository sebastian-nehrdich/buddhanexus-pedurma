import re
import sys
import pandas as pd

filename = sys.argv[1]
#filename = "../extracted/T06D4064_pedurma.txt"

def test_if_junk(line):
    onlychar = re.sub("[^a-zA-Z+/']+","",line)
    #if len(onlychar) < 40 or len(onlychar) > 90:
    if "rgya gar skad du" in line:
        return 0
    elif re.search("[»«《》]",line) or abs(len(line)-len(onlychar)) > (len(line)/1.5):
        return 1    
    else:
        return 0

def clean_beg(lines):
    rgya_flag = False
    for line in lines[:20]:
        if "rgya gar" in line:
            rgya_flag = True
    if rgya_flag == 1:
        result_lines = []
        start_flag = False
        for line in lines[:20]:
            if  "rgya gar" in line:
                start_flag = 1
                line = "rgya gar" + line.split("rgya gar")[1]
            if start_flag:
                result_lines.append(line)
        lines = result_lines + lines[20:]
    start_flag = 0
    result_lines = []
    for line in lines[:20]:
        if not test_if_junk(line):
            start_flag = 1
        if start_flag:
            result_lines.append(line)
    lines = result_lines + lines[20:]
    return lines


def clean_end(lines):
    footnote_flag = False
    for line in lines[-100:]:
         if re.search("[»«《》]",line):
             footnote_flag = True
    if footnote_flag:
        result_lines = []
        remove_flag = False
        for line in lines[-100:]:
            if re.search("[»«《》]",line):
                remove_flag = True
                line = re.sub("[»«《》].*","",line)
                result_lines.append(line)                
                break
            result_lines.append(line)
            lines = lines[:-100] + result_lines
    return lines

def clean_pagenumbers(lines):
    result_lines = []
    for line in lines:
            line =  re.sub("(^\[[0-9][^a-zA-Z]*\]).*",r"\1",line)
            line = line.replace("\\u0f72_","")
            line = line.replace("\\u0f72","")
            line = line.replace("\\u0f38","")
            line = line.replace("_"," ")
            line = re.sub("\\\\u0f[7-8][a-z]","",line)
            line = re.sub(" +"," ",line)
            line = line.replace("/ /","//")
            result_lines.append(line)
    return result_lines


def clean_file(path):
    global lines
    cfile = open(path,"r")
    lines = cfile.readlines()
    lines = clean_beg(lines)
    lines = clean_end(lines)
    lines = clean_pagenumbers(lines)
    result_string = "".join(lines)
    with open(path + "_cleaned","w") as outfile:
        outfile.write(result_string)
    
    
    
clean_file(filename)
