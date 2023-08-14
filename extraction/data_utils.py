import fitz
import sys 
import os
os.chdir(sys.path[0])

def pdf2text(pdf_file, ):
    doc = fitz.open(pdf_file)

    text = ""
    for page in doc:
        text += page.get_text()

    text_lst = list(filter(lambda x: x!='', map(lambda x:x.strip(), text.split('\n'))))
    for t in text_lst:
        if '行业评级' in t:
            return t[-2:]
    print()

pdf2text('../data/pdf/H3_AP202307191592422843_1.pdf')