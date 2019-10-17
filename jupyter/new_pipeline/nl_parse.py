''' This script 
usage:

'''

import re

import markdown2
from bs4 import BeautifulSoup
from nltk import word_tokenize

def is_code_tag_in_nl(nl):
    html = markdown2.markdown(nl)
    try:
        bs = BeautifulSoup(html, "html.parser")
    except:
        return False
    if bs.find_all('code'):
        return True
    return False

def normalize_nl_leave_code_tokenize(nl):
    html = markdown2.markdown(nl)
    bs = BeautifulSoup(html, "html.parser")

    # print(bs)

    # we change the code tag since word tokenize will separate the carrots
    bs_string = str(bs)
    # print('tokenized code', bs_string)
    bs_string = bs_string.replace('<code>', ' CTAG ')
    bs_string = bs_string.replace('</code>', ' CTAG ')

    # replace any residual html tags
    p = re.compile(r'<.*?>')
    bs_string = p.sub('', bs_string)
    # print(bs_string)
    return word_tokenize(bs_string)


def unwrap_nested(tag):
    ''' if we just take the last top level tag, it won't include important
    nested code tags within it, such as human_files in the example below.

    <p>In the code cell below, we import a dataset of human images, where the file paths are stored in the numpy array <code>human_files</code>.</p>
    :param tag:
    :return:
    '''
    for nested in tag.find_all():
        nested.unwrap()

