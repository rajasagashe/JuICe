

import json
from os import listdir
from os.path import isfile, join

from tqdm import tqdm
# from dotmap import DotMap
# from toolbox.util import jsoniter


def get_files_under_dir(path, extension=''):
    files_dirs = [join(path, f) for f in listdir(path)]
    only_files = [f for f in files_dirs if isfile(f)]
    if extension:
        only_files = [f for f in only_files if f.endswith(extension)]
    return only_files

def num_lines_in_file(filename):
    return sum(1 for line in open(filename))

def jdumpl(lst, filename):
    with open(filename, 'w') as f:
        for js in lst:
            assert not isinstance(js, str)
            f.write(json.dumps(js) + '\n')

def jsoniter(filename, lines=None):
    fobj = open(filename)
    for i, line in enumerate(fobj):
        if lines and i == lines:
            break
        yield json.loads(line)


def jloadl(filename, lines=-1, dotmap=False, progress=False):
    with open(filename) as fobj:
        lst = []
        for i, line in tqdm(enumerate(fobj)):
            if i == lines:
                break
            js = json.loads(line)
            lst.append(js)
        return lst



# todo-release remove this
# from toolbox.nlp_tools.featurizer import Bow
# from toolbox.nlp_tools.matrix import topk
# from toolbox.nlp_tools.retrieval import closest_index
# from toolbox.code_gen.bleu import get_bleu
