''' This script 
usage:
'''

import json
import logging
import random
import shutil
from pathlib import Path

import dask.bag as db
from dask.diagnostics import ProgressBar
from polyglot.detect import Detector

from jupyter.jupyter_utils import is_markdown
from jupyter.new_pipeline.nl_parse import is_code_tag_in_nl

random.seed(1123)

pbar = ProgressBar()
pbar.register()


def get_markdown_language(nb):
    def get_language(string):
        logging.getLogger("polyglot").setLevel(logging.CRITICAL)
        try:
            d = Detector(string, quiet=True)
            if d.reliable:
                return d.language.name
            else:
                return d.languages[0].name
        except:
            # detector breaks on weird ascii chars, seems like
            # they come from english
            return 'failed'

    nl = ""
    for cell in nb['cells']:
        if is_markdown(cell) and 'source' in cell and cell['source']:
            nl += cell['source'] + ' '
    lang = get_language(nl)
    # if lang == 'Italian':
    #     print('==========\n', nl)
    #     pass
    return lang

def filter_for_english(nbs_indir, nbs_outdir):
    # assert '/scratch/nbgrader-pipeline/dask-gen' in nbs_outdir
    assert '/scratch/jupyter-pipeline' in nbs_outdir
    shutil.rmtree(nbs_outdir, ignore_errors=True)
    Path(nbs_outdir).mkdir(exist_ok=True)

    # bag=db.read_text(nbs_indir +'/*.jsonl').map(json.loads)
    # kernel_type = \
    #     bag.map(lambda nb: get_markdown_language(nb)) \
    #         .frequencies() \
    #         .topk(k=10, key=lambda tup: tup[1])
    # print('Counts of languages used for nl===========')
    # pprint(kernel_type.compute())

    # todo deal with un! aka get the comments
    (db.read_text(nbs_indir +'/*.jsonl').map(json.loads)
        # un language means no nl, for these likely its all in the comments
        .filter(lambda nb: get_markdown_language(nb) in ['English', 'un'])
        .map(json.dumps).to_textfiles(nbs_outdir+'/*.jsonl'))
    print('num after english filter', count_num_recs_total(nbs_outdir))


    assert '/scratch/jupyter-pipeline' in nbs_indir
    shutil.rmtree(nbs_indir, ignore_errors=True)


def filter_on_condition(indir, outdir, func):
    (db.read_text(indir +'/*.jsonl').map(json.loads)
     .filter(lambda js: func(js))
     .map(json.dumps).to_textfiles(outdir+'/*.jsonl'))
def count_num_recs_total(indir):
    return db.read_text(indir+'/*.jsonl').count().compute()
def delete_create_dir(outdir):
    assert '/scratch/jupyter-pipeline' in outdir
    shutil.rmtree(outdir, ignore_errors=True)
    Path(outdir).mkdir(exist_ok=True)

def markdown_ratio(nb):
    markdowns = [1 if is_markdown(c) else 0 for c in nb['cells']]
    if len(markdowns):
        return sum(markdowns)/len(markdowns)
    return 0

def filter_for_markdown(nbs_indir, nbs_outdir, min_markdown_ratio):
    delete_create_dir(nbs_outdir)
    def filter_func(nb):
        return markdown_ratio(nb) >= min_markdown_ratio
    filter_on_condition(nbs_indir, nbs_outdir, filter_func)

    print('num after markdown filter', count_num_recs_total(nbs_outdir))

    assert '/scratch/jupyter-pipeline' in nbs_indir
    shutil.rmtree(nbs_indir, ignore_errors=True)


def get_kernel_name(nb):
    if 'kernelspec' in nb['metadata']:
        # return nb['metadata']['kernelspec']['name']
        if 'language' in nb['metadata']['kernelspec']:
            return nb['metadata']['kernelspec']['language']
        else:
            return 'no lang'
    else:
        return 'no kernel specified'

def filter_for_python(nbs_indir, nbs_outdir):
    # assert '/scratch/nbgrader-pipeline/dask-gen' in nbs_outdir
    assert '/scratch/jupyter-pipeline' in nbs_outdir
    shutil.rmtree(nbs_outdir, ignore_errors=True)
    Path(nbs_outdir).mkdir(exist_ok=True)

    # bag=db.read_text(nbs_indir +'/*.jsonl').map(json.loads)
    # kernel_type = \
    #      bag.map(lambda nb: get_kernel_name(nb)) \
    #         .frequencies() \
    #         .topk(k=10, key=lambda tup: tup[1])
    # print('Counts of languages used for kernel===========')
    # pprint(kernel_type.compute())

    db.read_text(nbs_indir +'/*.jsonl').map(json.loads) \
      .filter(lambda nb: get_kernel_name(nb) in ['python', 'python2']) \
      .map(json.dumps).to_textfiles(nbs_outdir+'/*.jsonl')

    print('num after python filter', count_num_recs_total(nbs_outdir))

    assert '/scratch/jupyter-pipeline' in nbs_indir
    shutil.rmtree(nbs_indir, ignore_errors=True)



############################ special filters to get train distribution to look more like nbgrader
#################################################################################################



def filter_code_tags(nbs_indir, nbs_outdir):
    delete_create_dir(nbs_outdir)
    def filter_func(nb):
        for c in nb['cells']:
            if is_markdown(c) and is_code_tag_in_nl(c['source']):
                return True
        return False

    filter_on_condition(nbs_indir, nbs_outdir, filter_func)

    print('num after code tag filter', count_num_recs_total(nbs_outdir))
