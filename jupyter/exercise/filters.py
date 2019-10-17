import json
import logging
import shutil
from pathlib import Path

import dask.bag as db
from polyglot.detect import Detector

from jupyter.jupyter_utils import is_markdown

from dask.diagnostics import ProgressBar
logger = logging.getLogger(__name__)

def log_delete_indir(func):
    def wrapper(*args, **kwargs):
        # logger.info("Ordering: %s", func.__name__)
        print(func.__name__)
        print(args)
        func(*args, **kwargs)
        # logger.debug("Order result: %s", order.result)
    return wrapper

def count_num_recs_total(indir):
    return db.read_text(indir+'/*.jsonl').count().compute()


def get_kernel_name(nb):
    # print(nb)
    if 'metadata' in nb and 'kernelspec' in nb['metadata']:
        # return nb['metadata']['kernelspec']['name']
        if 'language' in nb['metadata']['kernelspec']:
            return nb['metadata']['kernelspec']['language']
        else:
            return 'no lang'
    else:
        return 'no kernel specified'

def filter_for_python(nbs_indir, nbs_outdir):
    # assert '/scratch/nbgrader-pipeline/dask-gen' in nbs_outdir
    assert '/tmp' in nbs_outdir
    shutil.rmtree(nbs_outdir, ignore_errors=True)
    Path(nbs_outdir).mkdir(exist_ok=True)

    # bag=db.read_text(nbs_indir +'/*.jsonl').map(json.loads)
    # kernel_type = \
    #      bag.map(lambda nb: get_kernel_name(nb)) \
    #         .frequencies() \
    #         .topk(k=10, key=lambda tup: tup[1])
    # print('Counts of languages used for kernel===========')
    # pprint(kernel_type.compute())

    with ProgressBar(15):
        db.read_text(nbs_indir +'/*.jsonl').map(json.loads) \
        .filter(lambda nb: get_kernel_name(nb) in ['python', 'python2']) \
        .map(json.dumps).to_textfiles(nbs_outdir+'/*.jsonl')

    logger.info('num after python filter %s', count_num_recs_total(nbs_outdir))

    # assert '/scratch/jupyter-pipeline' in nbs_indir
    # shutil.rmtree(nbs_indir, ignore_errors=True)

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
    assert '/tmp' in nbs_outdir
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
    with ProgressBar(15):
        (db.read_text(nbs_indir +'/*.jsonl').map(json.loads)
     # un language means no nl, for these likely its all in the comments
     .filter(lambda nb: get_markdown_language(nb) in ['English', 'un'])
     .map(json.dumps).to_textfiles(nbs_outdir+'/*.jsonl'))
    logger.info('num after english filter %s', count_num_recs_total(nbs_outdir))


    # assert '/scratch/jupyter-pipeline' in nbs_indir
    # shutil.rmtree(nbs_indir, ignore_errors=True)
