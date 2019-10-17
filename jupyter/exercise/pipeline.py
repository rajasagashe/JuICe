'''Generates dataset from exercise notebooks.
'''
import argparse
import json
import logging
import shutil
import tempfile
from pathlib import Path

import dask.bag as db

from jupyter.exercise import code_filter
from jupyter.exercise import dedup
from jupyter.exercise import extraction
from jupyter.exercise import filters
from jupyter.nbgrader.split import split_simple

format = '%(asctime)s[%(filename)25s:%(lineno)4s - %(funcName)30s()] - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    datefmt='%I:%M:%S %p',
    format=format,
    handlers=[
        logging.FileHandler("exercise.log", mode='w'),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)


parser = argparse.ArgumentParser(description='')
parser.add_argument('-input_nbs_dir', required=True)
parser.add_argument("-pipeline_dir", required=True)
args = parser.parse_args()

Path(args.pipeline_dir).mkdir(parents=True, exist_ok=True)
raw_dump = f'{args.pipeline_dir}/raw-dump'
dataset_dir = f'{args.pipeline_dir}/dataset'
nb_vizdir = f'{args.pipeline_dir}/nbviz'
extracted_dir = f'{args.pipeline_dir}/extracted'
parseable_dir = f'{args.pipeline_dir}/parseable'
onefuncmax_dir = f'{args.pipeline_dir}/onefuncmax'

dev_file = f'{dataset_dir}/dev.jsonl'
test_file = f'{dataset_dir}/test.jsonl'

def beginning_logic(input_nbs_dir):
    Path(dataset_dir).mkdir(exist_ok=True)
    shutil.rmtree(nb_vizdir, ignore_errors=True)
    shutil.rmtree(extracted_dir, ignore_errors=True)
    Path(extracted_dir).mkdir(exist_ok=True)
    Path(nb_vizdir).mkdir()

    def recs_to_nb(indir, outdir):
        '''Add necessary metadata and filter nbgrader nbs'''
        def update_notebook_metadata(line, nb_index=None):
            rec = json.loads(line)
            nb = json.loads(rec['contents'])
            nb['metadata']['repo'] = rec['repo']
            nb['metadata']['path'] = rec['path']
            nb['metadata']['nb_index'] = nb_index
            # nb['metadata']['local_path'] = abspath(join(outdir, f'{nb_index}.ipynb'))
            nb['metadata']['celltoolbar'] = 'Create Assignment'
            return nb

        (db.read_text(indir+'/*.jsonl').
             filter(lambda rec_string: 'nbgrader' not in rec_string).
             map(update_notebook_metadata).
             # when we use full dataset there is invalid json nb
             filter(lambda nb: nb is not None).
             map(json.dumps).to_textfiles(outdir+'/*.jsonl'))


        logger.info('num nbs to start %s', db.read_text(indir+'/*.jsonl').count().compute())

    out = tempfile.mkdtemp(suffix='extract')
    recs_to_nb(input_nbs_dir, out)

    out3 = tempfile.mkdtemp(suffix='extract')
    filters.filter_for_python(out, out3)
    shutil.rmtree(out)

    out2 = tempfile.mkdtemp(suffix='extract')
    filters.filter_for_english(out3, out2)
    shutil.rmtree(out3)
    return out2

logger.info('start')
out2 = beginning_logic(args.input_nbs_dir)
extraction.extract(indir=out2, outdir=extracted_dir, nb_vizdir=nb_vizdir, context_len=11111)
shutil.rmtree(out2)

code_filter.filter_parseable_code_cells(extracted_dir, parseable_dir, code_key='code')

code_filter.one_func_max_api_seq(parseable_dir, onefuncmax_dir, max_api_seq_len=15, min_api_seq_len=0, code_key='code')


dedup_nl_boilerextract_dataset =  dataset_dir+'/nldedup_boilerplateextract.jsonl'
dedup.main(onefuncmax_dir, dedup_nl_boilerextract_dataset, max_tokens=120)#, key='code_tokens', key2='nl')

split_simple(dedup_nl_boilerextract_dataset, dev_file=dev_file, test_file=test_file,
             test_size=.55)

