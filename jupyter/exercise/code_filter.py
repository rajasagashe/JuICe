''' This script basically the same as one in nbgrader pipeline.
'''

import ast
import logging
import json
import random
import shutil
from pathlib import Path

import dask.bag as db
from dask.diagnostics import ProgressBar
from dotmap import DotMap

from jupyter.nbgrader.checksum_util import compute_checksum
from jupyter.preprocess.tokenize_utils import tokenize_and_templatize, gen_api_seq

random.seed(1123)

logger = logging.getLogger(__name__)


def remove_magic_inline(cell, key):
    lines = []
    for line in cell[key].splitlines():
        if not line.strip().startswith('%'): #and not line.strip().startswith('print'):
            lines.append(line)
    code = '\n'.join(lines)
    cell[key] = code
    return cell

def rajas_print_fix(cell, key):
    lines = cell[key].splitlines()
    new_lines = []
    for line in lines:
        # line = line.strip()
        if line.strip().startswith('print '):
            line = line.replace('print ', 'print(')
            line += ')'
        new_lines.append(line)
    cell[key] = '\n'.join(new_lines)
    return cell

def does_parse(original_code, url=None):
    assert isinstance(original_code, str)
    try:
        x = ast.parse(original_code)
        y = tokenize_and_templatize(original_code)
        return True
    except:
        return False

def mark_cell_boilerplate_solution(cell):
    # some buggy cells don't have grade_id, so we insert
    if 'grade_id' not in cell['metadata']['nbgrader']:
        cell['metadata']['nbgrader']['grade_id'] = 'dummy!!!id'

    # x = compute_checksum(DotMap(cell))
    # assert 'checksum' in cell['nbgrader']
    cell['metadata']['nbgrader']['is_boilerplate'] = (
        'checksum' in cell['metadata']['nbgrader']
        and compute_checksum(DotMap(cell)) == cell['metadata']['nbgrader']['checksum'])

    cell['metadata']['nbgrader']['is_instructor_answer'] = 'BEGIN SOLUTION' in cell['source']
    return cell

def temp(c):
    if not c:
        print(c)
        raise ValueError
    assert c
    if not c['code']:
        print(c)
        raise ValueError
    return c

def filter_parseable_code_cells(cells_indir, cells_outdir, code_key='source'):
    shutil.rmtree(cells_outdir, ignore_errors=True)
    Path(cells_outdir).mkdir(exist_ok=True)

    logger.info('num cells before filtering parseable %s', db.read_text(cells_indir+'/*.jsonl').count().compute())

    with ProgressBar(minimum=15):
        (db.read_text(cells_indir+'/*.jsonl', blocksize='180mib').map(json.loads)
    # .map(temp)
     # .filter(lambda cell: is_code(cell))

    # todo add these in later
    #  .map(lambda cell: remove_magic_inline(cell, code_key))
    #  .map(lambda cell: rajas_print_fix(cell, code_key))
     .filter(lambda cell: cell and does_parse(cell[code_key]))
     .map(json.dumps).to_textfiles(cells_outdir+'/*.jsonl'))

    logger.info('num cells after filtering parseable %s', db.read_text(cells_outdir+'/*.jsonl').count().compute())
    # exit('yo')

def grading_type(cell):
    if 'nbgrader' in cell['metadata']:
        data = cell['metadata']['nbgrader']


        if 'solution' in data and 'grade' in data:
            if (data['solution']) and (data['grade'] == False):
                return 'autograded code'
            elif (data['solution']) and (data['grade']):
                return 'manual graded code'
            elif (data['solution'] == False) and (data['grade']):
                return 'autograder tests'
    # todo investigate unknown more
    return 'unknown'

def is_boilerplate(cell):
    return 'nbgrader' in cell['metadata'] and cell['metadata']['nbgrader']['is_boilerplate']

def logic_type(cell, max_api_seq_len, min_api_seq_len, key):
    if is_boilerplate(cell):
        # boilerplate may not tokenize so leave it
        return 'boilerplate'

    try:
        toks, types = tokenize_and_templatize(cell[key])
    except:
        return 'untokenizeable'

    api_seq = gen_api_seq(toks, types)
    if len(api_seq) > max_api_seq_len:
        return f'api seq longer than {max_api_seq_len}'

    if len(api_seq) < min_api_seq_len:
        return f'api seq shorter than {min_api_seq_len}'


    if 'class' in toks:
        return 'class'
    elif toks.count('def') > 1:
        return 'more than 1 function'
    elif toks.count('def') == 1:
        return '1 function'
    else:
        return 'pure logic'


def one_func_max_api_seq(cells_indir, cells_outdir, max_api_seq_len, min_api_seq_len, code_key):
    # assert '/scratch/jupyter-pipeline' in cells_outdir or cells_outdir.startswith('/tmp')
    shutil.rmtree(cells_outdir, ignore_errors=True)
    Path(cells_outdir).mkdir(exist_ok=True)


    bag=db.read_text(cells_indir +'/*.jsonl').map(json.loads)
    kernel_type = \
        bag.map(lambda cell: logic_type(cell, max_api_seq_len, min_api_seq_len, code_key)) \
            .frequencies() \
            .topk(k=50, key=lambda tup: tup[1])

    logger.info('Counts of function/class type %s', kernel_type.compute())

    with ProgressBar(minimum=15):
        db.read_text(cells_indir +'/*.jsonl').map(json.loads) \
        .filter(lambda cell: logic_type(cell, max_api_seq_len, min_api_seq_len, code_key) in ['1 function', 'pure logic', 'boilerplate']) \
        .map(json.dumps).to_textfiles(cells_outdir+'/*.jsonl')


