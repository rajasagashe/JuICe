''' This script 
usage:
'''

import ast
import copy
import json
import random
import shutil
from pathlib import Path

import dask.bag as db
from dask.diagnostics import ProgressBar
from dotmap import DotMap

from jupyter.jupyter_utils import is_code, is_markdown
from jupyter.nbgrader.checksum_util import compute_checksum
from jupyter.preprocess.tokenize_utils import tokenize_and_templatize, gen_api_seq

import logging
logger = logging.getLogger(__name__)


random.seed(1123)


def remove_magic_inline(cell):
    lines = []
    for line in cell['source'].splitlines():
        if not line.strip().startswith('%'): #and not line.strip().startswith('print'):
            lines.append(line)
    code = '\n'.join(lines)
    cell['source'] = code
    return cell

def python2_print_fix(cell):
    lines = cell['source'].splitlines()
    new_lines = []
    for line in lines:
        # line = line.strip()
        if line.strip().startswith('print '):
            line = line.replace('print ', 'print(')
            line += ')'
        new_lines.append(line)
    cell['source'] = '\n'.join(new_lines)
    return cell

from lib2to3.refactor import RefactoringTool


def convert2to3(cell):
    original_code = cell['source']
    try:
        x = ast.parse(original_code)
        y = tokenize_and_templatize(original_code)
        return cell
    except:
        pass

    # fixers = ['lib2to3.fixes.fix_print']
    # refactor = RefactoringTool(fixer_names=fixers)
    # tree = refactor.refactor_string(original_code, 'temp')
    try:

        # fixers = get_fixers_from_package('lib2to3.fixes')
        # the only python2 issue is print statements so we only import this
        # fixer since it makes RefactorTool way faster
        fixers = ['lib2to3.fixes.fix_print']
        refactor = RefactoringTool(fixer_names=fixers)
        tree = refactor.refactor_string(original_code, 'temp')
        converted_code = str(tree)
        print(converted_code)
        x = ast.parse(converted_code)
        # y = tokenize_and_templatize(converted_code)

        # print('===============\n', original_code, '\n', converted_code)
        cell['source'] = converted_code
    except Exception as e:
        print(e)
        # print('==========\n', print(original_code))
        pass

    return cell



def does_parse(original_code, url=None):
    assert isinstance(original_code, str)
    # lines with %matplotlib inline don't parse, we'll keep them for now
    # lines = []
    # for line in original_code.splitlines():
    #     if not line.strip().startswith('%'):
    #         lines.append(line)
    # code = '\n'.join(lines)

    try:
        x = ast.parse(original_code)
        y = tokenize_and_templatize(original_code)
        return True
    except:
        # todo deal with these
        '''
        what gets removed:
        - slight indentation issue
        '''

        # print('=========\n', original_code, url)
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

    # todo check for end solution as well. in addition use the same logic as in dedup_get_sol
    # since its more complete
    cell['metadata']['nbgrader']['is_instructor_answer'] = 'BEGIN SOLUTION' in cell['source']
    return cell


def filter_parseable_code_cells(cells_indir, cells_outdir, isnbgrader_logic, key='source'):
    '''Filter out cells that dont parse. Preprocess code by converting python2 to 3 and
    removing ipython inline statements to increase odds of parsing. '''

    shutil.rmtree(cells_outdir, ignore_errors=True)
    Path(cells_outdir).mkdir(exist_ok=True)

    logger.info('Num cells before %s', db.read_text(cells_indir+'/*.jsonl').count().compute())

    if isnbgrader_logic:
        # nbgrader we want to keep unparseable cells if they're boilerplate which may not parse,
        #  so we have a slightly modified version
        with ProgressBar(minimum=15):
            (db.read_text(cells_indir+'/*.jsonl').map(json.loads)
            .map(mark_cell_boilerplate_solution)
            .filter(lambda cell: is_code(cell))
            .map(lambda cell: remove_magic_inline(cell))

            .map(lambda cell: python2_print_fix(cell))
            # boilerplate cells don't have to parse
            .filter(lambda cell: does_parse(cell['source'], cell['metadata']['nb_orig_url']) or cell['metadata']['nbgrader']['is_boilerplate'])
            .map(json.dumps).to_textfiles(cells_outdir+'/*.jsonl'))
    else:
        with ProgressBar(minimum=15):
            (db.read_text(cells_indir+'/*.jsonl').map(json.loads)
         .filter(lambda cell: is_code(cell))
         .map(lambda cell: remove_magic_inline(cell))
         .filter(lambda cell: does_parse(cell['source']))
         .map(json.dumps).to_textfiles(cells_outdir+'/*.jsonl'))

    logger.info('Num cells after %s', db.read_text(cells_outdir+'/*.jsonl').count().compute())

def grading_type(cell):
    if 'nbgrader' in cell['metadata']:
        data = cell['metadata']['nbgrader']

        if 'solution' not in data and 'grade' in data and data['grade']:
            return 'autograder tests'

        if 'solution' in data and 'grade' in data:
            if (data['solution']) and (data['grade'] == False):
                return 'autograded code'
            elif (data['solution']) and (data['grade']):
                return 'manual graded code'
            elif (data['solution'] == False) and (data['grade']):
                return 'autograder tests'

            # Read only have both set to false
            return 'read only'

    return 'no nbgrader'

def filter_graded_code_cells(cells_indir, cells_outdir):
    '''Keep only autograded code cells since they are high quality.'''
    # assert '/scratch/jupyter-pipeline' in cells_outdir
    shutil.rmtree(cells_outdir, ignore_errors=True)
    Path(cells_outdir).mkdir(exist_ok=True)

    bag = db.read_text(cells_indir +'/*.jsonl').map(json.loads)
    lst = \
        bag.map(lambda cell: grading_type(cell)) \
            .frequencies() \
            .topk(k=50, key=lambda tup: tup[1])

    logger.info('Counts of grading types of code cells %s', lst.compute())

    with ProgressBar(minimum=15):
        db.read_text(cells_indir +'/*.jsonl').map(json.loads) \
        .filter(lambda cell: grading_type(cell) in ['autograded code']) \
        .map(json.dumps).to_textfiles(cells_outdir+'/*.jsonl')


def is_boilerplate(cell):
    return 'nbgrader' in cell['metadata'] and cell['metadata']['nbgrader']['is_boilerplate']

def logic_type(cell, max_api_seq_len, min_api_seq_len):
    if is_boilerplate(cell):
        # boilerplate may not tokenize so leave it
        return 'boilerplate'

    try:
        toks, types = tokenize_and_templatize(cell['source'])
    except:
        raise ValueError
        # assert False
        # return 'untokenizeable'

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


def one_func_max_api_seq(cells_indir, cells_outdir, max_api_seq_len, min_api_seq_len):
    '''Filter cells with more than 1 function or long api sequence.'''
    shutil.rmtree(cells_outdir, ignore_errors=True)
    Path(cells_outdir).mkdir(exist_ok=True)


    bag=db.read_text(cells_indir +'/*.jsonl').map(json.loads)
    kernel_type = \
        bag.map(lambda cell: logic_type(cell, max_api_seq_len, min_api_seq_len)) \
            .frequencies() \
            .topk(k=50, key=lambda tup: tup[1])

    logger.info('Counts of function/class types')
    logger.info('%s', kernel_type.compute())


    with ProgressBar(minimum=15):
        db.read_text(cells_indir +'/*.jsonl').map(json.loads) \
        .filter(lambda cell: logic_type(cell, max_api_seq_len, min_api_seq_len) in ['1 function', 'pure logic', 'boilerplate']) \
        .map(json.dumps).to_textfiles(cells_outdir+'/*.jsonl')

def add_key(js, cell=False):
    assert 'nb_index' in js['metadata']
    if cell:
        # some cells missing output key which messes up dataframe generation
        # also the they all need to have the same type
        js['outputs'] = []
        js['execution_count'] = 0

        assert js.keys() == set(['cell_type', 'execution_count', 'metadata', 'outputs', 'source'])
        js['og_cell'] = copy.deepcopy(js)

    js['nb_index'] = js['metadata']['nb_index']
    return js

def filter_cells_nl_above_dataframe(cells_indir, cells_outdir, nbs_indir, max_dist):
    '''Filter cells if markdown is more than max_dist away.'''
    shutil.rmtree(cells_outdir, ignore_errors=True)
    Path(cells_outdir).mkdir(exist_ok=True)

    logging.info(f'Num cells before nl dist %s filter %s', max_dist, db.read_text(cells_indir+'/*.jsonl').count().compute())


    # this is required by dask
    cells_meta= {'cell_type': str,
                 'execution_count': int,
                 'metadata': object,
                 'outputs': object,
                 'source': str,
                 'og_cell': object,
                 'nb_index': int}
    # nb_index is added by the add_key function so important to have it
    nb_meta= {'cells': object, 'metadata': object, 'nbformat': int, 'nbformat_minor': int,
              'nb_index': int}

    cells_df = db.read_text(cells_indir +'/*.jsonl').map(json.loads).map(lambda js: add_key(js,cell=True)).to_dataframe(meta=cells_meta)
    nbs_df = db.read_text(nbs_indir +'/*.jsonl').map(json.loads).map(lambda js: add_key(js)).to_dataframe(meta=nb_meta)

    def get_cell(row):
        '''if not nl above return high int representing infinite distance'''
        cells = row.cells
        cell_index = row.metadata_cell['cell_index']
        reversed_cells_before = reversed(cells[:cell_index])
        # iterate over all cells above starting with one directly above
        for i, c in enumerate(reversed_cells_before):
            if i+1 <= max_dist and is_markdown(c):
                return row.og_cell
        # print(row.og_cell['metadata']['nb_orig_url'])
        return {}



    with ProgressBar(minimum=15):
        (cells_df.merge(nbs_df, on='nb_index', suffixes=['_cell', '_nb'])
     .apply(get_cell, meta=object, axis=1).to_bag()
     .filter(lambda js: 'source' in js)
     .map(json.dumps).to_textfiles(cells_outdir + '/*.jsonl'))



    logging.info(f'Num cells after nl dist %s filter %s', max_dist, db.read_text(cells_outdir+'/*.jsonl').count().compute())
