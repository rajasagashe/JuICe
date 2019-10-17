import copy
import json
import logging
import random
import shutil
from os.path import abspath
from pathlib import Path

import dask.bag as db
from dask.diagnostics import ProgressBar

from jupyter.jupyter_utils import get_url, is_code, is_markdown
from jupyter.new_pipeline.filter import grading_type

logger = logging.getLogger()

random.seed(1123)


def is_valid_cell(cell):
    is_basic_correct = ('source' in cell and cell['source']
                        and 'cell_type' in cell and cell['cell_type'])
    if not is_basic_correct:
        return False
    # if metadata is in cell, it should be dict
    if 'metadata' in cell and not isinstance(cell['metadata'], dict):
        return False
    assert isinstance(cell['source'], str)

    return True

def standardize_cell(cell):
    if 'source' not in cell or not cell['source']:
        cell['source'] = ''
    if 'cell_type' not in cell:
        cell['cell_type'] = ''

    if 'outputs' not in cell:
        cell['outputs'] = []
    # cell['outputs'] = []
    if 'execution_count' not in cell:
        cell['execution_count'] = 0
    cell.pop('attachments', None)
    cell.pop('level', None)
    cell.pop('prompt_number', None)
    cell.pop('execution_state', None)
    cell.pop('outputExpanded', None)
    cell.pop('_comment', None)

    # i'm sure ive popped all weird keys above, but this additional popping below will guarantee
    # all cells have same key hopefully. this is here if more nbs were scraped which have more
    # unusual keys
    # smart move: more exceptions dict_keys(['cell_type', 'metadata', 'input', 'language', 'outputs', 'collapsed', 'source', 'execution_count'])
    keys = list(cell.keys())
    for k in keys:
        if k not in set(['cell_type', 'execution_count', 'metadata', 'outputs', 'source']):
            cell.pop(k)

    assert cell.keys() == set(['cell_type', 'execution_count', 'metadata', 'outputs', 'source'])
    return cell


def is_ascii(nl):
    try:
        nl.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False

def process_dump_get_cells(nb, nb_vizdir, write_cells=False):
    '''
    add some metadata to cells, dump for vizualization
    for the training set write_cells should be False
        - this saves memory as the notebook is written once
        - link to notebook added to cell

    for nbgrader dev set we write notebook with each record:
        - this way the extension can scroll down to the cell and save time
    :param nb:
    :param nb_vizdir:
    :param write_cells:
    :param filter_code_tags: for noisytrain take cells with nl above having code tags
    :param filter_docstring: for noisy train take cells with a nice function docstring
    :return:
    '''

    nb_to_dump = copy.deepcopy(nb)

    # cell quality control here, non null source, cell type, etc.
    cells = nb['cells']
    nb_index = nb['metadata']['nb_index']


    for cell_index, c in enumerate(cells):
        if 'metadata' not in c or isinstance(c['metadata'], list):
            c['metadata'] = {}
        c['metadata']['nb_index'] = nb_index
        c['metadata']['cell_index'] = cell_index
        c['metadata']['repo'] = nb['metadata']['repo']
        c['metadata']['path'] = nb['metadata']['path']

    # cells = [c for c in cells if is_valid_cell(c)]
    cells = [standardize_cell(c) for c in cells]

    if not write_cells:
        new_cells = []
        # we keep only code cells with markdown that has code tag. we keep
        # all markdown cells.
        for i, c in enumerate(cells):
            if is_code(c) and i:
                cell_above = cells[i-1]
                if is_markdown(cell_above):
                    if is_ascii(cell_above['source']):
                        # confirm that ascii conversion works
                        new_cells.append(c)
                    else:
                        # just remove an entire notebook if it has unicode!
                        # otherwise context will have unicode characters.
                        return []
        del cells


        # this is for the noisy train
        # dump the whole notebook once to save memory
        file_path = abspath(nb_vizdir + f'/nb_{nb_index}.ipynb')

        for c in new_cells:
            c['metadata']['nb_orig_url'] = get_url(file_path)

        # dump the whole notebook to see the ctx with cell
        with open(file_path, 'w') as outfile:
            json.dump(nb_to_dump, outfile)

        return new_cells
    else:

        found_test = False
        test_passed = False
        from_output_nb = False
        # we go in reverse since the tests come after the autograded code block
        for c in reversed(cells):
            if grading_type(c) == 'autograder tests':
                found_test = True

                # later outputs will be removed so important to capture this
                if c['outputs']:
                    from_output_nb = True

                # if the test has an error or was never executed, we decide that the tests didn't pass
                if ((c['outputs'] and 'output_type' in c['outputs'][0] and c['outputs'][0]['output_type'] == 'error')
                    or c['execution_count'] == 0):
                    test_passed = False
                else:
                    test_passed = True
            elif grading_type(c) in ['autograded code', 'manual graded code']:
                # we'll prefer records that come from a notebook with outputs
                c['metadata']['from_output_nb'] = from_output_nb
                if found_test:
                    c['metadata']['test_below_passed'] = test_passed


        # this is for dev/test set where we take only nbgrader cells
        nbgrader_cells = []
        for cell_index, c in enumerate(cells):
            if 'nbgrader' in c['metadata']:
                # we write the title in this format so the extension will
                # scroll to this c
                file_path = abspath(nb_vizdir + f'/nb_{nb_index}_cell_{cell_index}.ipynb')

                # add nb and location to c for later visualization
                c['metadata']['nb_orig_url'] = get_url(file_path)

                # dump the whole notebook to see the ctx with c
                with open(file_path, 'w') as outfile:
                    json.dump(nb_to_dump, outfile)

                nbgrader_cells.append(c)

        return nbgrader_cells

def dump_cells(nbs_dir, dataset_outdir, viz_outdir, write_cells=False):
    shutil.rmtree(dataset_outdir, ignore_errors=True)
    shutil.rmtree(viz_outdir, ignore_errors=True)
    Path(dataset_outdir).mkdir(exist_ok=True)
    Path(viz_outdir).mkdir(exist_ok=True)

    logger.info('')

    with ProgressBar(minimum=15):
        (db.read_text(nbs_dir+'/*.jsonl')
            # todo-release delete this
            # .random_sample(.0001, random_state=1)
            .map(json.loads).
            map(process_dump_get_cells, nb_vizdir=viz_outdir, write_cells=write_cells).
            flatten().
            map(json.dumps).to_textfiles(dataset_outdir+'/*.jsonl'))

    logger.info('Num total cells %s', db.read_text(dataset_outdir+'/*.jsonl').count().compute())

