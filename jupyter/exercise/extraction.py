''' This script 
usage:

'''

import json
import logging
from os.path import basename, dirname, join, abspath
from pathlib import Path

import dask.bag as db
from dask.diagnostics import ProgressBar
from datasketch import MinHash, MinHashLSH

from jupyter import jdumpl
from jupyter.jupyter_utils import is_code, get_url
from jupyter.new_pipeline.to_dataset import compute_dataset_record_helper

logger = logging.getLogger(__name__)


def compute_minhash(lst):
    m1 = MinHash(num_perm=128)
    for d in set(lst):
        m1.update(d.encode('utf8'))
    return m1

def create_minhashlsh(minhashes):
    lsh = MinHashLSH(threshold=0.5, num_perm=128)
    for i, hashe in enumerate(minhashes):
        lsh.insert(i, hashe)
    return lsh

def get_nb_minhash(nb):
    lst = []
    # for cell in _cells(nb):
    for cell in nb['cells']:
        # this covers markdown and code cells
        if 'source' in cell and cell['source']:
            assert type(cell['source']) == type('')
            lst.append(cell['source'])
    return compute_minhash(lst)

def dump_nb(outdir, row, cell_index):
    filebase = basename(row.path).replace('_', '-').replace('.ipynb', '') + f'nb_1_cell_{cell_index}.ipynb'
    # print('dirname', dirname(row.path))

    # the dirname takes format '/checkpoint' and join will discard previous args
    # since its a abs path, so we remove the forward slash
    checkpoint_dir = dirname(row.path)
    if checkpoint_dir.startswith('/'):
        checkpoint_dir = checkpoint_dir[1:]

    filedir = join(outdir, checkpoint_dir)
    filepath = join(filedir, filebase)

    Path(filedir).mkdir(exist_ok=True, parents=True)
    with open(filepath, 'w') as outfile:
        outfile.write(row.contents.strip())
        return filepath

def compute_problem_cells(row_sol, row_sub, nb_vizdir, context_len):
    ''' We compare each cell between exercise and solution notebook
    to find the cells intended for student to fill. '''
    cells = []

    # load em both, see if same num cells, try to diff.
    nb_sol = json.loads(row_sol.contents)
    nb_sub = json.loads(row_sub.contents)
    # we require solution and submission to have same number of cells so we
    # can identify problem cells automatically.
    if len(nb_sol['cells']) != len(nb_sub['cells']):
        return []

    for i, (csol, csub) in enumerate(zip(nb_sol['cells'], nb_sub['cells'])):

        if is_code(csol) and csol['source'] != csub['source']:
            # set the right metadata
            csol['metadata'] = {}
            csol['metadata']['repo'] = row_sol.repo
            csol['metadata']['path'] = row_sol.path
            csol['metadata']['boilerplate'] = csub['source']
            csol['metadata']['boilerplate_path'] = row_sub.path

            sol_path = dump_nb(nb_vizdir, row_sol, i)
            sub_path = dump_nb(nb_vizdir, row_sub, i)
            # for c in cells:
            csol['metadata']['url'] = get_url(abspath(sol_path))
            csol['metadata']['submission_url'] = get_url(abspath(sub_path))

            # convert the solution cell into a dataset record
            js = compute_dataset_record_helper(c=csol, nb_cells=nb_sol['cells'],
                                          cell_index=i, context_len=context_len, max_tokens=1111111)
            cells.append(js)


    return cells

def load_nb(group, i):
    return json.loads(group.iloc[i].contents)
def get_solution_cells(group, nb_vizdir, context_len):
    '''Hash each notebook within repo to find notebooks with 50% of cells overlapping.
    '''
    cells = []

    # dump all nbs into lsh
    hashes = [get_nb_minhash(json.loads(row.contents)) for _, row in group.iterrows()]
    lsh = create_minhashlsh(hashes)

    solution_paths = set()
    for i, minhash in enumerate(hashes):
        if 'solution' in group.iloc[i].path.lower():

            result = lsh.query(minhash)
            # get indices of potential submission notebooks
            sub_inds = [j for j in result if j != i]
            sub_inds = [j for j in sub_inds if 'solution' not in group.iloc[j].path.lower()]

            # for now we loop over all candidates to be safe, but should likely be 1.
            for j in sub_inds:
                cells.extend(compute_problem_cells(group.iloc[i],
                                                   group.iloc[j],
                                                   nb_vizdir,
                                                   context_len))
                solution_paths.add(group.iloc[i].path)

    return cells


def extract(indir, outdir, nb_vizdir, context_len):
    '''Get all dataset records from solution cells.'''
    logger.info('')
    def nb_to_rec_format(nb):
        return {'contents': json.dumps(nb),
                'repo': nb['metadata']['repo'],
                'path': nb['metadata']['path']}

    nbs = (db.read_text(indir+'/*.jsonl', blocksize='120mib').
           map(json.loads).
           # for compatibility with extraction code above change back to rec format, so
           # we can group by repo
           map(nb_to_rec_format)
           .to_dataframe())

    with ProgressBar():
        nb_cells = nbs.groupby('repo').apply(
            (lambda group: get_solution_cells(group, nb_vizdir, context_len)), meta=object).compute()

        logger.info('num repos %s', len(nb_cells))
        logger.info('num repos at least 1 solution extracted %s', len([c for c in nb_cells if c]))

    nb_cells = nb_cells.tolist()
    cells = [c for n in nb_cells for c in n if n and c]
    logger.info('num extracted solution cells %s', len(cells))
    jdumpl(cells, outdir + '/cells.jsonl')


