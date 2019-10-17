import copy
import json
import logging
from collections import Counter

import dask.bag as db
from dask.diagnostics import ProgressBar

from jupyter import jdumpl
from jupyter.jupyter_utils import is_markdown
from jupyter.new_pipeline.to_dataset import replace_newlines_indents
from jupyter.preprocess.tokenize_utils import tokenize_and_templatize

logger = logging.getLogger(__name__)

def get_solution_new(group):
    '''We deduplicate solutions and pick most common solution.'''
    cells = [row.target_cell for _, row in group.iterrows()]

    solution2count = Counter()
    for c in cells:
        solution2count[c['code']] += 1
    submission2count = Counter()
    for c in cells:
        submission2count[c['metadata']['boilerplate']] += 1


    most_comm_sol = solution2count.most_common(1)[0][0]
    most_comm_sub = submission2count.most_common(1)[0][0]
    if (solution2count.most_common(1)[0][1] > 1 and
        submission2count.most_common(1)[0][1] > 1):
        top_solution_cells = [c for c in cells if c['code'] == most_comm_sol]

        # verify from different repositories
        repos = set([c['metadata']['repo'] for c in top_solution_cells])
        if len(repos) < 2:
            return 'only from 1 repo'

        # we arbitrarily pick the first one
        target_cell = top_solution_cells[0]
        if not target_cell['code_tokens']:
            return 'empty target cell tokens'


        # Now we want to pick code blocks with an empty boilerplate.
        target_cell['boilerplate_code'] = most_comm_sub
        if most_comm_sub.strip():
            try:
                toks, types = tokenize_and_templatize(most_comm_sub)
            except:
                return 'boiler tokenization failed'
            replaced = replace_newlines_indents(toks, types, enable_assert=False, comments=True)
            replaced = [t for t in replaced if t not in ['NEWLINE', 'INDENT']]
            if len(replaced) > 10:
                # i like 10 as a threshold since some spurious submissions are actually answers
                # and hence would be a spurious boilerplate. the main motivation is to allow comments
                # which would be removed by the replaced above. unfortunately empty function
                # boilerplates also removed
                return 'boilerplate longer than 10 toks'

        target_cell['boilerplate_code_tokens'] = []

        return target_cell

    else:
        return 'only 1 solution'

def add_keys(c):
    '''Grouping on the nl and distance from it.'''
    if 'nl' not in c:
        if 'comments' in c and c['comments']:
            c['nl'] = c['comments']
        else:
            c['nl'] = ['yo']
    nl = ' '.join(c['nl'])
    dist = [x['distance_target'] for x in c['context'] if is_markdown(x)]
    dist = dist[0] if dist else 0

    new_js = {}
    new_js['groupbykey'] = nl + str(dist)
    new_js['target_cell'] = copy.deepcopy(c)

    return new_js

def main(cell_indir, dataset_outfile, max_tokens):
    logger.info('')
    with ProgressBar(minimum=15):
        cell_df = db.read_text(cell_indir +'/*.jsonl').map(json.loads).map(add_keys).to_dataframe()

        dataset = cell_df.groupby('groupbykey').apply((lambda group: get_solution_new(group)),
                                                      meta=object).compute()

        dataset = dataset.tolist()
        logger.info('Total in unfiltered dataset %s', len(dataset))
        logger.info('Output breakdown %s', Counter([js if isinstance(js, str) else 'success' for js in dataset]).most_common(11))
        dataset = [r for r in dataset if isinstance(r, dict)]
        logger.info('len dataset %s', len(dataset))



    dataset = [j for j in dataset if j['code_tokens_clean']]
    logger.info('len dataset after removing empty code tok records %s', len(dataset))

    # we add 5 here since comments may be in code_tokens, but we have changed this
    # in to_dataset
    dataset = [j for j in dataset if len(j['code_tokens_clean']) <= max_tokens+5]
    logger.info('len dataset after removing tokens more than %s long %s', max_tokens, len(dataset))

    jdumpl(dataset, dataset_outfile)

    # count non num
    # logger.info('num non none target solution', len([js for js in dataset if js['code_tokens']]))
    logger.info('num alternate target solution %s', len([js for js in dataset if 'alternate_api_sequence' in js]))

    code_set = set()
    for js in dataset:
        if not js['code_tokens_clean']:
            print(js['code'])
        code_set.add(tuple(js['code_tokens_clean']))
    logger.info('num unique code in dataset %s', len(code_set))
