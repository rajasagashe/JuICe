''' This script extracts the code that doesn't occur in the boilerplate code,
and dedups the nbgrader dataset.
'''

import copy
import difflib
import logging
import random
from collections import defaultdict, Counter

import numpy as np
import pandas as pd
from dotmap import DotMap
from tqdm import tqdm

from jupyter import jdumpl, get_files_under_dir, jloadl
from jupyter.jupyter_utils import is_markdown
from jupyter.nbgrader.checksum_util import compute_checksum
from jupyter.new_pipeline.to_dataset import replace_newlines_indents
from jupyter.preprocess.tokenize_utils import tokenize_and_templatize

logger = logging.getLogger(__name__)
random.seed(1)


def extract_target_code(boilerplate, solution):
    ''' e.g.
    boilerplate:
        def get_kurtosis():
            return []
    solution:
        def get_kurtosis():
            return [2, -0.5, (4*math.gamma(3/4)**2)/math.gamma(1/4)**2, 6]
    We must generate only the return statement.

    :param boilerplate: The code cell presented to students.
    :param solution: The code cell after student modifications.
    :return: The lines of code the student added.
    '''
    d = difflib.Differ()
    diff = d.compare(boilerplate.splitlines(),
                     solution.splitlines())

    arr = []
    diffed_arr = []
    for x in diff:
        diff_string = str(x)
        if diff_string.startswith('+'):
            # diffed_arr.append(diff_string)
            stripped = diff_string.strip('+').strip()
            # arr.append(1)
            if (stripped
                and not stripped.startswith('"""')
                and not stripped.startswith('#')):
                # don't count trivial newlines or comments as insertions
                if 'import ' not in stripped:
                    # don't add imports since they may split up the code into multiple as shown
                    # in example get_kurtosis above where student also added an import math statement
                    # at the top.
                    diffed_arr.append(diff_string)
                    arr.append(1)
        elif not diff_string.startswith('-'):
            # empty lines shouldn't be added because they split up the + blocks.
            if diff_string.strip() and not diff_string.startswith('?') and not diff_string.strip().startswith('#') and not diff_string.strip().startswith('raise'):
                diffed_arr.append(diff_string)
                arr.append(0)


    if not sum(arr):
        # no insertions found
        return None

    # The purpose of this numpy logic is to chunk together the additions in a
    # clean/efficient way. The type_change_locs captures where a different chunk
    # so we split on this to get the chunks.

    # each value will be xord with value before it, thus if the index with an
    # insertion will become 1 since the value before it was 0
    next_val = np.roll(arr, 1)
    # the rolling wraps around, we don't want this to happen for first element
    # so this assignment will make sure xor is 0
    next_val[0] = arr[0]
    type_change_locs = np.logical_xor(arr, next_val)

    indices = np.where(type_change_locs == 1)
    chunked = np.split(diffed_arr, indices[0])

    cells = []

    for chunk in chunked:
        cell = {
            'source': '\n'.join(chunk),
            'cell_type': 'code'
        }
        # indicates target code block
        if chunk[0].startswith('+'):
            cell['is_solution'] = True
        else:
            cell['is_solution'] = False
        cells.append(cell)

    solutions = [c for c in cells if c['is_solution']]
    num_solutions= len(solutions)

    # We want 1 consecutive block that was filled in, not multiple since it will
    # be complicated.
    if num_solutions == 1:
        lines = []
        for line in solutions[0]['source'].splitlines():
            # First two characters are added '+ ' so we remove them.
            lines.append(line[2:])
        return '\n'.join(lines)


    return ''

def get_index(tokens, string):
    '''Different from list().index() method since it checks whether string contained
    in an element.'''
    for i, t in enumerate(tokens):
        if string in t:
            return i
    return -1

def get_in_between_solution_comments(code_tokens):
    '''Return code indices between begin and end solution.'''
    assert isinstance(code_tokens, list)
    # none code tokens means it didn't parse likely since empty implementation
    if code_tokens:
        # we need a special function since the comments for these two
        # don't follow the standard ### BEGIN SOLUTION format. e.g. it may
        # be ### BEGIN SOLUTION HERE or have an extra space at the end
        start = get_index(code_tokens, 'BEGIN SOLUTION')
        end = get_index(code_tokens, 'END SOLUTION')
        if start == -1 or end == -1:
            return []

        solution = code_tokens[start+1:end]
        if ' '.join(solution).strip():
            return start+1, end
            # return solution, code_tokens[:start+1] + code_tokens[end:]

    return []


def diff_cell(code, boiler):
    if boiler == '# YOUR CODE HERE\nraise NotImplementedError()':
        # no need to run fancy extraction if trivial boilerplate.
        return code

    extracted = extract_target_code(boiler, code)
    if extracted:
        return extracted
    return ''



def handle_solution(all_cells):
    ''' extract code within begin solution tag. requirement is that its non empty
    and multiple cells have the same solution.'''

    # filter cells with empty solution, these can become boilerplate!!
    implemented_solutions =[]
    for c in all_cells:
        if c['code_tokens']:
            if get_in_between_solution_comments(c['code_tokens']):
                implemented_solutions.append(c)
    del all_cells

    # dictionary of solution to cell indices
    sol2idx = defaultdict(list)
    for i, c in enumerate(implemented_solutions):
        sol2idx[c['code']].append(i)

    # we pick from most common
    for indices in sorted(list(sol2idx.values()), key=lambda x:len(x), reverse=True):
        if len(indices) < 2:
            return 'only 1 solution'

        target_cell = implemented_solutions[indices[0]]
        start, end = get_in_between_solution_comments(target_cell['code_tokens'])

        # We tokenize again to get the types
        toks, types = tokenize_and_templatize(target_cell['code'])

        target_cell['extracted_code_tokens'] = replace_newlines_indents(toks[start:end],
                                                                        types[start:end],
                                                                        enable_assert=False, strings=True,comments=True)
        target_cell['extracted_code_types'] = types[start:end]

        # Get the boilerplate tokens surrounding the solution.
        boiler_toks = toks[:start] + toks[end:]
        boiler_types = types[:start] + types[end:]
        target_cell['boilerplate_code_tokens'] = replace_newlines_indents(boiler_toks, boiler_types,enable_assert=False, strings=True,comments=True)
        target_cell['boilerplate_code_types'] = boiler_types

        return target_cell

def is_trivial_boilerplate(boiler_toks):
    # The "raise NotImplementedError()" is trivial boilerplate since it won't help
    # the model generate the code, meaning there's nothing from this code to incorporate.
    # we find first non newline index this way since the same trivial snippet
    # can have varying number of new lines at the start. thus, this is like a string
    # lstrip
    non_newline = [0 if t == 'NEWLINE' else 1 for t in boiler_toks].index(1)
    if boiler_toks[non_newline:] == ['raise', 'NotImplementedError', '(', ')']:
        return True
    return False



def dedup_boiler_extract(group, boiler_set):
    solutions = group[group.is_boilerplate==False]
    if not solutions.empty:
        # if group.is_boilerplate.any():
        #     return 'both'

        all_cells = [row.target_cell for _, row in solutions.iterrows()]
        # Instructor answers have a different extraction process.
        for c in all_cells:
            if c['is_instructor_answer']:
                return handle_solution(all_cells)


        # Get cells which passed test cases.
        passed_cells = []
        for c in all_cells:
            if 'test_below_passed' in c['metadata'] and c['metadata']['test_below_passed']:
                # Prefer cells from output notebooks by adding to front.
                if 'from_output_nb' in c['metadata'] and c['metadata']['from_output_nb']:
                    passed_cells.insert(0, c)
                else:
                    passed_cells.append(c)

        # If no passed test cases omit examples.
        if not passed_cells:
            return 'no autograder tests or none passed'

        target_cell = passed_cells[0]

        # Retrieve boilerplate, either if its not in our dataset, we try to match
        # all the boilerplates or dataset contains with the cell.
        if group.is_boilerplate.any():
            boiler = group[group.is_boilerplate].iloc[0].target_cell['code']
        else:
            boiler = find_boilerplate_for_checksum(boiler_set, target_cell)


        if boiler:
            # Try to extract the student code from the solution, excluding boilerplate.
            extracted = diff_cell(target_cell['code'], boiler)
            if extracted:
                # boiler_js = get_all_code_process(boiler)
                # extract_js = get_all_code_process(extracted)
                target_cell['extracted_code'] = extracted
                target_cell['boilerplate_code'] = boiler

                # Try to tokenize extracted code and boilerplate.
                try:
                    toks, types = tokenize_and_templatize(extracted)
                    target_cell['extracted_code_tokens'] = replace_newlines_indents(toks, types, enable_assert=False, comments=True, strings=True)
                    target_cell['extracted_code_types'] = types

                    toks, types = tokenize_and_templatize(boiler)
                    boiler_toks = replace_newlines_indents(toks, types, enable_assert=False, comments=True, strings=True)

                    # If trivial just leave boilerplate tokens empty.
                    if is_trivial_boilerplate(boiler_toks):
                        boiler_toks = []
                        types = []
                    target_cell['boilerplate_code_tokens'] = boiler_toks
                    target_cell['boilerplate_code_types'] = types

                    return target_cell
                except:
                    return 'not tokenizable autograded'
            else:
                return 'multiple insertion points'
        else:
            return 'no boilerplate'

    return 'only boiler'


def find_boilerplate_for_checksum(boilerplate_set, cell):
    if 'checksum' not in cell['metadata']['nbgrader']:
        return ''
    for b in (boilerplate_set):
        # We make a DotMap here since compute_checksum likes that format.
        c = DotMap({
            'source': b,
            'cell_type': 'code',
            'metadata': cell['metadata']
        })
        # If the checksum is equivalent this means the boilerplate is indeed what the
        # instructor had provided.
        if compute_checksum(DotMap(c)) == cell['metadata']['nbgrader']['checksum']:
            return b
    return ''


def groupem(cells):
    cell_df = pd.DataFrame(cells)

    # Get all possible boilerplates so we can try these for cells without boilerplates.
    boiler_set = set([c['code'] for c in cells if c['is_boilerplate']])
    logger.info('num unique boilers %s', len(boiler_set))

    # dedup and filter the cells!
    cells = cell_df.groupby('groupbykey').apply(lambda group: dedup_boiler_extract(group, boiler_set))
    cells = cells.tolist()
    logger.info('Failure cause counts %s', Counter([o for o in cells if isinstance(o, str)]).most_common(11))
    cells = [c for c in cells if isinstance(c, dict)]
    logger.info('len cells after filtering failed %s', len(cells))
    return cells


def add_keys(c):
    c.update(c['metadata']['nbgrader'])
    c['target_cell'] = copy.deepcopy(c)

    if 'checksum' not in c['metadata']['nbgrader']:
        c['checksum'] = 'dummy-checksum'
    if 'points' not in c['metadata']['nbgrader']:
        c['points'] = 'dummy-checksum'
    if 'grade_id' not in c['metadata']['nbgrader']:
        c['grade_id'] = 'dummy-checksum'

    nl = ' '.join(c['nl'])
    dist = [x['distance_target'] for x in c['context'] if is_markdown(x)]
    dist = dist[0]

    # we add the checksum since the nl cells "your answer here" requesting a manually
    # entered nl will cause a lot of false collisions. We factor distance since same
    # nl could be used for multiple targets or if they occur consecutively under the
    # same nl.
    c['groupbykey'] = nl + str(dist) + c['checksum']
    return c

def main(cell_indir, dataset_outfile, max_tokens=120):
    cells = []
    # way faster than a bag load
    for path in tqdm(get_files_under_dir(cell_indir, '.jsonl')):
        cells.extend(jloadl(path))
    logger.info('Num cells %s', len(cells))

    # getting our groupbykey and target cell
    cells = list(map(add_keys, cells))

    dataset = groupem(cells)

    # if it doesn't have code tokens, it means the code was likely a comment
    dataset = [j for j in dataset if j['code_tokens_clean']]
    # dataset = [j for j in dataset if j['extracted_code_tokens']]
    logger.info('Len dataset after removing empty code tok records %s', len(dataset))

    dataset = [j for j in dataset if j['extracted_code_tokens']]
    logger.info('Len dataset after removing empty extracted code tok records %s', len(dataset))

    dataset = [j for j in dataset if len(j['extracted_code_tokens']) <= max_tokens]
    logger.info(f'Len dataset after removing tokens more than %s long %s', max_tokens, len(dataset))

    jdumpl(dataset, dataset_outfile)


    code_set = set()
    for js in dataset:
        assert ''.join(js['extracted_code_tokens']) not in ['NEWLINEraiseNotImplementedError()',
                                                            'raiseNotImplementedError()']

        code_set.add(tuple(js['code_tokens_clean']))
    logger.info('num unique code in nbgrader dataset %s', len(code_set))

