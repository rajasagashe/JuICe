import ast
import json
import logging
import re
import shutil
from pathlib import Path

import astor
import dask.bag as db
from dask.diagnostics import ProgressBar

from jupyter.jupyter_utils import is_code, is_markdown, is_valid_cell
from jupyter.new_pipeline.filter import add_key
from jupyter.new_pipeline.filter import grading_type
from jupyter.new_pipeline.nl_parse import normalize_nl_leave_code_tokenize
from jupyter.preprocess.tokenize_utils import tokenize_and_templatize, gen_api_seq
from jupyter.preprocess.visitor import normalize_code

logger = logging.getLogger(__name__)


def ignore_this_source(code):
    if code == 'd1000 = 7316717653133062491922511967442657474235534919493496983520312774506326239578318016984801869478851843858615607891129494954595017379583319528532088055111254069874715852386305071569329096329522744304355766896648950445244523161731856403098711121722383113622298934233803081353362766142828064444866452387493035890729629049156044077239071381051585930796086670172427121883998797908792274921901699720888093776657273330010533678812202354218097512545405947522435258490771167055601360483958644670632441572215539753697817977846174064955149290862569321978468622482839722413756570560574902614079729686524145351004748216637048440319989000889524345065854122758866688116427171479924442928230863465674813919123162824586178664583591245665294765456828489128831426076900422421902267105562632111110937054421750694165896040807198403850962455444362981230987879927244284909188845801561660979191338754992005240636899125607176060588611646710940507754100225698315520005593572972571636269561882670428252483600823257530420752963450':
        return True
    if code == 'num = 7316717653133062491922511967442657474235534919493496983520312774506326239578318016984801869478851843858615607891129494954595017379583319528532088055111254069874715852386305071569329096329522744304355766896648950445244523161731856403098711121722383113622298934233803081353362766142828064444866452387493035890729629049156044077239071381051585930796086670172427121883998797908792274921901699720888093776657273330010533678812202354218097512545405947522435258490771167055601360483958644670632441572215539753697817977846174064955149290862569321978468622482839722413756570560574902614079729686524145351004748216637048440319989000889524345065854122758866688116427171479924442928230863465674813919123162824586178664583591245665294765456828489128831426076900422421902267105562632111110937054421750694165896040807198403850962455444362981230987879927244284909188845801561660979191338754992005240636899125607176060588611646710940507754100225698315520005593572972571636269561882670428252483600823257530420752963450':
        return True
    return False

def extract_comments(tokens, types):
    ''' returns each comment seperated by a tok separator.
    :param tokens:
    :param types:
    :return:
    '''
    comments = []
    docstring = None
    # print(tokens, types)
    for i,(tok, type) in enumerate(zip(tokens, types)):
        if type == 'COMMENT':
            comments.extend(tok.split() + ['COMMENTSEP'])
        # if its a docstring
        if type == 'STRING' and i and types[i-1] == 'INDENT':

            tok = tok.replace("'''", ' docstring ').replace('"""', ' docstring ')
            # no word tokenize since docstrings contain mainy special characters
            # which word_tokenize screws up
            comments.extend(tok.split() + ['COMMENTSEP'])
            docstring = tok.split()

    return comments, docstring


def truncate_code_to_lines(toks, newline_char, lines=10):
    num = 0
    for i, tok in enumerate(toks):
        if tok == newline_char:
            num += 1
            if num == lines:
                return toks[:i]
    return toks


def get_imports_simple(nb_cells, cell_index):
    # get all imports including from current cell
    imports = []
    import_toks = []
    for c in nb_cells[:cell_index+1]:
        if is_code(c) and is_valid_cell(c):
            for line in c['source'].splitlines():
                if (line.strip().startswith('import ')
                    # some edge cases have from but no import token
                    or (line.strip().startswith('from ')  and 'import' in line)):

                    # we pick after import to save on total num tokens
                    im =  line[line.index('import'):].strip()
                    imports.append(im)

                    # get toks, we want to handle edge case where multiple comma separated
                    # tokens imported. ideally if there was an 'as' id like to take what
                    # occurs after it, but lets keep it simple for now.
                    for tok in re.split('[^a-zA-Z_]', im):
                        if tok not in ['import', 'as', '']:
                            import_toks.append(tok)
    # print('\n'.join(imports))
    # print(import_toks)
    return import_toks

def get_imports(nb_cells, cell_index):
    # get all imports including from current cell
    imports = []
    for c in nb_cells[:cell_index+1]:
        if is_code(c):
            for line in c['source'].splitlines():
                if (line.strip().startswith('import ')
                    # some edge cases have from but no import token
                    or (line.strip().startswith('from ')  and 'import' in line)):
                    # if 'import' not in line:
                    #     print(line)
                    #     raise ValueError(line)

                    # we pick after import to save on total num tokens
                    imports.append(line[line.index('import'):])
    imports_string = '\n'.join(imports)
    import_toks = []
    try:
        toks, types = tokenize_and_templatize(imports_string)

        # print(toks, types)
        for i,(tok, type) in enumerate(zip(toks, types)):
            if type in ['NAME', 'import', 'as', ',']:
                import_toks.append(tok)

    except:
        # the tokenization failed because of a multiline import using parenthesis
        import_toks = re.split('(\W+)', imports_string)
        # toks look like this: ['import', ' ', 'pyspark', '\n',] so we want to exclude
        # newlines and spaces
        import_toks = [t for t in import_toks if t.strip()]
    return import_toks

def take_single_function(code):
    tree = ast.parse(code)
    new_body = []
    for i in range(len(tree.body)):
        n = tree.body[i]
        # if theres a function we focus on only generating the function
        if isinstance(n, ast.FunctionDef):
            # remove the docstring from the function if one is present
            if isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Str):
                n.body = n.body[1:]
            tree.body = [n]
            return astor.to_source(tree)

        # filter imports
        if not isinstance(n, ast.Import):
            new_body.append(n)

    tree.body = new_body
    return astor.to_source(tree)


def replace_newlines_indents(toks, types, newline_tok='NEWLINE', enable_assert=True, strings=False, comments=False):
    replaced = []
    for to, ty in zip(toks, types):
        # skip dedents
        if ty == 'DEDENT':
            continue
        elif comments and ty == 'COMMENT':
            continue
        # strings canonicalize
        elif (strings and ty == 'STRING'):
            replaced.append("'jupyter_string'")
        # replace newlinles with special tok
        elif to == '\n':
            replaced.append(newline_tok)
        # replace indents with special tok
        elif ty == 'INDENT':
            replaced.append(ty)
        else:
            replaced.append(to)


    if replaced[-1] == newline_tok:
        replaced = replaced[:-1]
    return replaced

def get_all_code_process(code, allow_api_declarations=False):
    '''Lots of old code tokenization. We use "code_tokens_clean" '''
    try:
        targ_tokens, types = tokenize_and_templatize(code)
        api_sequence = gen_api_seq(targ_tokens, types, allow_declarations=allow_api_declarations)
        comments, docstring = extract_comments(targ_tokens, types)

        # full code, with no extraneous newlines, and normalized strings. i plan to use this for
        # context and target code!
        clean_code = astor.to_source(ast.parse(code))
        clean_code_toks, clean_code_types = tokenize_and_templatize(clean_code)
        clean_code_toks = replace_newlines_indents(clean_code_toks, clean_code_types, strings=True, enable_assert=False)


        # todo do a lstrip newlines on targ_tokens

        strip_import_or_func = take_single_function(code)
        strip_import_or_func_toks, strip_types = tokenize_and_templatize(strip_import_or_func)


        normalized = normalize_code(strip_import_or_func)
        normalized_tokens, norm_types = tokenize_and_templatize(normalized)
    except:
        targ_tokens = None
        clean_code_toks = None
        api_sequence = None
        normalized_tokens = None
        norm_types = None
        comments = None
        docstring = None
        strip_import_or_func_toks = None
        strip_types = None

    newline_tok = 'NEWLINE'
    if strip_import_or_func_toks:
        # here we do partial normalization. replace strings, but leave variable names.
        # this is particularly useful for context cells so we can copy initializations
        # from above
        code_tokens_with_vars_no_strings = replace_newlines_indents(strip_import_or_func_toks, strip_types, newline_tok,  strings=True)

        code_tokens_with_vars_no_strings_trunc = truncate_code_to_lines(code_tokens_with_vars_no_strings, newline_tok, 10)
    else:
        code_tokens_with_vars_no_strings_trunc = None

    # we move this logic out of try since better to avoid try if possible
    if normalized_tokens:
        replaced = replace_newlines_indents(normalized_tokens, norm_types, newline_tok)
        normalized_trunc = truncate_code_to_lines(replaced, newline_tok, 10)
        if len(api_sequence) == 0:
            api_sequence = ['NO_API_SEQUENCE']
    else:
        normalized_tokens = None
        normalized_trunc = None

    js = {
        'code': code,
        'code_tokens_normalized': normalized_tokens,
        'code_tokens_normalized_trunc': normalized_trunc,
        'code_tokens_partial_normalized_trunc': code_tokens_with_vars_no_strings_trunc,
        'code_tokens': targ_tokens,
        'code_tokens_clean': clean_code_toks,
        'comments': comments,
        'docstring': docstring,
        'api_sequence': api_sequence,
    }
    return js


def compute_dataset_record_helper(c, nb_cells, cell_index, context_len, max_tokens):
    js = {
        'execution_count': c['execution_count'],
        'cell_type': c['cell_type'],
        # 'code': c['source'],
        'metadata': c['metadata'],
        'context': []
    }
    js.update(get_all_code_process(c['source']))

    if not js['code_tokens']:
        return None
    if len(js['code_tokens_clean']) > max_tokens:
        return None

    # get cells above in reversed order
    reversed_cells_before = reversed(nb_cells[:cell_index])

    # we store number here for metrics purposes later on.
    js['num_cells_above'] = len(nb_cells[:cell_index])

    js['imports'] = get_imports_simple(nb_cells, cell_index)

    for i, c in enumerate(reversed_cells_before):
        dist = i+1
        if dist <= context_len and is_valid_cell(c):
            new_js = {
                'cell_type': c['cell_type'],
                'distance_target': dist,
            }
            if is_markdown(c):
                try:
                    tokens = normalize_nl_leave_code_tokenize(c['source'])
                except:
                    tokens = c['source'].split()

                # nl may not be directly above, so we pick closest one in context above
                if 'nl' not in js:
                    js['nl'] = tokens

                new_js.update({
                    'nl': tokens,
                    'nl_original': c['source']
                })
            else:
                # skip test cases since they don't provide signal for api seq generation
                # ok to leave in here for noisy train since it'll have type unknown
                if grading_type(c) == 'autograder tests':
                    continue

                j = get_all_code_process(c['source'], allow_api_declarations=True)
                new_js.update(j)

            js['context'].append(new_js)

    return js



def compute_dataset_record(row, context_len, max_tokens):
    '''This small wrapper used so the helper function can be re-used by other pipelines.'''
    cell = row.og_cell
    nb_cells = row.cells
    cell_index = row.metadata_cell['cell_index']

    return compute_dataset_record_helper(cell, nb_cells, cell_index, context_len, max_tokens)


def get_code_context_records(cells_indir, cells_outdir, nbs_indir, context_len, max_tokens):
    '''Convert cells into the dataset format where each record will store the
    context/code pairs.'''
    logger.info('')
    shutil.rmtree(cells_outdir, ignore_errors=True)
    Path(cells_outdir).mkdir(exist_ok=True)


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


    # add nb_index key first to be able to join
    cells_df = db.read_text(cells_indir +'/*.jsonl').map(json.loads).map(lambda js: add_key(js,cell=True)).to_dataframe(meta=cells_meta)
    nbs_df = db.read_text(nbs_indir +'/*.jsonl').map(json.loads).map(lambda js: add_key(js)).to_dataframe(meta=nb_meta)


    with ProgressBar(minimum=15):
        # join each cell with nb to compute dataset record
        (cells_df.merge(nbs_df, on='nb_index', suffixes=['_cell', '_nb'])
     .apply(compute_dataset_record, context_len=context_len, max_tokens=max_tokens, meta=object, axis=1).to_bag()
     # records with len greater than max tokens will be none so we filter for valid records
     .filter(lambda js: js and js['code_tokens'])
     .map(json.dumps).to_textfiles(cells_outdir + '/*.jsonl'))

