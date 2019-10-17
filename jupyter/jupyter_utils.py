import json
from io import StringIO

from nbformat import reads, write, read, writes
from nbformat.v4 import new_notebook, new_code_cell, new_markdown_cell

def _cells(nb):
    """Yield all cells in an nbformat-insensitive manner"""
    if nb.nbformat < 4:
        for ws in nb.worksheets:
            for cell in ws.cells:
                yield cell
    else:
        for cell in nb.cells:
            yield cell
def get_cells(nb):
    return [cell for cell in _cells(nb)]
def is_code(cell):
    return 'cell_type' in cell and cell['cell_type'] == 'code'
def is_markdown(cell):
    return 'cell_type' in cell and cell['cell_type'] == 'markdown'

def dump_nb_string_to_file(nb_string, file):
    nb = load_nb_string(nb_string)
    dump_nb_to_file(nb, file)

def dump_nb_to_file(nb, file):
    try:
        write(nb, file, version=4)
    except:
        json.dump(nb, open(file, 'w'), indent=4)


def dump_nb_to_file_jsonlib(nb, file):
    json.dump(nb, open(file, 'w'), indent=4)

# the reason we add these jsonlib functions is that nbformat
# doesnt know how to handle nb cells with all the additional
# metadata we add later in the pipeline. thus better to avoid
# this library
def dump_nb_to_string_jsonlib(nb):
    return json.dumps(nb)
def dump_nb_to_string(nb):
    return writes(nb, version=4)


def load_nb_file(file):
    # return read(StringIO(nb_string), as_version=4)
    return read(file, as_version=4)
def load_nb_string(nb_string):
    # return read(StringIO(nb_string), as_version=4)
    return reads(nb_string, as_version=4)
def load_nb_string_jsonlib(nb_string):
    return json.loads(nb_string)

def is_valid_cell(cell):
    return cell and 'source' in cell and cell['source'] and 'cell_type' in cell

def num_code_cells(cells):
    return sum([is_code(cell) for cell in cells if is_valid_cell(cell)])
def num_markdown_cells(cells):
    return sum([is_markdown(cell) for cell in cells if is_valid_cell(cell)])

def create_cell(lst, cell_type='code'):
    d = {'source': '\n'.join(lst),
         'cell_type': cell_type,
         'metadata':{}}
    if cell_type == 'code':
        d['outputs'] = []
        d['execution_count'] = 0
    return d

def is_python_nb(nb):
    return 'kernelspec' in nb['metadata'] and 'language' in nb['metadata']['kernelspec'] and nb['metadata']['kernelspec']['language'] == 'python'

def is_python3_nb(nb):
    # print(nb['metadata'])
    return nb['metadata']['kernelspec']['name'] == 'python3'

def get_url(abs_path):
    url = 'http://kingboo.cs.washington.edu:34000/tree{}'
    url = url.format(abs_path)
    return url.replace(' ', '%20')

def get_annotation_url(abs_path):
    # todo deprecate this
    abs_path = abs_path.replace('/scratch/annotation', '')
    url = 'http://kingboo.cs.washington.edu:8887/tree{}'
    url = url.format(abs_path)
    return url.replace(' ', '%20')

