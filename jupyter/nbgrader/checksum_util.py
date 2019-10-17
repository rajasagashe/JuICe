''' This script 
usage:

'''

import hashlib
import sys


def is_grade(cell):
    """Returns True if the cell is a grade cell."""
    if 'nbgrader' not in cell.metadata:
        return False
    return cell.metadata['nbgrader'].get('grade', False)
def is_solution(cell):
    """Returns True if the cell is a solution cell."""
    if 'nbgrader' not in cell.metadata:
        return False
    return cell.metadata['nbgrader'].get('solution', False)
def is_locked(cell):
    """Returns True if the cell source is locked (will be overwritten)."""
    if 'nbgrader' not in cell.metadata:
        return False
    elif is_solution(cell):
        return False
    elif is_grade(cell):
        return True
    else:
        return cell.metadata['nbgrader'].get('locked', False)
def to_bytes(string):
    """A python 2/3 compatible function for converting a string to bytes.
    In Python 2, this just returns the 8-bit string. In Python 3, this first
    encodes the string to utf-8.
    """
    if sys.version_info[0] == 3 or (sys.version_info[0] == 2 and isinstance(string, unicode)):
        return bytes(string.encode('utf-8'))
    else:
        return bytes(string)
def compute_checksum(cell):
    import hashlib
    m = hashlib.md5()
    # add the cell source and type
    m.update(to_bytes(cell.source))
    m.update(to_bytes(cell.cell_type))

    # add whether it's a grade cell and/or solution cell
    m.update(to_bytes(str(is_grade(cell))))
    m.update(to_bytes(str(is_solution(cell))))
    m.update(to_bytes(str(is_locked(cell))))

    # include the cell id
    # print(cell.metadata.nbgrader['grade_id'])
    m.update(to_bytes(cell.metadata.nbgrader['grade_id']))

    # include the number of points that the cell is worth, if it is a grade cell
    if is_grade(cell):
        m.update(to_bytes(str(float(cell.metadata.nbgrader['points']))))

    return m.hexdigest()

