''' This script 
usage:
python -m jupyter.preprocess.parse_utils

'''

import ast
import astor

def does_code_parse(cell):
    source = cell['source']
    try:
        ast.parse(source)
        return True
    except:
        # print(source)
        return False

def is_access(node):
    return isinstance(node, ast.Expr) and (
                isinstance(node.value, ast.Name) or
                isinstance(node.value, ast.Attribute) or
                isinstance(node.value, ast.Subscript))

def remove_last_variable(code):
    '''code cells tend to end with a single variable, or a variable
    attribute access. this is so the outputs can be examined. we
    this, since its hard predict whether to generate this.'''
    try:
        tree = ast.parse(code)
        if is_access(tree.body[-1]):
            tree.body.pop()
            return astor.to_source(tree)
    except:
        pass
    return code

class NameLoadStoreVisitor(ast.NodeTransformer):
    '''computes which identifers were loaded and stored.

    attributes aren't an ast node so these arent visited
    e.g.  lst.append()  the append is an attribute of lst
    '''
    def __init__(self):
        super(NameLoadStoreVisitor, self).__init__()
        self.in_assign = False
        self.identifiers_loaded = set()
        self.identifiers_stored = set()
    def visit_FunctionDef(self, node):
        self.identifiers_stored.add(node.name)
        self.generic_visit(node)
        return node
    def visit_ClassDef(self, node):
        self.identifiers_stored.add(node.name)
        self.generic_visit(node)
        return node
    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Store):
            self.identifiers_stored.add(node.id)

        if isinstance(node.ctx, ast.Load):
            # it couldve been defined in previous stmt, if so we shouldnt
            # consider it
            if node.id not in self.identifiers_stored:
                self.identifiers_loaded.add(node.id)
        return node
    def visit_arg(self, node):
        self.identifiers_stored.add(node.arg)
        self.generic_visit(node)
    def visit_alias(self, node):
        self.identifiers_stored.add(node.name)
        if node.asname:
            self.identifiers_stored.add(node.asname)
        self.generic_visit(node)
    def visit_ExceptHandler(self, node):
        ''' except Exception as e: the e will be stored'''
        self.identifiers_stored.add(node.name)
        self.generic_visit(node)

def get_all_stored_identifiers(code):
    tree = ast.parse(code)
    visitor = NameLoadStoreVisitor()
    visitor.visit(tree)
    return visitor.identifiers_stored

def get_non_attribute_identifiers_loaded(code):
    tree = ast.parse(code)
    visitor = NameLoadStoreVisitor()
    visitor.visit(tree)
    # list comprehensions the store happens the load, so to handle
    # that case, if ident is stored somewhere in code, remove it from loaded
    return visitor.identifiers_loaded - visitor.identifiers_stored

if __name__ == '__main__':
    code = '''
try:
    print()
except Exception as e:
    print(e)
'''
    print(ast.dump(ast.parse(code)))
    print(get_non_attribute_identifiers_loaded(code))

    expr = """x = set();x;x.y;x.y.z"""
    # expr = """x = set();x"""
    expr_ast = ast.parse(expr)
    # print(ast.dump(expr_ast))
    print(remove_last_variable(expr))
