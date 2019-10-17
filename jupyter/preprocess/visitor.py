''' This script is a visitor to replace strings, method params, and local assignments.
'''

import ast
import astor

from nbconvert.filters.strings import ipython2python

class NormalizeStringOnly(ast.NodeTransformer):
    def visit_Str(self, node):
        node.s = 'jupyter_string'
        return node

class NormalizeCodeTokens(NormalizeStringOnly):

    def __init__(self):
        super(NormalizeCodeTokens, self).__init__()
        self.args = {}
        self.assigns = {}
        self.in_assign = False

    def visit_Assign(self, node):
        self.in_assign = True
        self.generic_visit(node)
        self.in_assign = False

        return node

    def visit_FunctionDef(self, node):
        # print(vars(node))
        # print(ast.get_docstring(node))
        # normalize method name
        node.name = 'method_name'

        for arg in node.args.args:
            norm_arg_name = f'arg{len(self.args)}'
            self.args[arg.arg] = norm_arg_name
            arg.arg = norm_arg_name

        self.generic_visit(node)
        return node

    def visit_Name(self, node):
        # the right side of the assign expression will also have in_assign
        # set, but its ctx with be Load, which is why we look for Store which
        # is the left side
        if self.in_assign and isinstance(node.ctx, ast.Store):
            norm_arg_name = f'loc{len(self.assigns)}'
            self.assigns[node.id] = norm_arg_name

        if node.id in self.args:
            node.id = self.args[node.id]
        elif node.id in self.assigns:
            node.id = self.assigns[node.id]

        return node


def normalize_code(code_str, only_strings=False):
    # code_str = ipython2python(code_str)
    expr_ast = ast.parse(code_str)
    visitor = NormalizeStringOnly() if only_strings else NormalizeCodeTokens()
    visited = visitor.visit(expr_ast)
    return astor.to_source(visited)

s = '''
x = """
yo
"""
y = 4
'''

if __name__ == '__main__':
    # expr = """import pandas as pd;x = pd.DataFrame([])"""
    expr = """x = set();x.add(4)"""
    expr_ast = ast.parse(expr)
    print(ast.dump(expr_ast))

    # testing triple quoted strings
    expr_ast = ast.parse(s)
    visitor = NormalizeCodeTokens()
    visited = visitor.visit(expr_ast)
    print(astor.to_source(visited))
    # exit()


    expr = """def add(arg1, arg2): return arg1 + arg2"""
    expr_ast = ast.parse(expr)
    visitor = NormalizeCodeTokens()
    visited = visitor.visit(expr_ast)
    print(astor.to_source(visited))
    assert astor.to_source(visited) == """def method_name(arg0, arg1):\n    return arg0 + arg1\n"""

    # exit()

    expr = """x = 4;x += 1; a, b = f(); foo = bar = 1; z.y = x.x"""
    expr_ast = ast.parse(expr)
    visitor = NormalizeCodeTokens()
    visited = visitor.visit(expr_ast)
    print(astor.to_source(visited))
    assert astor.to_source(visited) == """loc0 = 4\nloc0 += 1\nloc1, loc2 = f()\nloc3 = loc4 = 1\nz.y = loc0.x\n"""

    expr = """x = 'yo';y = "cat" """
    expr_ast = ast.parse(expr)
    visitor = NormalizeCodeTokens()
    visited = visitor.visit(expr_ast)
    print(astor.to_source(visited))
    assert astor.to_source(visited) == """loc0 = 'jupyter_string'\nloc1 = 'jupyter_string'\n"""

    # todo deal with loop variable normalization
    expr = """for i in range(2):    print(i)"""
    expr_ast = ast.parse(expr)
    visitor = NormalizeCodeTokens()
    visited = visitor.visit(expr_ast)
    print(astor.to_source(visited))
    assert astor.to_source(visited) == """loc0 = 'jupyter_string'\nloc1 = 'jupyter_string'\n"""

    # print(ipython2python('import matplotlib\n%matplotlib inline\nimport matplotlib.pyplot as plt'))

    expr = 'print(sub.stdout)'
    expr_ast = ast.parse(expr)
    visitor = NormalizeCodeTokens()
    visited = visitor.visit(expr_ast)
    print(astor.to_source(visited))
    # assert astor.to_source(visited) == """loc0 = 'jupyter_string'\nloc1 = 'jupyter_string'\n"""

    expr = 'from sklearn.neighbors import KNeighborsClassifier\n\nknn = KNeighborsClassifier()\nknn.fit(wine_data_train, wine_labels_train)'
    print(normalize_code(expr, only_strings=True))
    print(normalize_code(expr, only_strings=False))
