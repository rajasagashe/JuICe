''' This script 
usage:

'''


from io import StringIO
import token as tk
import builtins
from tokenize import generate_tokens
from tokenize import tokenize, untokenize, NUMBER, STRING, NAME, OP, tok_name
from keyword import kwlist

# dong_keywords = ['in', 'classmethod', 'not', 'return', 'finally', 'reversed', 'next', 'bool', 'while', 'True', 'id', 'open', 'help', 'assert', 'filter', 'issubclass', 'iter', 'range', 'and', 'round', 'map', 'int', 'format', 'if', 'str', 'sum', 'as', 'lambda', 'enumerate', 'getattr', 'False', 'abs', 'unicode', '__import__', 'compile', 'isinstance', 'is', 'setattr', 'yield', 'sorted', 'hasattr', 'delattr', 'else', 'dir', 'max', 'hash', 'class', 'any', 'ord', 'repr', 'del', 'zip', 'with', 'super', 'for', 'object', 'except', 'callable', 'reload', 'global', 'staticmethod', 'print', 'elif', 'tuple', 'min', 'input', 'def', 'len', 'or', 'from', 'vars', 'property', 'float', 'slice', 'memoryview', 'list', 'reduce', 'set', 'divmod', 'all', 'xrange', 'eval', 'None', 'file', 'raise', 'type', 'dict', 'try', 'import']
builtin_keywords = set(dir(builtins)) | set(kwlist)
# python2 keywords
builtin_keywords.update(['xrange', 'reduce', 'unicode', 'file', 'reload'])

assert 'print' in builtin_keywords
assert 'Exception' in builtin_keywords
assert 'str' in builtin_keywords
assert 'RuntimeError' in builtin_keywords

# print(set(dong_keywords)-builtin_keywords)
# print(kwlist)
# assert set(dong_keywords).issubset(builtin_keywords)

def get_all_identifiers(code):
    tokens, types = tokenize_and_templatize(code)
    return [tok for tok, ty in zip(tokens, types) if ty == 'NAME']

def tokenize_and_templatize(code, name_for_keywords=False):
    token_stream = generate_tokens(StringIO(code).readline)
    tokens = []
    template_tokens = []
    for toknum, tokval, (srow, scol), (erow, ecol), _ in token_stream:
        # print(srow, scol, erow, ecol, tokval)
        if toknum == tk.ENDMARKER:
            break
        tokens.append(tokval)

        if toknum == NUMBER:
            template_tokens.append('NUMBER')
        elif toknum == NAME:
            # the benefit here is that these keywords occur in the nl, so
            # when we look for nl overlap more meaningful variable names
            # will be used
            if tokval in builtin_keywords and not name_for_keywords:
                # template_tokens.append('KEYWORD')
                template_tokens.append(tokval)
                # template_tokens.append('NAME')
            else:
                template_tokens.append('NAME')
        elif toknum == STRING:
            template_tokens.append('STRING')
        elif toknum == OP:
            # template_tokens.append('OP')
            template_tokens.append(tokval)
        else:
            # for comments, COMMENT will be added
            template_tokens.append(tok_name[toknum])

    return tokens, template_tokens

def untokenize_lst(lst):
    return untokenize(lst)

def gen_api_seq(tokens, types, allow_declarations=False):
    # todo come back and parse for extracting full parent, ignoreing func and class decs
    seq = []

    for i in range(len(tokens)-1):
        typ = types[i]
        # tokens,typ =
        if tokens[i+1] == '(':
            # ignore function/class defs
            if (i-1)>=0 and tokens[i-1] in ['def', 'class']:
                if allow_declarations:
                    # we'll add the def/class tokens to provide more information for the model
                    seq.append(tokens[i-1])
                    seq.append(tokens[i])

                else:
                    continue
            # if typ in ['NAME', 'sum', 'min', 'max'] or ((i-1) and tokens[i-1] == '.'):
            # after or condition checks for cases like np.sum or re.compile. these
            # functions are keywords to typ would be the keyword, thats why we check
            # for the .
            if typ == 'NAME' or ((i-1)>=0 and tokens[i-1] == '.'):
                seq.append(tokens[i])
    # if not seq:
    #     print('===============')
    #     print(tokens)
    #     print(seq)

    return seq
