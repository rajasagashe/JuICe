''' This script 
usage:

'''

import argparse
import random

random.seed(1123)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-input')
    parser.add_argument('-output')
    parser.add_argument('-flag', action='store_true')
    opts = parser.parse_args()