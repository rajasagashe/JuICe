''' Generates dataset from nbgrader notebooks.
'''
import argparse
import logging
from pathlib import Path

from jupyter.nbgrader import dedup_get_solution
from jupyter.nbgrader import split
from jupyter.new_pipeline import filter
from jupyter.new_pipeline import shared_pipeline

format = '%(asctime)s[%(filename)25s:%(lineno)4s - %(funcName)30s()] - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    datefmt='%I:%M:%S %p',
    format=format,
    handlers=[
        logging.FileHandler("nbgrader.log", mode='w'),
        logging.StreamHandler()
    ])

def run_pipeline(opts):
    pipeline_outdir = f'{opts.pipeline_outdir}'
    # this one we split into train/dev/test so we can fine tune on train
    dataset_outdir = f'{pipeline_outdir}/dataset'
    # this one we split only into dev/test
    dataset_outdir_eval = f'{pipeline_outdir}/dataset-evalonly'

    Path(pipeline_outdir).mkdir(exist_ok=True, parents=True)
    Path(dataset_outdir).mkdir(exist_ok=True)
    Path(dataset_outdir_eval).mkdir(exist_ok=True)

    dataset_outdir7 = f'{opts.pipeline_outdir}/cells7-autograded'
    nbgrader_dataset = f'{dataset_outdir}/nbgrader_dataset.jsonl'

    dev_file_eval = f'{dataset_outdir_eval}/dev.jsonl'
    test_file_eval = f'{dataset_outdir_eval}/test.jsonl'


    dataset_dir = shared_pipeline.main(input_nbs_dir=opts.input_nbs_dir,
                                        pipeline_outdir=pipeline_outdir,
                                        max_nl_distance=opts.max_nl_distance,
                                        context_len=opts.context_len,
                                        max_api_seq_len=opts.max_api_seq_len,
                                        min_api_seq_len=opts.min_api_seq_len,
                                        is_nbgrader=True)

    filter.filter_graded_code_cells(dataset_dir, dataset_outdir7)

    dedup_get_solution.main(dataset_outdir7, nbgrader_dataset, max_tokens=120)


    # for only dev/test eval split
    split.split_simple(nbgrader_dataset, dev_file=dev_file_eval, test_file=test_file_eval,
                test_size=.55)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-input_nbs_dir')
    parser.add_argument('-large')
    parser.add_argument('-pipeline_outdir')
    parser.add_argument('-max_nl_distance', type=int)
    parser.add_argument('-max_api_seq_len', type=int, default=15)
    parser.add_argument('-min_api_seq_len', type=int, default=0)
    parser.add_argument('-context_len', type=int)
    opts = parser.parse_args()

    assert opts.max_nl_distance <= opts.context_len
    run_pipeline(opts)
