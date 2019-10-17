'''Generates train dataset from all jupyter notebooks, excluding
nbgrader and exercise notebooks.
'''
import argparse
import logging
from pathlib import Path

from jupyter.new_pipeline import shared_pipeline
from jupyter.new_pipeline import split


format = '%(asctime)s[%(filename)25s:%(lineno)4s - %(funcName)30s()] - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    datefmt='%I:%M:%S %p',
    format=format,
    handlers=[
        logging.FileHandler("train.log", mode='w'),
        logging.StreamHandler()
    ])


def run_pipeline(opts):
    Path(opts.pipeline_outdir).mkdir(exist_ok=True)
    dataset_dir = f'{opts.pipeline_outdir}/datasets'
    Path(dataset_dir).mkdir(exist_ok=True)

    deduped_code_dir = f'{opts.pipeline_outdir}/deduped-code'
    Path(deduped_code_dir).mkdir(exist_ok=True)
    deduped_nl_dir = f'{opts.pipeline_outdir}/deduped-nl'
    Path(deduped_nl_dir).mkdir(exist_ok=True)

    dataset_strict_dir = f'{opts.pipeline_outdir}/datasets-strict'
    Path(dataset_strict_dir).mkdir(exist_ok=True)


    dumped_rec_dir = shared_pipeline.main(input_nbs_dir=opts.input_nbs_dir,
                                                pipeline_outdir=opts.pipeline_outdir,
                                                max_nl_distance=opts.max_nl_distance,
                                                context_len=opts.context_len,
                                                max_api_seq_len=opts.max_api_seq_len,
                                                min_api_seq_len=opts.min_api_seq_len,
                                                downsample=opts.downsample,
                                                min_markdown_ratio=opts.min_markdown_ratio,
                                                max_tokens=opts.max_tokens)


    # we dedup on code, than a strict one on the nl
    split.dedup_dump(dumped_rec_dir, deduped_code_dir, 'code_tokens')
    split.dedup_dump(deduped_code_dir, deduped_nl_dir, 'nl')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-input_nbs_dir', required=True)
    parser.add_argument('-pipeline_outdir', required=True)
    parser.add_argument('-max_api_seq_len', type=int, default=15)
    parser.add_argument('-min_api_seq_len', type=int, default=0)
    parser.add_argument('-max_nl_distance', type=int)
    parser.add_argument('-context_len', type=int)
    parser.add_argument('-downsample', type=float, default=-1)
    parser.add_argument('-min_markdown_ratio', type=float)
    parser.add_argument('-max_tokens', type=int)
    opts = parser.parse_args()

    assert opts.max_nl_distance <= opts.context_len

    run_pipeline(opts)
