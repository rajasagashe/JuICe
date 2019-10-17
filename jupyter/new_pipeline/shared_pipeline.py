''' This script filters code cells and outputs the dataset.
'''

from jupyter.new_pipeline import filter
from jupyter.new_pipeline import to_dataset
from jupyter.new_pipeline.preprocess import dump_cells


def main(input_nbs_dir,
         pipeline_outdir,
         max_nl_distance,
         context_len,
         max_api_seq_len,
         min_api_seq_len,
         is_nbgrader=False,
         downsample=-1,
         min_markdown_ratio=-1,
         filter_code_tags=False,
         filter_docstring=False,
         exclusion_path_recs='',
         max_tokens=1111111
         ):

    # Each preprocessing steps caches outputs into these directories.
    dataset_outdir = f'{pipeline_outdir}/cells'
    dataset_outdir2 = f'{pipeline_outdir}/cells2-parseable'
    dataset_outdir4 = f'{pipeline_outdir}/cells4-onefuncmaxapi'
    dataset_outdir5 = f'{pipeline_outdir}/cells5-nl{max_nl_distance}distaway'
    dataset_outdir6 = f'{pipeline_outdir}/cells6-dataset'
    datasetviz_outdir = f'{pipeline_outdir}/cells-viz'

    dump_cells(nbs_dir=input_nbs_dir,
                   dataset_outdir=dataset_outdir,
                   viz_outdir=datasetviz_outdir,
                   write_cells=is_nbgrader)

    filter.filter_parseable_code_cells(dataset_outdir, dataset_outdir2, is_nbgrader)

    filter.one_func_max_api_seq(dataset_outdir2, dataset_outdir4, max_api_seq_len=max_api_seq_len,
                                min_api_seq_len=min_api_seq_len)

    if is_nbgrader:
        # there is scope to increase size of nbgrader dataset. here we filter to cells with markdown
        # above but there are some good target cells which have a comment target nl. not very frequent
        filter.filter_cells_nl_above_dataframe(dataset_outdir4, dataset_outdir5,
                                           nbs_indir=input_nbs_dir, max_dist=max_nl_distance)
    else:
        # for noisy train we already make sure markdown above in the dump_cells method
        # since the join is expensive
        dataset_outdir5 = dataset_outdir4


    to_dataset.get_code_context_records(dataset_outdir5, dataset_outdir6, input_nbs_dir,
                                        context_len=context_len, max_tokens=max_tokens)

    return dataset_outdir6

