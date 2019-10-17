#!/usr/bin/env bash
# ./run_all.sh  /home/rajas/downloads/juice-notebooks /scratch/temp-pipeline

# should point to juice-notebooks
DATASET_DIR=$1
# directory where pipeline is written out
PIPELINE_DIR=$2


echo "Nbgrader pipeline"
python -m jupyter.nbgrader.pipeline_nbgrader -input_nbs_dir $DATASET_DIR/nbgrader  -pipeline_outdir $PIPELINE_DIR/nbgrader -max_nl_distance 3 -max_api_seq_len 15 -min_api_seq_len 0 -context_len 1200

echo "Exercise pipeline"
python -m jupyter.exercise.pipeline -input_nbs_dir $DATASET_DIR/exercise -pipeline_dir $PIPELINE_DIR/exercise

echo "Train pipeline"
python -m jupyter.new_pipeline.pipeline_train -input_nbs_dir $DATASET_DIR/train -pipeline_outdir $PIPELINE_DIR/train  -max_nl_distance 1 -context_len 12  -min_markdown_ratio 0.3  -max_tokens 120

./jupyter/combine_dataset.sh $PIPELINE_DIR
