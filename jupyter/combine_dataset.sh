#!/usr/bin/env bash

PIPELINE_DIR=$1

# define a target dir
TARGET=$PIPELINE_DIR/final-dataset
mkdir -p $TARGET

NBGRADER=$PIPELINE_DIR/nbgrader/dataset-evalonly
SOLUTION=$PIPELINE_DIR/exercise/dataset
NOISY_TRAIN=$PIPELINE_DIR/train/deduped-nl/deduped_nl.jsonl

get_seeded_random()
{
  seed="$1"
  openssl enc -aes-256-ctr -pass pass:"$seed" -nosalt \
    </dev/zero 2>/dev/null
}

# combine dev/test from each pipeline and shuffle
cat $NBGRADER/dev.jsonl $SOLUTION/dev.jsonl | shuf --random-source=<(get_seeded_random 42) --output=$TARGET/dev.jsonl
cat $NBGRADER/test.jsonl $SOLUTION/test.jsonl | shuf --random-source=<(get_seeded_random 42) --output=$TARGET/test.jsonl

cat $NOISY_TRAIN | shuf --random-source=<(get_seeded_random 42) --output=$TARGET/train.jsonl
