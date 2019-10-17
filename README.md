# Juice Dataset
Code for the [paper](https://arxiv.org/abs/1910.02216). This repository produces the dataset from the collected Jupyter notebooks. The dataset is available [here](https://drive.google.com/file/d/1xWDV__5hjTWVuJlXD42Ar7nkjU2hRTic/view?usp=sharing) if you don't want to run this pipeline.

## Leaderboard

| **Model** | **Dev Bleu** | **Dev EM** | **Test Bleu** | **Test EM** | 
| :---: | :---:  | :---: | :---: | :---: |
| LSTM Baseline | 21.66 | 5.57  | 20.92 | 5.71 |


## Download notebooks
Get the notebooks [here](https://drive.google.com/file/d/1hNtknKrJyOXBM5IfApYs_YyBESLOz_6w/view?usp=sharing). 


## Setup
    conda create -n {name} python=3.6 anaconda
    source activate {name}
    
    pip install -r requirements.txt
    
    # decompress the downloaded notebooks file
    unzip juice_notebooks.zip

## Running pipelines
Produces the dataset. The pipeline requires around 254gb of disk space, and takes about 12 hours to complete on a 12 core machine.

    ./run_all {downloaded notebooks directory} {pipeline directory}
    
The datasets will be created under ```{pipeline directory}/final-dataset```

## Dataset Format
Each dataset record contains the following keys:
    
```code_tokens_clean```: The tokenized target code to generate.

```context```: A list of cells above starting with the cell directly above. If the cell is markdown the key ```nl``` will store the tokenized mardown. If it's a code cell the ```code_tokens_clean``` will store the tokenized code.
    
 




