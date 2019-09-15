#!env bash
ENV_NAME=bicis
NAME=Bicis

# Create and activate env
conda create -yn $ENV_NAME python=2.7
source activate $ENV_NAME

# Dependencies
conda install -yc conda-forge pip
pip install -r "$(dirname "$0")"/../requirements.txt


# Jupyter Kernel
python -m ipykernel install --user --name $ENV_NAME --display-name=$NAME
