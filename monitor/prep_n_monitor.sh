#!/usr/bin/env bash

# Create output directory
mkdir -p ~/yarn-eagle/output
#chmod -R 777 ~/yarn-eagle/output


source ~/yarn-eagle/miniconda/etc/profile.d/conda.sh

conda activate venv
pip list
python --version
~/yarn-eagle/miniconda/envs/venv/bin/python ~/yarn-eagle/monitor/monitor.py ~/yarn-eagle/monitor/cluster.config
