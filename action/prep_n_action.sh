#!/usr/bin/env bash
#source ~/.profile


filename=$1
cd ~/yarn-eagle/miniconda
source ~/yarn-eagle/miniconda/etc/profile.d/conda.sh
conda activate venv

cd ~/yarn-eagle/action
~/yarn-eagle/miniconda/envs/venv/bin/python action.py ~/yarn-eagle/monitor/cluster.config $filename

