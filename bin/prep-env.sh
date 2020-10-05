#!/usr/bin/env bash

if [ ! -d ~/miniconda ]; then

  echo "installing miniconda "
  # Set your corporate proxy in case
  export http_proxy=your_proxy_server # change your_proxy_server to your corporate proxy server:port
  export https_proxy=$http_proxy
  # Get miniconda
  wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
  # run install
  bash ~/Miniconda3-latest-Linux-x86_64.sh -b -p ~/miniconda
  # create venv
  ~/miniconda/bin/conda create -n venv --copy -y -q python=3.8.3 pandas numpy openpyxl flask croniter  pytz dash
else
  echo " miniconda exists .. continue with next step"
fi