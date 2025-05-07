#!/bin/bash

./run_hdf5.sh

./run_dask.sh "(1, 3600, 1800)"
./run_dask.sh "(1, 900, 450)"
./run_dask.sh "(1, 225, 100)"
./run_dask.sh "(1, 50, 25)"

python3 check_pickle_files.py